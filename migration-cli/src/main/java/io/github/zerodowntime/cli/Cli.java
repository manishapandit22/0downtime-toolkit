package io.github.zerodowntime.cli;

import io.github.zerodowntime.core.Backfiller;
import io.github.zerodowntime.core.CheckpointStore;
import io.github.zerodowntime.core.CutoverController;
import io.github.zerodowntime.core.CutoverRejectedException;
import io.github.zerodowntime.core.Diff;
import io.github.zerodowntime.core.OutboxRelay;
import io.github.zerodowntime.core.ReverseRelay;
import io.github.zerodowntime.core.SourceSchema;
import io.github.zerodowntime.core.VerificationReport;
import io.github.zerodowntime.core.Verifier;
import java.io.PrintStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Operator commands. Exit codes: 0 success, 1 the migration refused or found differences
 * (nothing is broken, the operator must act), 2 usage error, 3 unexpected failure.
 */
public final class Cli {

    public static final int OK = 0;
    public static final int REFUSED = 1;
    public static final int USAGE = 2;
    public static final int FAILED = 3;

    private static final int MAX_DIFFS_PRINTED = 20;

    private final Services s;
    private final PrintStream out;
    private final PrintStream err;

    public Cli(Services services, PrintStream out, PrintStream err) {
        this.s = services;
        this.out = out;
        this.err = err;
    }

    public static String usage() {
        return """
                usage: migrate <command> [options]

                  init                              create tables, schema, and the authority flag (idempotent)
                  status                            phase, authority, outbox and reverse-log backlog
                  drain                             deliver all pending outbox events to the target
                  backfill [--job NAME] [--chunk N] copy existing rows; resumes from its checkpoint
                  verify   [--rounds N] [--settle-ms N]
                                                    diff source vs target; exit 1 if anything remains
                  cutover                           gated switch to the target as authority
                  rollback                          switch authority back to the source
                """;
    }

    public int run(String[] args) {
        if (args.length == 0) {
            err.print(usage());
            return USAGE;
        }
        Map<String, String> opts = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                err.println("bad option: " + args[i]);
                err.print(usage());
                return USAGE;
            }
            opts.put(args[i].substring(2), args[++i]);
        }
        try {
            return switch (args[0]) {
                case "init" -> init();
                case "status" -> status();
                case "drain" -> drain();
                case "backfill" -> backfill(opts);
                case "verify" -> verify(opts);
                case "cutover" -> cutover();
                case "rollback" -> rollback();
                default -> {
                    err.println("unknown command: " + args[0]);
                    err.print(usage());
                    yield USAGE;
                }
            };
        } catch (NumberFormatException e) {
            err.println("option must be a number: " + e.getMessage());
            return USAGE;
        } catch (RuntimeException e) {
            err.println("FAILED: " + e);
            return FAILED;
        }
    }

    private int init() {
        SourceSchema.create(s.postgres());
        createTable(s.dataTable());
        createTable(s.controlTable());
        s.targetStore().initAuthority();
        out.println("initialised: phase=" + s.state().current() + " authority=" + s.targetStore().authority());
        return OK;
    }

    private void createTable(String name) {
        try {
            s.dynamo().createTable(CreateTableRequest.builder()
                    .tableName(name)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("pk")
                            .attributeType(ScalarAttributeType.S)
                            .build())
                    .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                    // provisioned, small: stays inside the always-free 25 RCU / 25 WCU
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L)
                            .writeCapacityUnits(5L)
                            .build())
                    .build());
            s.dynamo().waiter().waitUntilTableExists(r -> r.tableName(name)); // real AWS creates asynchronously
            out.println("created table " + name);
        } catch (ResourceInUseException exists) {
            out.println("table " + name + " already exists");
        }
    }

    private int status() {
        out.println("phase:            " + s.state().current());
        out.println("authority:        " + s.targetStore().authority());
        out.println("outbox pending:   " + s.outbox().pendingCount());
        out.println("reverse pending:  " + s.targetStore().pendingReverse().size());
        return OK;
    }

    private int drain() {
        var stats = new OutboxRelay(s.outbox(), s.dataWriter(), 100).drain();
        out.println("delivered=" + stats.delivered() + " applied=" + stats.applied() + " skippedStale=" + stats.skippedStale());
        return OK;
    }

    private int backfill(Map<String, String> opts) {
        String job = opts.getOrDefault("job", "default");
        int chunk = Integer.parseInt(opts.getOrDefault("chunk", "500"));
        long t0 = System.nanoTime();
        var stats = new Backfiller(s.source(), s.dataWriter(), new CheckpointStore(s.postgres()), job, chunk).run();
        double secs = Math.max((System.nanoTime() - t0) / 1e9, 1e-9);
        out.printf(
                "backfill job=%s rows=%d applied=%d skippedStale=%d chunks=%d (%.0f rows/s)%n",
                job, stats.rows(), stats.applied(), stats.skippedStale(), stats.chunks(), stats.rows() / secs);
        return OK;
    }

    private int verify(Map<String, String> opts) {
        int rounds = Integer.parseInt(opts.getOrDefault("rounds", "3"));
        long settleMs = Long.parseLong(opts.getOrDefault("settle-ms", "1000"));
        VerificationReport report = verifier(rounds, Duration.ofMillis(settleMs)).verify();
        out.println(report.summary());
        List<Diff> diffs = report.diffs();
        diffs.stream()
                .limit(MAX_DIFFS_PRINTED)
                .forEach(d -> out.printf(
                        "  %-10s %s source=v%d target=v%d%n", d.type(), d.key(), d.sourceVersion(), d.targetVersion()));
        if (diffs.size() > MAX_DIFFS_PRINTED) {
            out.println("  ... and " + (diffs.size() - MAX_DIFFS_PRINTED) + " more");
        }
        out.println(report.isClean() ? "RESULT: clean" : "RESULT: differences remain");
        return report.isClean() ? OK : REFUSED;
    }

    private int cutover() {
        try {
            controller().cutover();
        } catch (CutoverRejectedException rejected) {
            out.println("CUTOVER REFUSED: " + rejected.getMessage());
            out.println("phase is back to " + s.state().current() + "; writes are accepted again");
            return REFUSED;
        }
        out.println("CUTOVER complete: phase=" + s.state().current() + " authority=" + s.targetStore().authority());
        return OK;
    }

    private int rollback() {
        try {
            controller().rollback();
        } catch (IllegalStateException blocked) {
            out.println("ROLLBACK NOT COMPLETE: " + blocked.getMessage());
            out.println("phase=" + s.state().current() + " (writes stay paused; fix the cause and run rollback again)");
            return REFUSED;
        }
        out.println("ROLLBACK complete: phase=" + s.state().current() + " authority=" + s.targetStore().authority());
        // the in-pause check was scoped to changed keys; confirm the whole table now, writes flowing
        VerificationReport full = verifier(3, Duration.ofMillis(500)).verify();
        out.println("post-rollback full verify: " + full.summary());
        if (!full.isClean()) {
            out.println("WARNING: rolled back, but the full verify found differences; investigate before retrying");
            return REFUSED;
        }
        return OK;
    }

    /** Writes are paused whenever this runs, so there is nothing to settle: rounds = 0. */
    private CutoverController controller() {
        var relay = new OutboxRelay(s.outbox(), s.dataWriter(), 100);
        var reverse = new ReverseRelay(s.targetStore(), s.source());
        // live pre-check with settling (writes still flowing); the pause itself only re-checks changed keys
        return new CutoverController(
                s.state(),
                s.targetStore(),
                relay,
                s.outbox(),
                reverse,
                verifier(0, Duration.ZERO),
                null,
                verifier(3, Duration.ofMillis(500)));
    }

    private Verifier verifier(int rounds, Duration settle) {
        return new Verifier(s.source(), s.dataWriter(), s.dataWriter(), 500, rounds, settle);
    }
}
