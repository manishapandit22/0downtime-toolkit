package io.github.zerodowntime.demo;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.zerodowntime.cli.Cli;
import io.github.zerodowntime.cli.Services;
import io.github.zerodowntime.core.Phase;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DemoTest {

    private static Infra infra;

    @BeforeAll
    static void up() {
        infra = new Infra();
    }

    @AfterAll
    static void down() {
        infra.close();
    }

    private static final class Run {
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        final Cli cli;
        final Services s;

        Run(Services s) {
            this.s = s;
            this.cli = new Cli(s, new PrintStream(buf), new PrintStream(buf));
        }

        int run(String... args) {
            return cli.run(args);
        }

        String output() {
            return buf.toString();
        }
    }

    /** The full operator workflow through the CLI, ending in cutover and rollback. */
    @Test
    void operatorWorkflowThroughTheCli() {
        var r = new Run(infra.newServices());
        assertThat(r.run("init")).isEqualTo(Cli.OK);
        assertThat(r.run("init")).as("idempotent").isEqualTo(Cli.OK);

        for (int i = 0; i < 30; i++) {
            r.s.source().upsert("k" + i, Map.of("n", String.valueOf(i)));
        }

        // not backfilled and outbox not drained yet: verify must say so
        assertThat(r.run("verify", "--rounds", "0")).isEqualTo(Cli.REFUSED);
        assertThat(r.output()).contains("MISSING").contains("differences remain");

        assertThat(r.run("backfill", "--job", "j", "--chunk", "7")).isEqualTo(Cli.OK);
        assertThat(r.run("drain")).isEqualTo(Cli.OK);
        assertThat(r.run("verify", "--rounds", "1", "--settle-ms", "1")).isEqualTo(Cli.OK);
        assertThat(r.output()).contains("RESULT: clean");

        assertThat(r.run("cutover")).isEqualTo(Cli.OK);
        assertThat(r.s.state().current()).isEqualTo(Phase.CUTOVER);
        r.s.targetStore().upsert("k1", Map.of("n", "on-target"));
        assertThat(r.run("status")).isEqualTo(Cli.OK);
        assertThat(r.output()).contains("phase:            CUTOVER").contains("reverse pending:  1");

        assertThat(r.run("rollback")).isEqualTo(Cli.OK);
        assertThat(r.s.state().current()).isEqualTo(Phase.ROLLED_BACK);
        assertThat(r.s.source().read("k1").orElseThrow().attributes()).containsEntry("n", "on-target");
        assertThat(r.run("verify", "--rounds", "0")).isEqualTo(Cli.OK);
    }

    @Test
    void cutoverRefusalIsAnExitCodeNotACrash() throws Exception {
        var r = new Run(infra.newServices());
        r.run("init");
        // a row that bypasses the outbox and is never backfilled
        try (var c = r.s.postgres().getConnection();
                var ps = c.prepareStatement(
                        "INSERT INTO source_records (record_key, version, deleted, attrs) VALUES ('orphan', 1, false, '{}')")) {
            ps.executeUpdate();
        }

        assertThat(r.run("cutover")).isEqualTo(Cli.REFUSED);
        assertThat(r.output()).contains("CUTOVER REFUSED");
        assertThat(r.s.state().current()).isEqualTo(Phase.DUAL_WRITE);
    }

    @Test
    void badUsageGivesExitCode2() {
        var r = new Run(infra.newServices());
        assertThat(r.run()).isEqualTo(Cli.USAGE);
        assertThat(r.run("nonsense")).isEqualTo(Cli.USAGE);
        assertThat(r.run("backfill", "--chunk")).isEqualTo(Cli.USAGE);
        assertThat(r.run("backfill", "--chunk", "abc")).isEqualTo(Cli.USAGE);
    }

    /** A short version of the benchmark run: the invariants must hold, whatever the speed. */
    @Test
    void migrationUnderLiveLoadLosesNothing() throws Exception {
        var report = DemoRunner.run(
                infra.newServices(),
                new DemoRunner.Config(2000, 4, 150, 2),
                new PrintStream(new ByteArrayOutputStream()));

        assertThat(report.lostOrWrongWrites()).as(report.toMarkdown()).isZero();
        assertThat(report.finalDiffs()).as(report.toMarkdown()).isZero();
        assertThat(report.writesByPhase().get("target-authoritative")).as("writes landed on the target").isGreaterThan(0);
        assertThat(report.writesByPhase().get("rolled-back")).isGreaterThan(0);
        assertThat(report.totalAckedWrites()).isGreaterThan(500);
    }
}
