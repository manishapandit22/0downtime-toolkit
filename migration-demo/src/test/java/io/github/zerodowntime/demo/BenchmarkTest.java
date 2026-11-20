package io.github.zerodowntime.demo;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * The measured run. Skipped in normal builds. Run with:
 * <pre>./mvnw -o -pl migration-demo -am verify -Dbench=true -Dbench.rows=50000 -Dtest=BenchmarkTest -Dsurefire.failIfNoSpecifiedTests=false</pre>
 * Options: bench.rows (20000), bench.writers (4), bench.rate (200), bench.phase (5), bench.out (file).
 */
@EnabledIfSystemProperty(named = "bench", matches = "true")
class BenchmarkTest {

    @Test
    void measuredRun() throws Exception {
        var cfg = new DemoRunner.Config(
                Integer.getInteger("bench.rows", 20_000),
                Integer.getInteger("bench.writers", 4),
                Integer.getInteger("bench.rate", 200),
                Integer.getInteger("bench.phase", 5));
        try (Infra infra = new Infra()) {
            var report = DemoRunner.run(infra.newServices(), cfg, System.out);
            String md = report.toMarkdown();
            System.out.println("\n" + md);
            Path out = Path.of(System.getProperty("bench.out", "target/benchmark.md"));
            Files.writeString(out, md);
            assertThat(report.lostOrWrongWrites()).isZero();
            assertThat(report.finalDiffs()).isZero();
        }
    }
}
