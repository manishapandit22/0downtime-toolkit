package io.github.zerodowntime.demo;

/**
 * Runs the whole migration under live load on this machine and prints the measured numbers.
 * Options: --rows N (default 20000) --writers N (4) --rate N (writes/s, 200) --phase-seconds N (5)
 */
public final class DemoMain {

    private DemoMain() {}

    public static void main(String[] args) throws Exception {
        int rows = 20_000;
        int writers = 4;
        int rate = 200;
        int phase = 5;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--rows" -> rows = Integer.parseInt(args[i + 1]);
                case "--writers" -> writers = Integer.parseInt(args[i + 1]);
                case "--rate" -> rate = Integer.parseInt(args[i + 1]);
                case "--phase-seconds" -> phase = Integer.parseInt(args[i + 1]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        try (Infra infra = new Infra()) {
            var report = DemoRunner.run(infra.newServices(), new DemoRunner.Config(rows, writers, rate, phase), System.out);
            System.out.println();
            System.out.println(report.toMarkdown());
            if (report.lostOrWrongWrites() != 0 || report.finalDiffs() != 0) {
                System.exit(1);
            }
        }
    }
}
