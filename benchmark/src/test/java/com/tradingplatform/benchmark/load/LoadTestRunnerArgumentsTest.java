package com.tradingplatform.benchmark.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradingplatform.benchmark.load.LoadTestRunner.Arguments;
import org.junit.jupiter.api.Test;

/** CLI-parsing sanity for {@link LoadTestRunner}'s {@code --name=value} argument syntax. */
class LoadTestRunnerArgumentsTest {

    @Test
    void defaultsToSteadyWorkloadAndRouterTarget() {
        Arguments arguments = Arguments.parse(new String[0]);

        assertEquals("steady", arguments.workloadName());
        assertEquals("router", arguments.targetName());
        assertTrue(arguments.producerCount() >= 1);
    }

    @Test
    void parsesEveryRecognizedOption() {
        Arguments arguments = Arguments.parse(new String[] {
            "--workload=high-cancel-rate", "--target=coarse-lock", "--producers=3",
            "--orders=1000", "--workers=2", "--seed=42"
        });

        assertEquals("high-cancel-rate", arguments.workloadName());
        assertEquals("coarse-lock", arguments.targetName());
        assertEquals(3, arguments.producerCount());
        assertEquals(1000, arguments.orderCountOverride());
        assertEquals(2, arguments.workerCountOverride());
        assertEquals(42L, arguments.seedOverride());
    }

    @Test
    void rejectsMalformedArgumentMissingEquals() {
        assertThrows(IllegalArgumentException.class, () -> Arguments.parse(new String[] {"--workload"}));
    }

    @Test
    void rejectsArgumentNotStartingWithDoubleDash() {
        assertThrows(IllegalArgumentException.class, () -> Arguments.parse(new String[] {"workload=steady"}));
    }

    @Test
    void rejectsUnknownOption() {
        assertThrows(
                IllegalArgumentException.class, () -> Arguments.parse(new String[] {"--bogus=1"}));
    }

    @Test
    void workloadAppliesOverridesOnTopOfTheNamedBaseline() {
        Arguments arguments = Arguments.parse(new String[] {
            "--workload=steady", "--orders=1234", "--workers=7", "--seed=99"
        });

        Workload resolved = arguments.workload();

        assertEquals("steady", resolved.name());
        assertEquals(1234, resolved.orderCount());
        assertEquals(7, resolved.workerCount());
        assertEquals(99L, resolved.seed());
        // Fields with no CLI override keep the named workload's own baseline value.
        assertEquals(Workload.steady().symbolCount(), resolved.symbolCount());
        assertEquals(Workload.steady().cancelRatio(), resolved.cancelRatio());
    }

    @Test
    void workloadWithoutOverridesMatchesTheNamedBaselineExactly() {
        Arguments arguments = Arguments.parse(new String[] {"--workload=many-symbols"});

        assertEquals(Workload.manySymbols(), arguments.workload());
    }

    @Test
    void buildTargetConstructsAndClosesEveryKnownTarget() {
        for (String targetName : new String[] {"router", "coarse-lock", "queue-array", "queue-linked"}) {
            Arguments arguments = Arguments.parse(new String[] {
                "--target=" + targetName, "--workload=steady", "--workers=1"
            });
            try (SubmissionTarget target = arguments.buildTarget(arguments.workload())) {
                assertEquals(0, target.queueDepth());
            }
        }
    }

    @Test
    void buildTargetRejectsUnknownTargetName() {
        Arguments arguments = Arguments.parse(new String[] {"--target=does-not-exist"});
        assertThrows(
                IllegalArgumentException.class, () -> arguments.buildTarget(arguments.workload()));
    }
}
