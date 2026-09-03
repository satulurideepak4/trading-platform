package com.tradingplatform.benchmark.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class WorkloadTest {

    @Test
    void namedReturnsSevenDistinctlyNamedWorkloads() {
        Workload[] workloads = Workload.named();

        assertEquals(7, workloads.length);
        Set<String> names = new HashSet<>();
        for (Workload workload : workloads) {
            names.add(workload.name());
        }
        assertEquals(7, names.size());
    }

    @Test
    void byNameFindsEveryNamedWorkload() {
        for (Workload workload : Workload.named()) {
            assertSame(workload.name(), Workload.byName(workload.name()).name());
        }
    }

    @Test
    void byNameRejectsUnknownWorkload() {
        assertThrows(IllegalArgumentException.class, () -> Workload.byName("does-not-exist"));
    }

    @Test
    void burstVariesOnlyOrdersPerSecond() {
        assertVariesInExactlyOneField(Workload.steady(), Workload.burst(), "ordersPerSecond");
    }

    @Test
    void singleHotSymbolVariesOnlySymbolCount() {
        assertVariesInExactlyOneField(Workload.steady(), Workload.singleHotSymbol(), "symbolCount");
        assertEquals(1, Workload.singleHotSymbol().symbolCount());
    }

    @Test
    void manySymbolsVariesOnlySymbolCount() {
        assertVariesInExactlyOneField(Workload.steady(), Workload.manySymbols(), "symbolCount");
    }

    @Test
    void highCancelRateVariesOnlyCancelRatio() {
        assertVariesInExactlyOneField(Workload.steady(), Workload.highCancelRate(), "cancelRatio");
        assertTrue(Workload.highCancelRate().cancelRatio() > 0);
    }

    @Test
    void highMatchRateVariesOnlyMatchBandTicks() {
        assertVariesInExactlyOneField(Workload.steady(), Workload.highMatchRate(), "matchBandTicks");
        assertTrue(Workload.highMatchRate().matchBandTicks() < Workload.steady().matchBandTicks());
    }

    @Test
    void lowMatchRateVariesOnlyMatchBandTicks() {
        assertVariesInExactlyOneField(Workload.steady(), Workload.lowMatchRate(), "matchBandTicks");
        assertTrue(Workload.lowMatchRate().matchBandTicks() > Workload.steady().matchBandTicks());
    }

    /**
     * Every named workload other than steady() itself is documented as varying from it by exactly
     * one field (plus its name). Reflection keeps this test honest against the record's actual
     * component list instead of hand-maintaining a duplicate field list here.
     */
    private static void assertVariesInExactlyOneField(Workload base, Workload variant, String field) {
        java.util.List<String> differing = new java.util.ArrayList<>();
        for (var component : Workload.class.getRecordComponents()) {
            if ("name".equals(component.getName())) {
                continue;
            }
            try {
                Object baseValue = component.getAccessor().invoke(base);
                Object variantValue = component.getAccessor().invoke(variant);
                if (!baseValue.equals(variantValue)) {
                    differing.add(component.getName());
                }
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
        }
        assertEquals(java.util.List.of(field), differing);
    }
}
