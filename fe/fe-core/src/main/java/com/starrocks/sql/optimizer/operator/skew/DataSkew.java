package com.starrocks.sql.optimizer.operator.skew;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import javax.validation.constraints.NotNull;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class DataSkew {
    private static final int DEFAULT_MCV_LIMIT = 5;
    private static final double DEFAULT_RELATIVE_ROW_THRESHOLD = 0.2;
    private static final Thresholds DEFAULT_THRESHOLDS = new Thresholds(DEFAULT_MCV_LIMIT, DEFAULT_RELATIVE_ROW_THRESHOLD);

    public record Thresholds(int mcvLimit, double relativeRowThreshold) {
    }

    /**
     * Utility method to check if a certain column is skewed based on statistics.
     */
    public static boolean isColumnSkewed(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic, Thresholds thresholds) {
        final var rowCount = statistics.getOutputRowCount();
        if (rowCount < 1 || columnStatistic.isUnknown() || columnStatistic.isUnknownValue()) {
            // Without sufficient information we can not make a decision.
            return false;
        }

        if (columnStatistic.getNullsFraction() >= thresholds.relativeRowThreshold) {
            // A lot of NULL values also indicate skew.
            return true;
        }

        final var histogram = columnStatistic.getHistogram();
        if (histogram != null) {
            final var mcv = histogram.getMCV();

            List<Pair<String, Long>> mcvs = Lists.newArrayList();
            int mcvLimit = Math.min(thresholds.mcvLimit, mcv.size());
            mcv.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) //
                    .limit(thresholds.mcvLimit)
                    .forEach(entry -> { // TODO(o.layer): Record class
                        mcvs.add(Pair.create(entry.getKey(), entry.getValue()));
                    });

            long rowCountOfMcvs = mcvs.stream().mapToLong(pair -> pair.second).sum();
            return ((double) rowCountOfMcvs / rowCount) > thresholds.relativeRowThreshold;
        }

        // No histogram information, can not deduce skew.
        return false;
    }

    /* Always using default thresholds */
    public static boolean isColumnSkewed(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic) {
       return isColumnSkewed(statistics, columnStatistic, DEFAULT_THRESHOLDS);
    }
}
