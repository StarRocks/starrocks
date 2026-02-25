// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.starrocks.sql.optimizer.skew;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.validation.constraints.NotNull;

public class DataSkew {
    private static final int DEFAULT_MCV_LIMIT = 5;
    private static final double DEFAULT_RELATIVE_ROW_THRESHOLD = 0.2;
    private static final Thresholds DEFAULT_THRESHOLDS = new Thresholds(DEFAULT_MCV_LIMIT, DEFAULT_RELATIVE_ROW_THRESHOLD);

    public record Thresholds(int mcvLimit, double relativeRowThreshold) {
        public static Thresholds withRelativeRowThreshold(double relativeRowThreshold) {
            return new Thresholds(DEFAULT_MCV_LIMIT, relativeRowThreshold);
        }

        public static Thresholds withMcvLimit(int mcvLimit) {
            return new Thresholds(mcvLimit, DEFAULT_RELATIVE_ROW_THRESHOLD);
        }
    }

    public enum SkewType {
        NOT_SKEWED,
        SKEWED_NULL,
        SKEWED_MCV
    }

    public record SkewInfo(SkewType type, Optional<List<Pair<String, Long>>> maybeMcvs) {
        public SkewInfo(SkewType type) {
            this(type, Optional.empty());
        }

        public boolean isSkewed() {
            return type != SkewType.NOT_SKEWED;
        }
    }

    public record SkewCandidates(boolean includeNull,
                                 List<Pair<String, Long>> mcvs,
                                 Optional<Double> nullSkewFactor,
                                 Optional<Double> mcvSkewFactor,
                                 Optional<Double> maxMcvRatio) {
        public boolean isSkewed() {
            return includeNull || (mcvs != null && !mcvs.isEmpty());
        }
    }

    private record McvSkewInfo(boolean skewed, Optional<Double> mcvSkewFactor, Optional<List<Pair<String, Long>>> mcvs) {
        public McvSkewInfo(boolean skewed) {
            this(skewed, Optional.empty(), Optional.empty());
        }
    }

    private static McvSkewInfo getMcvSkewInfo(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic,
                                              Thresholds thresholds) {
        final var rowCount = statistics.getOutputRowCount();
        final var histogram = columnStatistic.getHistogram();
        if (histogram != null) {
            final var mcv = histogram.getMCV();

            if (mcv != null) {
                int mcvLimit = Math.min(thresholds.mcvLimit, mcv.size());
                List<Pair<String, Long>> mcvs = Lists.newArrayList();
                mcv.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) //
                        .limit(mcvLimit)
                        .forEach(entry -> {
                            mcvs.add(Pair.create(entry.getKey(), entry.getValue()));
                        });

                long rowCountOfMcvs = mcvs.stream().mapToLong(pair -> pair.second).sum();
                final var mcvSkewFactor = rowCountOfMcvs / rowCount;

                if (mcvSkewFactor > thresholds.relativeRowThreshold) {
                    return new McvSkewInfo(true, Optional.of(mcvSkewFactor), Optional.of(mcvs));
                }
            }
        }

        return new McvSkewInfo(false);
    }

    private record NullSkewInfo(boolean skewed, Optional<Double> nullSkewFactor) {
        public NullSkewInfo(boolean skewed) {
            this(skewed, Optional.empty());
        }
    }

    private static NullSkewInfo getNullSkewInfo(@NotNull ColumnStatistic columnStatistic, Thresholds thresholds) {
        if (columnStatistic.getNullsFraction() >= thresholds.relativeRowThreshold) {
            // A lot of NULL values also indicate skew.
            return new NullSkewInfo(true, Optional.of(columnStatistic.getNullsFraction()));
        }

        return new NullSkewInfo(false);
    }

    /**
     * Utility method to get detailed information about if a column is skewed and how it is skewed.
     */
    public static SkewInfo getColumnSkewInfo(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic) {
        return getColumnSkewInfo(statistics, columnStatistic, DEFAULT_THRESHOLDS);
    }

    /**
     * Utility method to get detailed information about if a column is skewed and how it is skewed.
     */
    public static SkewInfo getColumnSkewInfo(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic,
                                             Thresholds thresholds) {
        if (statistics.isTableRowCountMayInaccurate() || statistics.getOutputRowCount() < 1) {
            // Without sufficient information we can not make a decision.
            return new SkewInfo(SkewType.NOT_SKEWED);
        }

        final var nullSkewInfo = getNullSkewInfo(columnStatistic, thresholds);
        final var mcvSkewInfo = getMcvSkewInfo(statistics, columnStatistic, thresholds);

        if (nullSkewInfo.skewed && mcvSkewInfo.skewed) {
            // If there is skew in the MCVs as well as NULLs, we decide for the one that is more skewed.
            if (nullSkewInfo.nullSkewFactor.get() >= mcvSkewInfo.mcvSkewFactor.get()) {
                return new SkewInfo(SkewType.SKEWED_NULL);
            } else {
                return new SkewInfo(SkewType.SKEWED_MCV, mcvSkewInfo.mcvs);
            }
        } else if (nullSkewInfo.skewed) {
            return new SkewInfo(SkewType.SKEWED_NULL);
        } else if (mcvSkewInfo.skewed) {
            return new SkewInfo(SkewType.SKEWED_MCV, mcvSkewInfo.mcvs);
        }

        // Can not deduce skew.
        return new SkewInfo(SkewType.NOT_SKEWED);
    }

    public static SkewCandidates getSkewCandidates(@NotNull Statistics statistics,
                                                   @NotNull ColumnStatistic columnStatistic,
                                                   Thresholds thresholds,
                                                   double singleMcvThreshold) {
        if (statistics.isTableRowCountMayInaccurate() || statistics.getOutputRowCount() < 1) {
            return new SkewCandidates(false, List.of(), Optional.empty(), Optional.empty(), Optional.empty());
        }

        final var rowCount = statistics.getOutputRowCount();
        final var nullSkewInfo = getNullSkewInfo(columnStatistic, thresholds);
        final var mcvSkewInfo = getMcvSkewInfo(statistics, columnStatistic, thresholds);

        boolean includeNull = nullSkewInfo.skewed;

        List<Pair<String, Long>> mcvCandidates = List.of();
        Optional<Double> maxMcvRatio = Optional.empty();
        if (mcvSkewInfo.skewed && mcvSkewInfo.mcvs.isPresent()) {
            List<Pair<String, Long>> topK = mcvSkewInfo.mcvs.get();
            // Filter candidates by per-value threshold if needed.
            if (singleMcvThreshold > 0) {
                topK = topK.stream()
                        .filter(p -> (p.second / rowCount) >= singleMcvThreshold)
                        .toList();
            }
            mcvCandidates = topK;

            maxMcvRatio = mcvCandidates.stream()
                    .mapToDouble(p -> p.second / rowCount)
                    .max()
                    .stream()
                    .boxed()
                    .findFirst();
        }

        return new SkewCandidates(includeNull, mcvCandidates,
                nullSkewInfo.nullSkewFactor, mcvSkewInfo.mcvSkewFactor, maxMcvRatio);
    }

    /**
     * Utility method to check if a certain column is skewed based on statistics.
     */
    public static boolean isColumnSkewed(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic,
                                         Thresholds thresholds) {
        return getColumnSkewInfo(statistics, columnStatistic, thresholds).isSkewed();
    }

    /* Always using default thresholds */
    public static boolean isColumnSkewed(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic) {
        return isColumnSkewed(statistics, columnStatistic, DEFAULT_THRESHOLDS);
    }
}
