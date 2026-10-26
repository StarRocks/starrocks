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

package com.starrocks.connector.statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.StatsConstants;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.ImmutableTriple;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StatisticsUtils {

    // Nominal relative error of the backend's HLL sketch: 1.04/sqrt(2^14 registers), about 0.8% (see
    // be/src/types/constexpr.h). U and S are two separate estimates over related sets, so anything
    // within a couple of sigma of a boundary is treated as sitting on it.
    private static final double HLL_RELATIVE_ERROR_BAND = 0.02;

    private static final Logger LOG = LogManager.getLogger(StatisticsUtils.class);

    public static Table getTableByUUID(ConnectContext context, String tableUUID) {
        String[] splits = tableUUID.split("\\.");

        Preconditions.checkState(splits.length == 4);
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, splits[0], splits[1], splits[2]);
        if (table == null) {
            throw new SemanticException("Table [%s.%s.%s] does not exist", splits[0], splits[1], splits[2]);
        }
        if (table.getUUID().equals(tableUUID)) {
            return table;
        } else {
            throw new SemanticException("Table [%s.%s.%s] does not exist", splits[0], splits[1], splits[2]);
        }
    }

    public static Triple<String, Database, Table> getTableTripleByUUID(ConnectContext context, String tableUUID) {
        String[] splits = tableUUID.split("\\.");

        Preconditions.checkState(splits.length == 4);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, splits[0], splits[1]);
        if (db == null) {
            throw new SemanticException("Database [%s.%s] does not exist", splits[0], splits[1]);
        }

        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, splits[0], splits[1], splits[2]);
        if (table == null) {
            throw new SemanticException("Table [%s.%s.%s] does not exist", splits[0], splits[1], splits[2]);
        }
        if (!table.getUUID().equals(tableUUID)) {
            throw new SemanticException("Table [%s.%s.%s] does not exist", splits[0], splits[1], splits[2]);
        }

        return ImmutableTriple.of(splits[0], db, table);
    }

    public static List<String> getTableNameByUUID(String tableUUID) {
        String[] splits = tableUUID.split("\\.");
        Preconditions.checkState(splits.length >= 3);
        return ImmutableList.of(splits[0], splits[1], splits[2]);
    }

    public static Statistics buildDefaultStatistics(Set<ColumnRefOperator> columns) {
        Statistics.Builder statisticsBuilder = Statistics.builder();
        statisticsBuilder.setOutputRowCount(Config.default_statistics_output_row_count);
        statisticsBuilder.addColumnStatistics(
                columns.stream().collect(Collectors.toMap(column -> column, column -> ColumnStatistic.unknown())));
        statisticsBuilder.setStatsSource(Statistics.StatsSource.NONE);
        return statisticsBuilder.build();
    }

    public static ConnectorTableColumnStats estimateColumnStatistics(Table table, String columnName,
                                                                     ConnectorTableColumnStats connectorTableColumnStats) {
        try (ConnectContext.ContextScope scope = ConnectContext.enterOnlyReadIcebergCacheScope(ConnectContext.get())) {
            Triple<String, Database, Table> tableIdentifier = getTableTripleByUUID(scope.getContext(), table.getUUID());
            ExternalBasicStatsMeta externalBasicStatsMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().
                    getExternalTableBasicStatsMeta(tableIdentifier.getLeft(), tableIdentifier.getMiddle().getFullName(),
                            tableIdentifier.getRight().getName());

            // Rows in external_column_statistics only ever reach the CBO through this method, and they are
            // per-partition rows aggregated with no partition filter and no completeness gate (see
            // StatisticSQLBuilder#buildQueryExternalFullStatisticsSQL). ColumnStatsMeta is what says how many
            // partitions those rows actually cover, so without it the aggregate is an unknown fraction of the
            // table. A collection job that dies partway can leave exactly that state behind: its rows are
            // flushed but its metadata is never committed. Trusting them as whole-table values hands the
            // optimizer a confident underestimate, which is worse than having no statistics at all - so treat
            // any row set we cannot size as unknown.
            if (externalBasicStatsMeta == null) {
                LOG.warn("External statistics rows exist without any ExternalBasicStatsMeta, treating column {} of " +
                        "table {} as unknown (a previous collection likely failed before committing metadata)",
                        columnName, table.getName());
                return ConnectorTableColumnStats.unknown();
            }

            Map<String, ColumnStatsMeta> columnStatsMetaMap = externalBasicStatsMeta.getColumnStatsMetaMap();
            if (!columnStatsMetaMap.containsKey(columnName)) {
                LOG.warn("External statistics rows exist without a ColumnStatsMeta, treating column {} of table {} " +
                        "as unknown (a previous collection likely failed before committing metadata)",
                        columnName, table.getName());
                return ConnectorTableColumnStats.unknown();
            }

            ColumnStatsMeta columnStatsMeta = columnStatsMetaMap.get(columnName);
            if (columnStatsMeta.getType() == StatsConstants.AnalyzeType.FULL) {
                return connectorTableColumnStats;
            }

            // The column statistics analyze type is sample, so scale it up to the whole table.
            //
            // The denominator comes from the rows themselves - how many partitions actually contributed
            // to the aggregate - rather than from the partition set recorded in the metadata. The two are
            // maintained on different paths: the rows in external_column_statistics accumulate across
            // runs, while the recorded set is written by whichever collection job last committed, and a
            // run that only covered part of what it asked for makes them disagree. Dividing a numerator
            // by a denominator derived from somewhere else is what produced both of the estimation bugs
            // this replaced; taken from the same aggregate, they cannot disagree by construction.
            //
            // A backend too old to report it sends 0, and then the recorded set is all there is.
            int sampledPartitionSize =
                    (int) Math.min(Integer.MAX_VALUE, connectorTableColumnStats.getCollectedPartitionCount());
            if (sampledPartitionSize <= 0) {
                sampledPartitionSize = columnStatsMeta.getSampledPartitionsHashValue().size();
            }
            int totalPartitionSize = columnStatsMeta.getAllPartitionSize();

            // A SAMPLE meta that covers no partition cannot scale anything (and would divide by zero).
            if (sampledPartitionSize <= 0 || totalPartitionSize <= 0) {
                LOG.warn("Unusable sampled partition counts in ColumnStatsMeta for column {} of table {}: " +
                        "sampled={} total={}, treating it as unknown",
                        columnName, table.getName(), sampledPartitionSize, totalPartitionSize);
                return ConnectorTableColumnStats.unknown();
            }

            double avgPartitionRowCount = connectorTableColumnStats.getRowCount() * 1.0 / sampledPartitionSize;
            // Round once, at the end. Truncating the average first - (long) avg * total - throws away
            // everything below a whole row per partition before it is multiplied back up, so a column
            // whose sampled partitions hold less than one row each scales to zero however large the
            // table is. The result also feeds the distinct-value ceiling below, so a zero row count
            // takes the distinct count down with it.
            long totalRowCount = Math.round(avgPartitionRowCount * totalPartitionSize);

            ColumnStatistic columnStatistic = scaleNdv(connectorTableColumnStats, sampledPartitionSize,
                    totalPartitionSize, totalRowCount);
            return new ConnectorTableColumnStats(columnStatistic, totalRowCount,
                    connectorTableColumnStats.getUpdateTime(),
                    connectorTableColumnStats.getCollectedPartitionCount(),
                    connectorTableColumnStats.getPerPartitionNdvSum());
        }
    }

    /**
     * Replaces the sampled distinct count with an estimate for the whole table.
     *
     * <p>Row counts are scaled by the ratio of partitions; the distinct count is not, and left alone it
     * is simply the number of distinct values in the partitions that happened to be sampled. That is the
     * single most consequential number the optimizer gets - join cardinality estimates are built on it -
     * and it is wrong in a fixed direction, so plans are wrong in a fixed direction too.
     *
     * <p>How wrong depends on something the samples themselves reveal: whether a column's values repeat
     * across partitions. Sum the per-partition distinct counts and compare with the merged distinct
     * count. If they are equal, each partition holds values no other partition has - an order key, a
     * time-derived id - and the table's distinct count grows with the table. If the merged count equals
     * one partition's worth, every partition draws from the same small set - a foreign key into a
     * dimension - and the table has no more distinct values than one partition does. Real columns fall
     * between, and the occupancy model below interpolates between those two ends instead of picking one.
     */
    private static ColumnStatistic scaleNdv(ConnectorTableColumnStats stats, int sampledPartitionSize,
                                            int totalPartitionSize, long totalRowCount) {
        ColumnStatistic columnStatistic = stats.getColumnStatistic();
        double mergedNdv = columnStatistic.getDistinctValuesCount();
        long perPartitionNdvSum = stats.getPerPartitionNdvSum();
        if (perPartitionNdvSum <= 0 || mergedNdv <= 0 || totalPartitionSize <= sampledPartitionSize
                || stats.getCollectedPartitionCount() <= 0) {
            // Nothing to work with (a backend too old to report the sum, or an empty column), or nothing
            // to scale to. Leaving the sampled value alone is what happened before this existed.
            return columnStatistic;
        }

        double estimated = extrapolateNdv(mergedNdv, perPartitionNdvSum, sampledPartitionSize, totalPartitionSize);
        // Never below what was actually seen, and never above the same growth applied to every partition.
        // Both are properties of the model, not of the data: they say the estimate is somewhere between
        // "the remaining partitions add nothing" and "they add as much as the sampled ones did".
        double linearCeiling = mergedNdv * totalPartitionSize / (double) sampledPartitionSize;
        estimated = Math.max(mergedNdv, Math.min(estimated, linearCeiling));

        // A column cannot hold more distinct values than it has non-null rows. Unlike the two above,
        // this is a physical bound, so it applies even below mergedNdv: a sampled distinct count above
        // the extrapolated non-null row count means the two disagree, and the row count is the one that
        // cannot be exceeded.
        // An unset null fraction arrives as NaN, which would silently disable the whole bound; with no
        // idea how many rows are null, every row is the honest ceiling.
        double nullsFraction = columnStatistic.getNullsFraction();
        boolean usableNullsFraction = !Double.isNaN(nullsFraction) && nullsFraction >= 0 && nullsFraction < 1;
        double nonNullRows = usableNullsFraction ? totalRowCount * (1 - nullsFraction) : totalRowCount;
        if (nonNullRows > 0) {
            estimated = Math.min(estimated, nonNullRows);
        }
        return ColumnStatistic.buildFrom(columnStatistic).setDistinctValuesCount(estimated).build();
    }

    /**
     * Estimates the distinct count over {@code totalPartitions} from what {@code sampledPartitions}
     * showed.
     *
     * <p>Model: every partition holds {@code d = S/P} distinct values drawn from a shared domain of
     * size {@code D}. The expected number of distinct values across P partitions is then
     * {@code D * (1 - (1 - d/D)^P)} - the coupon-collector curve. Both ends fall out of it rather than
     * being special-cased: when the partitions share nothing the curve is still straight and the
     * estimate grows in proportion; when they all hold the same values it does not grow at all.
     *
     * <p>Solved for {@code q = d/D} - the chance that one partition holds a given value - rather than
     * for D itself. q is in [0, 1] whatever the data says, while D has no upper bound derivable from
     * U, S, P and N: an earlier version bisected D over [d, S*N], and whenever U came close to S the
     * root lay outside that range and the search pinned itself to the wrong end.
     *
     * <p>What this cannot do: (U, S, P, N) do not identify the answer. A table whose unsampled
     * partitions repeat the sampled values and one whose every unsampled partition brings new values
     * produce the same four numbers and differ by a factor of N/P. The estimate is a bounded guess
     * between those two readings, which is worth having next to the alternative of not extrapolating
     * at all - it is not a measurement.
     */
    @VisibleForTesting
    static double extrapolateNdv(double mergedNdv, double perPartitionNdvSum, int sampledPartitions,
                                 int totalPartitions) {
        if (sampledPartitions <= 0 || totalPartitions <= sampledPartitions) {
            return mergedNdv;
        }
        double d = perPartitionNdvSum / sampledPartitions;
        if (d <= 0) {
            return mergedNdv;
        }
        double linear = mergedNdv * totalPartitions / (double) sampledPartitions;

        // For exact sets d <= U <= S: a value lives in at least one partition, and in no more than all
        // of them counted separately. Measured through HLL, both ends are only known to within the
        // sketch's own error, and a U sitting inside that error of an end is far better explained by
        // the end itself than by an overlap that happens to hide in the noise. Comparing exactly, as
        // an earlier version did, made the same disjoint column come out either at full linear growth
        // or well below it depending on which way the sketch happened to round.
        if (mergedNdv <= d * (1 + HLL_RELATIVE_ERROR_BAND)) {
            return mergedNdv;
        }
        if (mergedNdv >= perPartitionNdvSum * (1 - HLL_RELATIVE_ERROR_BAND)) {
            return linear;
        }

        // U/d = (1 - (1-q)^P) / q, which decreases from P at q -> 0 to 1 at q = 1, so the measured
        // ratio has exactly one q behind it. 100 halvings of [0, 1] is past the point where a double
        // tells the endpoints apart.
        double target = mergedNdv / d;
        double low = 0;
        double high = 1;
        for (int i = 0; i < 100; i++) {
            double mid = low + (high - low) / 2;
            if (occupancyRatio(mid, sampledPartitions) > target) {
                low = mid;
            } else {
                high = mid;
            }
        }

        // Same curve over every partition. Written as d * ratio rather than (d/q) * (...) so a tiny q
        // cannot blow the intermediate up to infinity before the multiplication brings it back.
        return d * occupancyRatio((low + high) / 2, totalPartitions);
    }

    // (1 - (1-q)^partitions) / q: how many distinct values `partitions` partitions cover, per
    // partition's worth. log1p/expm1 keep the precision when q is tiny and the exponent large, where
    // (1-q) would otherwise round to 1.
    private static double occupancyRatio(double q, int partitions) {
        if (q <= 0) {
            return partitions;
        }
        if (q >= 1) {
            return 1;
        }
        return -Math.expm1(partitions * Math.log1p(-q)) / q;
    }
}
