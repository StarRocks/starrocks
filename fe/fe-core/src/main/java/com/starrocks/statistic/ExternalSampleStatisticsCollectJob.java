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

package com.starrocks.statistic;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExternalSampleStatisticsCollectJob extends ExternalFullStatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalSampleStatisticsCollectJob.class);

    public ExternalSampleStatisticsCollectJob(String catalogName, Database db, Table table, List<String> partitionNames,
                                              List<String> columnNames, List<Type> columnTypes,
                                              StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                              Map<String, String> properties, List<String> allPartitionNames) {
        super(catalogName, db, table, partitionNames, columnNames, columnTypes, type, scheduleType, properties);
        this.allPartitionNames = allPartitionNames;
    }

    public Set<Long> getSampledPartitionsHashValue() {
        HashFunction hashFunction = Hashing.murmur3_128();
        return partitionNames.stream().map(s -> hashFunction.hashUnencodedChars(s).asLong()).collect(Collectors.toSet());
    }

    public int getAllPartitionSize() {
        return allPartitionNames.size();
    }

    // Hash set of every partition in the table (not just the sampled subset - contrast with
    // getSampledPartitionsHashValue). Used to give direct-value partition columns (see
    // StatisticUtils#isDirectValuePartitionColumn) an honest "sampled == all" partition record in
    // ColumnStatsMeta, so the standard SAMPLE row-count extrapolation formula
    // (rowCount / sampledPartitionSize * allPartitionSize) naturally comes out to a 1x no-op for them,
    // instead of needing a special read-time bypass.
    public Set<Long> getAllPartitionsHashValue() {
        HashFunction hashFunction = Hashing.murmur3_128();
        return allPartitionNames.stream().map(s -> hashFunction.hashUnencodedChars(s).asLong()).collect(Collectors.toSet());
    }

    // Direct-value partition columns (see StatisticUtils#isDirectValuePartitionColumn) get their exact
    // NDV by scanning every partition instead of just the sampled subset - this is metadata-cheap (one
    // value per partition, partition-pruned) and the merged HLL that comes out of a full scan is already
    // exact, so it needs no sample extrapolation. All other columns keep the normal sampled-partition
    // behavior.
    //
    // The two groups are run as two independent phases, each force-flushed on its own: if the
    // direct-value phase finishes fully, its ColumnStatsMeta is committed (with full-coverage partition
    // counts) immediately, before the sampled-column phase even starts - so a later failure in that
    // second phase can't retroactively erase an already-successful full-partition scan (see PR #60703
    // review discussion). If the direct-value phase itself fails partway through, ColumnStatsMeta for
    // those columns simply isn't updated this run - the existing (possibly still-SAMPLE, possibly
    // absent) metadata stays in effect and gets correctly re-extrapolated as usual, never
    // over-multiplied, until the next successful ANALYZE.
    @Override
    protected void runCollectPhases(ConnectContext context, AnalyzeStatus analyzeStatus, long jobId) throws Exception {
        List<String> directValueColumnNames = new ArrayList<>();
        List<Type> directValueColumnTypes = new ArrayList<>();
        List<String> restColumnNames = new ArrayList<>();
        List<Type> restColumnTypes = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            if (StatisticUtils.isDirectValuePartitionColumn(table, columnName)) {
                directValueColumnNames.add(columnName);
                directValueColumnTypes.add(columnTypes.get(i));
            } else {
                restColumnNames.add(columnName);
                restColumnTypes.add(columnTypes.get(i));
            }
        }

        int parallelism = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
        List<List<String>> directValueSQLList = buildCollectSQLListForColumns(allPartitionNames, directValueColumnNames,
                directValueColumnTypes, parallelism);
        List<List<String>> restSQLList = buildCollectSQLListForColumns(partitionNames, restColumnNames, restColumnTypes,
                parallelism);
        long totalCollectSQL = directValueSQLList.size() + restSQLList.size();

        long finishedSQLNum = 0;
        if (!directValueColumnNames.isEmpty()) {
            finishedSQLNum = executeCollectSQLList(directValueSQLList, context, analyzeStatus, finishedSQLNum,
                    totalCollectSQL, parallelism);
            flushInsertStatisticsData(context, true);
            commitDirectValueColumnsFully(directValueColumnNames);
        }
        if (!restColumnNames.isEmpty()) {
            executeCollectSQLList(restSQLList, context, analyzeStatus, finishedSQLNum, totalCollectSQL, parallelism);
        }
        flushInsertStatisticsData(context, true);
        cleanupStaleRawKeyedRows(context, jobId);
    }

    // Best-effort early commit of ColumnStatsMeta for columns whose full-partition scan just succeeded
    // (see runCollectPhases): records their sampled-partition set as ALL partitions, so the standard
    // extrapolation formula naturally becomes a 1x no-op for them. Failure here is non-fatal - losing
    // just this early-commit optimization only costs a redundant full re-scan of every partition on the
    // next ANALYZE, not correctness (the existing metadata, whatever it says, is still handled correctly
    // by the normal SAMPLE/FULL logic in connector.statistics.StatisticsUtils#estimateColumnStatistics).
    private void commitDirectValueColumnsFully(List<String> directValueColumnNames) {
        try {
            new StatisticExecutor().commitExternalColumnStatsMeta(db, table, getCatalogName(), columnNames,
                    directValueColumnNames, StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), properties,
                    Collections.emptySet(), getAllPartitionsHashValue(), getAllPartitionSize(), true);
        } catch (Exception e) {
            LOG.warn("Failed to eagerly commit ColumnStatsMeta for direct-value partition columns {} on table {}",
                    directValueColumnNames, table.getName(), e);
        }
    }

    @Override
    public String getName() {
        return "ExternalSample";
    }
}
