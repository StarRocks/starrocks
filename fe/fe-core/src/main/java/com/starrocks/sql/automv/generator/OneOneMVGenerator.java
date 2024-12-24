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

package com.starrocks.sql.automv.generator;

import com.google.api.client.util.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TableUsage;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class OneOneMVGenerator {

    private static Optional<Set<Integer>> inferBucketKey(TableUsage tableUsage) {
        Map<Integer, Long> keyColumnReps =
                Stream.concat(tableUsage.getGroupByKeys().stream(), tableUsage.getJoinKeys().stream())
                        .filter(key -> key.size() >= 2)
                        .flatMap(ColumnRefSet::getStream)
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        if (keyColumnReps.size() < 2) {
            return Optional.empty();
        }
        if (keyColumnReps.size() == 2) {
            return Optional.of(keyColumnReps.keySet());
        }

        List<Integer> keyColumnsDescending = keyColumnReps.entrySet().stream()
                .map(e -> Pair.create(e.getKey(), e.getValue()))
                .sorted(Collections.reverseOrder(Pair.comparingBySecond()))
                .map(p -> p.first)
                .collect(Collectors.toList())
                .subList(0, Math.min(3, keyColumnReps.size()));

        Function<ColumnRefSet, Long> hitCount = key ->
                tableUsage.getJoinKeys().stream().filter(joinKey -> joinKey.containsAll(key)).count() +
                        tableUsage.getGroupByKeys().stream().filter(groupByKey -> groupByKey.containsAll(key)).count();

        return IntStream.range(0, keyColumnsDescending.size()).boxed()
                .flatMap(i -> IntStream.range(i + 1, keyColumnsDescending.size()).boxed().map(j -> Arrays.asList(i, j)))
                .map(ColumnRefSet::createByIds)
                .map(key -> Pair.create(key, hitCount.apply(key)))
                .sorted(Collections.reverseOrder(Pair.comparingBySecond()))
                .map(p -> p.first)
                .map(key -> key.getStream().collect(Collectors.toSet()))
                .findFirst();
    }

    public static Optional<QueryGenerateResult> generate(TableUsage tableUsage, MVGenerateContext context) {
        PrettyPrinter mvSchema = new PrettyPrinter();
        QueryGenerateContext queryGenerateContext = QueryGenerateContext.of11MV(tableUsage);
        PlanPiece tablePiece = tableUsage.getTablePiece().setConjuncts(tableUsage.getWhereConjuncts());
        QueryGenerateResult result = QueryGenerator.generate(tablePiece, queryGenerateContext);

        TieredMap<Integer, ColumnAlias> columnAliases = result.getColumnAliases();

        List<String> mvColumns = result.getOrderedColumns().stream()
                .map(p -> columnAliases.get(p.first))
                .map(ColumnAlias::getName).collect(Collectors.toList());

        String mvName = context.getMvNameGenerator().apply(result.getSubquery().getResult());
        mvSchema.add("CREATE MATERIALIZED VIEW").spaces(1).add(mvName).spaces(1).add("(").newLine();
        mvSchema.indentEnclose(() -> mvSchema.addItemsWithNlDel(", ", mvColumns));
        mvSchema.newLine().add(")").newLine();
        mvSchema.add("COMMENT").spaces(1).addDoubleQuoted("11-MV recommended by AutoMV").newLine();
        PartitionExtractor extractor = context.getOptions().getPartitionExtractor();

        Optional<PrettyPrinter> optPartitionExpr =
                PartitionPolicy.getPartitionClauseFor11MV(tablePiece, extractor, columnAliases);
        optPartitionExpr.ifPresent(mvSchema::addSuperStep);

        Optional<Set<Integer>> optCollocateBucketKey = inferBucketKey(tableUsage);

        List<Pair<Integer, GenericColumn>> bucketKey = result.getOrderedDimensions();
        if (optCollocateBucketKey.isPresent()) {
            Set<Integer> collocateBucketKey = optCollocateBucketKey.get();
            bucketKey = result.getOrderedDimensions().stream()
                    .filter(p -> collocateBucketKey.contains(p.first))
                    .collect(Collectors.toList());
        }
        bucketKey = bucketKey.stream()
                .filter(p -> p.second.getType().canDistributedBy())
                .collect(Collectors.toList());

        List<String> mvDimensionColumns = bucketKey.stream()
                .map(p -> columnAliases.get(p.first))
                .map(ColumnAlias::getName).collect(Collectors.toList());

        mvSchema.addSuperStep(DistributionPolicy.getDistribution(tablePiece, mvDimensionColumns));
        List<Pair<Integer, GenericColumn>> candidateOrderByColumns = Lists.newArrayList();

        final int maxOrderByColumns = context.getOptions().getMaxOrderByColumns();
        for (Pair<Integer, GenericColumn> columnPair : bucketKey) {
            if (candidateOrderByColumns.size() >= maxOrderByColumns) {
                break;
            }
            candidateOrderByColumns.add(columnPair);
        }
        if (candidateOrderByColumns.isEmpty()) {
            return Optional.empty();
        }

        List<String> orderByItems = candidateOrderByColumns.stream()
                .map(p -> columnAliases.get(p.first))
                .map(ColumnAlias::getName).collect(Collectors.toList());
        mvSchema.add("ORDER BY (").addItems(", ", orderByItems).add(")").newLine();
        //TODO(by satanson): At AutoMV-L2 stage, it is hard to infer a robust mv refresh policy, so
        // MV expert should specify one. in future (since AutoMV-L3 stage), a sophisticated refresh
        // policy will be developed.
        mvSchema.add("REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)").newLine();
        Optional<String> optCollocateGroup = optCollocateBucketKey.map(ignored -> mvName);
        mvSchema.addSuperStep(PropertiesPolicy.getProperties(tablePiece, columnAliases, optPartitionExpr.isPresent(),
                optCollocateGroup));
        mvSchema.add("AS").newLine();
        mvSchema.addSuperStep(result.getSubquery());
        QueryGenerateResult mvResult = result.updateSubquery(mvSchema)
                .setMvName(mvName)
                .setTraceLog(result.getTraceLog().orElse(null))
                .setCoveredQueries(tablePiece.getCommonState().getCoveredQueries());
        return Optional.of(mvResult);
    }
}
