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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.starrocks.analysis.TableName;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.PieceColumnPruner;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.pieces.TableUsage;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.qe.PartitionPlus;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OneOneMVGenerator {

    private static Optional<ColumnRefSet> tryToInferBucketKey(
            Map<Integer, Long> keyColumnReps, int nColumn,
            Function<ColumnRefSet, Long> hitCount,
            Predicate<ColumnRefSet> isGoodKey) {

        if (keyColumnReps.size() < nColumn) {
            return Optional.empty();
        }
        if (keyColumnReps.size() == nColumn) {
            ColumnRefSet key = ColumnRefSet.createByIds(keyColumnReps.keySet());
            if (isGoodKey.test(key)) {
                return Optional.of(key);
            } else {
                return Optional.empty();
            }
        }

        List<Integer> keyColumnsDescending = keyColumnReps.entrySet().stream()
                .map(e -> Pair.create(e.getKey(), e.getValue()))
                .sorted(Collections.reverseOrder(Pair.comparingBySecond()))
                .map(p -> p.first)
                .collect(Collectors.toList())
                .subList(0, Math.min(nColumn + 1, keyColumnReps.size()));

        ColumnRefSet candidateSuperKey = ColumnRefSet.createByIds(keyColumnsDescending);
        return keyColumnsDescending.stream()
                .map(id -> {
                    ColumnRefSet key = candidateSuperKey.clone();
                    key.except(ColumnRefSet.createByIds(Collections.singletonList(id)));
                    return key;
                })
                .filter(isGoodKey)
                .map(key -> Pair.create(key, hitCount.apply(key)))
                .sorted(Collections.reverseOrder(Pair.comparingBySecond()))
                .map(p -> p.first)
                .findFirst();
    }

    private static Optional<ColumnRefSet> inferBucketKey(TableUsage tableUsage) {
        Map<Integer, Long> keyColumnReps =
                Stream.concat(tableUsage.getGroupByKeys().stream(), tableUsage.getJoinKeys().stream())
                        .filter(key -> key.size() >= 2)
                        .flatMap(ColumnRefSet::getStream)
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Function<ColumnRefSet, Long> hitCount = key ->
                tableUsage.getJoinKeys().stream().filter(joinKey -> joinKey.containsAll(key)).count() +
                        tableUsage.getGroupByKeys().stream().filter(groupByKey -> groupByKey.containsAll(key)).count();

        Predicate<ColumnRefSet> isHighNdvKey =
                key -> tableUsage.getRowCount() > 1000000.0 && key.getStream().anyMatch(id ->
                        tableUsage.getColumnIdToNdvRatio().getOrDefault(id, 0.0) > 0.5);

        for (int i = 1; i < 4; ++i) {
            Optional<ColumnRefSet> optKey = tryToInferBucketKey(keyColumnReps, i, hitCount, isHighNdvKey);
            if (optKey.isPresent()) {
                return optKey;
            }
        }
        return tryToInferBucketKey(keyColumnReps, 4, hitCount, Predicates.alwaysTrue());
    }

    public static TablePiece addPartitionColumns(TablePiece tablePiece, PartitionExtractor extractor) {
        List<PartitionPlus> partitions = tablePiece.getPartitionColumns(extractor);
        Preconditions.checkState(partitions.size() == 1);
        List<Pair<Integer, GenericColumn>> partitionColumns = partitions.get(0).getPartitionColumns();
        ColumnRefSet newUsedColumns = tablePiece.getUsedColumns();
        partitionColumns.forEach(p -> newUsedColumns.union(p.first));
        TieredMap.Builder<Integer, GenericColumn> newColumnsBuilder = tablePiece.getColumns().newTier();
        partitionColumns.forEach(p -> {
            if (!tablePiece.getColumns().containsKey(p.first)) {
                newColumnsBuilder.put(p.first, p.second);
            }
        });
        TieredMap<Integer, GenericColumn> newColumns = newColumnsBuilder.build();
        return tablePiece.builder().mustCast(TablePiece.Builder.class)
                .setUsedColumns(newUsedColumns)
                .setColumns(newColumns)
                .build().cast();
    }

    public static Optional<QueryGenerateResult> generate(TableUsage tableUsage, MVGenerateContext context) {
        QueryGenerateContext queryGenerateContext = QueryGenerateContext.of11MV(tableUsage);
        PartitionExtractor partitionExtractor = context.getOptions().getPartitionExtractor();
        PlanPiece tablePiece = addPartitionColumns(tableUsage.getTablePiece(), partitionExtractor)
                .setConjuncts(TieredList.<Op>genesis());
        tablePiece = PieceColumnPruner.prune(tablePiece).cast();
        QueryGenerateResult result = QueryGenerator.generate(tablePiece, queryGenerateContext);

        TieredMap<Integer, ColumnAlias> columnAliases = result.getColumnAliases();

        Optional<ColumnRefSet> optCollocateBucketKey = inferBucketKey(tableUsage);

        List<Pair<Integer, GenericColumn>> candidateOrderKey = result.getOrderedDimensions()
                .stream()
                .filter(p -> p.second.getType().canDistributedBy())
                .collect(Collectors.toList());

        List<Pair<Integer, GenericColumn>> orderByColumns = Lists.newArrayList();

        final int maxOrderByColumns = context.getOptions().getMaxOrderByColumns();
        for (Pair<Integer, GenericColumn> columnPair : candidateOrderKey) {
            if (orderByColumns.size() >= maxOrderByColumns) {
                break;
            }
            orderByColumns.add(columnPair);
        }

        // if order key's column quota is not used up, we add some join key column into it
        // since runtime filter can get benefit from this.
        if (orderByColumns.size() < maxOrderByColumns) {
            ColumnRefSet joinColumnIds = ColumnRefSet.of();
            tableUsage.getJoinKeys().forEach(joinColumnIds::union);
            ColumnRefSet orderByColumnIds = ColumnRefSet.of();
            orderByColumns.forEach(p -> orderByColumnIds.union(p.first));
            ColumnRefSet remainJoinColumnIds = joinColumnIds.clone();
            remainJoinColumnIds.except(orderByColumnIds);
            for (Pair<Integer, GenericColumn> columnPair : candidateOrderKey) {
                if (orderByColumns.size() >= maxOrderByColumns) {
                    break;
                }
                if (remainJoinColumnIds.contains(columnPair.first)) {
                    orderByColumns.add(columnPair);
                }
            }
        }

        if (orderByColumns.isEmpty()) {
            return Optional.empty();
        }

        ColumnRefSet remainPredicateColumnIds = ColumnRefSet.createByIds(tableUsage.getConjunctFreq().keySet());
        ColumnRefSet orderByColumnIds =
                ColumnRefSet.createByIds(orderByColumns.stream().map(p -> p.first).collect(Collectors.toList()));
        remainPredicateColumnIds.except(orderByColumnIds);

        TieredList<PrettyPrinter> mvIndexes = result.getOrderedDimensions()
                .stream()
                .filter(p -> remainPredicateColumnIds.contains(p.first))
                .filter(p -> p.second.getType().isStringType())
                .map(p -> columnAliases.get(p.first).getName())
                .map(alias -> new PrettyPrinter()
                        .add("INDEX ")
                        .add(alias).add("_bitmap_index (").add(alias).add(") USING BITMAP"))
                .collect(TieredList.<PrettyPrinter>toList());

        PrettyPrinter mvSchema = new PrettyPrinter();
        TieredList<PrettyPrinter> mvColumns = result.getOrderedColumns().stream()
                .map(p -> new PrettyPrinter().add(columnAliases.get(p.first).getName()))
                .collect(TieredList.<PrettyPrinter>toList());
        TieredList<PrettyPrinter> mvFields = mvColumns.concat(mvIndexes);
        String defaultMVName = context.getMvNameGenerator().apply(result.getSubquery().getResult());
        MVName name = Objects.requireNonNull(MVName.parse(defaultMVName).orElse(null));
        TableName tableName = tablePiece.mustCast(TablePiece.class).getTable().getFqTableName();
        String mvName = Stream.of(
                name.getPrefix(),
                tableName.getCatalog(),
                tableName.getDb(),
                tableName.getTbl(),
                name.getCreateTime(),
                name.getDigest().substring(0, 8)
        ).filter(Objects::nonNull).collect(Collectors.joining("_")).replaceAll("_+", "_");

        mvSchema.add("CREATE MATERIALIZED VIEW").spaces(1).add(mvName).spaces(1).add("(").newLine();
        mvSchema.indentEnclose(() -> mvSchema.addSuperStepsWithNlDel(", ", mvFields));
        mvSchema.newLine().add(")").newLine();
        mvSchema.add("COMMENT").spaces(1).addDoubleQuoted("11-MV recommended by AutoMV").newLine();
        PartitionExtractor extractor = context.getOptions().getPartitionExtractor();

        Optional<PrettyPrinter> optPartitionExpr =
                PartitionPolicy.getPartitionClauseFor11MV(tablePiece, extractor, columnAliases);
        optPartitionExpr.ifPresent(mvSchema::addSuperStep);
        TieredMap<Integer, GenericColumn> columns = tablePiece.getColumns();

        List<String> bucketKey = optCollocateBucketKey.map(collocateBucketKey -> columns.keySet()
                .stream()
                .filter(collocateBucketKey::contains)
                .map(column -> columnAliases.get(column).getName())
                .collect(Collectors.toList())).orElseGet(Collections::emptyList);

        Pair<Boolean, PrettyPrinter> dist = DistributionPolicy.getDistribution(tablePiece, bucketKey);
        mvSchema.addSuperStep(dist.second);

        List<String> orderByItems = orderByColumns.stream()
                .map(p -> columnAliases.get(p.first))
                .map(ColumnAlias::getName).collect(Collectors.toList());
        mvSchema.add("ORDER BY (").addItems(", ", orderByItems).add(")").newLine();
        //TODO(by satanson): At AutoMV-L2 stage, it is hard to infer a robust mv refresh policy, so
        // MV expert should specify one. in future (since AutoMV-L3 stage), a sophisticated refresh
        // policy will be developed.
        mvSchema.add("REFRESH ASYNC START(\"2023-12-01 10:00:00\") EVERY(INTERVAL 1 DAY)").newLine();
        Optional<String> optCollocateGroup = optCollocateBucketKey.map(ignored -> mvName);
        mvSchema.addSuperStep(PropertiesPolicy.getProperties(tablePiece, columnAliases, optPartitionExpr.isPresent(),
                optCollocateGroup, dist.first));
        mvSchema.add("AS").newLine();
        mvSchema.addSuperStep(result.getSubquery());
        QueryGenerateResult mvResult = result.updateSubquery(mvSchema)
                .setMvName(mvName)
                .setTraceLog(result.getTraceLog().orElse(null))
                .setCoveredQueries(tablePiece.getCommonState().getCoveredQueries());
        return Optional.of(mvResult);
    }
}
