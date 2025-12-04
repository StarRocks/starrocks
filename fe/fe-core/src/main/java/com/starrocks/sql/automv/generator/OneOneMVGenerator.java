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
import com.google.common.collect.Maps;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.ColumnAlias;
import com.starrocks.sql.automv.column.DerivedColumn;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pieces.PieceColumnPruner;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.pieces.TableUsage;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.TimeGranule;
import com.starrocks.sql.automv.qe.PartitionExtractor;
import com.starrocks.sql.automv.qe.PartitionPlus;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OneOneMVGenerator {

    private static Optional<ColumnRefSet> inferBucketKey(TableUsage tableUsage) {
        Map<ColumnRefSet, Long> keyColumnReps =
                Stream.concat(tableUsage.getGroupByKeys().stream(), tableUsage.getJoinKeys().stream())
                        .filter(key -> !key.isEmpty())
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Map<ColumnRefSet, Long> keyColumnAccReps = Maps.newHashMap();

        for (Map.Entry<ColumnRefSet, Long> keyRep : keyColumnReps.entrySet()) {
            ColumnRefSet key = keyRep.getKey();
            Long reps = keyRep.getValue();
            Long accReps = reps + keyColumnReps.entrySet().stream()
                    .filter(e -> e.getKey().size() > key.size() && e.getKey().containsAll(key))
                    .map(Map.Entry::getValue).reduce(0L, Long::sum);
            keyColumnAccReps.put(key, accReps);
        }
        Map<ColumnRefSet, Long> extraKeyColumnAccReps = Maps.newHashMap();
        ColumnRefSet[] keys = keyColumnReps.keySet().toArray(new ColumnRefSet[0]);
        for (int i = 0; i < keys.length; ++i) {
            for (int j = i + 1; j < keys.length; ++j) {
                ColumnRefSet key1 = keys[i];
                ColumnRefSet key2 = keys[j];
                ColumnRefSet intersectKey = key1.clone();
                intersectKey.intersect(key2);
                if (intersectKey.isEmpty() || keyColumnAccReps.containsKey(intersectKey)) {
                    continue;
                }
                extraKeyColumnAccReps.put(intersectKey, keyColumnAccReps.get(key1) + keyColumnAccReps.get(key2));
            }
        }

        keyColumnAccReps.putAll(extraKeyColumnAccReps);

        Long thirdsTotalAccReps = keyColumnReps.values().stream().reduce(0L, Long::sum) / 3;
        Predicate<Pair<ColumnRefSet, Long>> isHighNdvKey =
                keyRep -> (tableUsage.getRowCount() == -1 && keyRep.first.size() >= 2 &&
                        keyRep.second >= thirdsTotalAccReps) ||
                        (tableUsage.getRowCount() > 1000000.0 && keyRep.first.getStream().anyMatch(id ->
                                tableUsage.getColumnIdToNdvRatio().getOrDefault(id, 0.0) > 0.5));

        return keyColumnAccReps.entrySet().stream()
                .filter(keyRep -> isHighNdvKey.test(Pair.create(keyRep.getKey(), keyRep.getValue())))
                .max(Comparator.comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getKey);
    }

    public static TablePiece adjustColumns(TablePiece tablePiece, PartitionExtractor extractor) {
        List<PartitionPlus> partitions = tablePiece.getPartitionColumns(extractor);
        Preconditions.checkState(partitions.size() == 1);
        List<Pair<Integer, GenericColumn>> partitionColumns = partitions.get(0).getPartitionColumns();
        ColumnRefSet partitionColumnIds = ColumnRefSet.of();
        ColumnRefSet newUsedColumns = tablePiece.getUsedColumns();
        partitionColumns.forEach(p -> partitionColumnIds.union(p.first));

        newUsedColumns.union(partitionColumnIds);
        TieredMap.Builder<Integer, GenericColumn> newColumnsBuilder = TieredMap.newGenesisTier();
        tablePiece.getColumns().forEach((id, column) -> {
            boolean shouldRetain = column.cast(DerivedColumn.class).map(GenericColumn::getOp)
                    .map(op -> !op.isVal() && !partitionColumnIds.containsAll(op.getIds()))
                    .orElse(true);
            if (shouldRetain) {
                newColumnsBuilder.put(id, column);
            } else {
                newUsedColumns.except(ColumnRefSet.createByIds(Collections.singleton(id)));
            }
        });

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
        PlanPiece tablePiece = adjustColumns(tableUsage.getTablePiece(), partitionExtractor)
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

        Optional<Pair<TimeGranule, PrettyPrinter>> optGranuleAndPartitionExpr =
                PartitionPolicy.getPartitionClauseFor11MV(tablePiece, extractor, columnAliases);
        Optional<PrettyPrinter> optPartitionExpr = optGranuleAndPartitionExpr.map(pair -> pair.second);
        Optional<TimeGranule> optGranule = optGranuleAndPartitionExpr.map(pair -> pair.first);

        optPartitionExpr.ifPresent(mvSchema::addSuperStep);

        TieredMap<Integer, GenericColumn> columns = tablePiece.getColumns();

        List<String> bucketKey = optCollocateBucketKey.map(collocateBucketKey -> columns.keySet()
                .stream()
                .filter(collocateBucketKey::contains)
                .map(column -> columnAliases.get(column).getName())
                .collect(Collectors.toList())).orElseGet(Collections::emptyList);

        boolean partitionIsScarce =
                optGranule.map(granule -> granule.getUnit().compareTo(TimeGranule.Unit.MONTH) > 0).orElse(true);

        Pair<Boolean, PrettyPrinter> dist =
                DistributionPolicy.getDistribution(tablePiece, bucketKey, partitionIsScarce);
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
