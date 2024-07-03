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

package com.starrocks.sql.automv.tunespace;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.sql.automv.column.ColumnRefToIdConverter;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.generator.QueryGenerateContext;
import com.starrocks.sql.automv.generator.QueryGenerator;
import com.starrocks.sql.automv.options.AutoMVOptions;
import com.starrocks.sql.automv.pieces.AggregatePiece;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pieces.PlanPiece;
import com.starrocks.sql.automv.pieces.PlanPieceBuilder;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.policies.AggregatePolicies;
import com.starrocks.sql.automv.policies.AggregatePolicy;
import com.starrocks.sql.automv.qe.ColumnPlus;
import com.starrocks.sql.automv.qe.TableInfo;
import com.starrocks.sql.automv.qe.TablePlus;
import com.starrocks.sql.automv.util.ColumnDescription;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.optimizer.OptExpression;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.sql.automv.qe.ColumnPlus.BIGINT;
import static com.starrocks.sql.automv.qe.ColumnPlus.DATETIME;
import static com.starrocks.sql.automv.qe.ColumnPlus.JSON;
import static com.starrocks.sql.automv.qe.ColumnPlus.VARBINARY;
import static com.starrocks.sql.automv.qe.ColumnPlus.VARCHAR;

public class PlanPieceInfo {
    public static final List<ColumnPlus> COLUMNS = collectColumns();
    private static final long CURRENT_VERSION = 1;
    @ColumnDescription(type = BIGINT, autoIncrement = true, isBucketColumn = true)
    private long id;
    @ColumnDescription(type = DATETIME)
    private Timestamp ts;
    @ColumnDescription(type = VARBINARY)
    private String originalQuery;
    @ColumnDescription(type = VARBINARY, nullable = true)
    private String query;
    @ColumnDescription(type = VARCHAR, len = 255)
    private Category category;
    @ColumnDescription(type = JSON)
    private PieceTraits traits;

    private static List<ColumnPlus> collectColumns() {
        return Stream.of(PlanPieceInfo.class.getDeclaredFields())
                .filter(ColumnPlus::isAcceptable)
                .map(ColumnPlus::fieldToColumn)
                .collect(ImmutableList.toImmutableList());
    }

    public static List<ColumnPlus> getColumns() {
        return COLUMNS;
    }

    // TablePlus returned by this function used to generating tunespace manipulating statements.
    public static TablePlus getTable(String fqTableName, int numBucket, int replicationNum) {
        List<Column> columns =
                getColumns().stream().map(ColumnPlus::getColumn).collect(ImmutableList.toImmutableList());

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        HashDistributionInfo distributionInfo = new HashDistributionInfo();
        distributionInfo.setBucketNum(numBucket);

        List<Column> bucketKey = getColumns().stream().filter(ColumnPlus::isBucketColumn)
                .map(ColumnPlus::getColumn)
                .collect(ImmutableList.toImmutableList());

        distributionInfo.setDistributionColumns(bucketKey);

        OlapTable table = new OlapTable(0xdeadbeef, fqTableName, columns, KeysType.PRIMARY_KEYS,
                partitionInfo, distributionInfo);

        return TablePlus.of(table, PlanPieceInfo.class, getColumns(), replicationNum);
    }

    public static PlanPieceInfo from(AutoMVOptions options, OptExpression subPlan, boolean enableTrace,
                                     Map<String, FQTable> fqTableMap) {
        ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
        PlanPiece planPiece = PlanPieceBuilder.createPlanPiece(subPlan, idConverter, fqTableMap);
        PrettyPrinter traceLog = enableTrace ? new PrettyPrinter() : null;
        AggregatePolicy policy = AggregatePolicies.defaultPolicies(options, traceLog);
        PlanPieceInfo planPieceInfo = PlanPieceInfo.from(planPiece, policy, fqTableMap);
        if (traceLog != null) {
            System.out.println(traceLog.getResult());
        }
        return planPieceInfo;
    }

    public static PlanPieceInfo fromLegacyMV(MaterializedViewPlus mvPlus, OptExpression entirePlan,
                                             Map<String, FQTable> fqTableMap) {
        ColumnRefToIdConverter idConverter = new ColumnRefToIdConverter();
        PlanPiece planPiece = PlanPieceBuilder.createPlanPiece(entirePlan, idConverter, fqTableMap);
        return PlanPieceInfo.fromLegacyMV(mvPlus, planPiece, fqTableMap);
    }

    private static PlanPieceInfo from(String originalQuery, String query, PlanPiece piece,
                                      Map<String, FQTable> fqTableMap) {
        PlanPieceInfo pieceInfo = new PlanPieceInfo();
        pieceInfo.setTs(new Timestamp(System.currentTimeMillis()));
        pieceInfo.setCategory(Category.MV);
        pieceInfo.setOriginalQuery(originalQuery);
        pieceInfo.setQuery(query);
        PieceTraits traits = new PieceTraits();
        Map<String, TableInfo> tables = fqTableMap.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, e -> TableInfo.from(e.getValue())));
        traits.setTables(tables);
        traits.setVersion(CURRENT_VERSION);
        piece.cast(AggregatePiece.class).ifPresent(aggPiece -> {
            traits.setNumDimensions(aggPiece.getDimensions().size());
            traits.setNumRollupDimensions(aggPiece.getRollupDimensions().size());
            traits.setNumMetrics(aggPiece.getMetrics().size());
            traits.setNumDistinctMetrics(aggPiece.getDistinctMetrics().size());

            Map<Boolean, TieredList<GenericColumn>> aggGroups = aggPiece.getMetrics().values()
                    .stream().collect(Collectors.partitioningBy(
                            AggregatePolicies::isRollupAble,
                            TieredList.<GenericColumn>toList()));

            TieredList<GenericColumn> rollupAbleAggs = aggGroups.get(true);
            aggGroups = aggGroups.get(false)
                    .stream().collect(Collectors.partitioningBy(
                            AggregatePolicies::isRollupConvertible,
                            TieredList.<GenericColumn>toList()));

            TieredList<GenericColumn> rollupConvertibleAggs = aggGroups.get(true);
            TieredList<GenericColumn> rollupUnableAggs = aggGroups.get(false);
            rollupConvertibleAggs = rollupConvertibleAggs.concat(aggPiece.getDistinctMetrics().values());

            traits.setAllRollupAble(rollupUnableAggs.isEmpty() && rollupConvertibleAggs.isEmpty());
            traits.setRollupConvertible(rollupUnableAggs.isEmpty());
            List<String> rollupAbleAggNames =
                    rollupAbleAggs.stream().map(Object::toString).collect(Collectors.toList());
            List<String> rollupConvertibleAggNames =
                    rollupConvertibleAggs.stream().map(Object::toString).collect(Collectors.toList());
            List<String> rollupUnableAggNames =
                    rollupUnableAggs.stream().map(Object::toString).collect(Collectors.toList());
            traits.setRollupAbleAggs(rollupAbleAggNames);
            traits.setRollupConvertibleAggs(rollupConvertibleAggNames);
            traits.setRollupUnableAggs(rollupUnableAggNames);
            traits.setNumHoistedConjuncts(aggPiece.getFlatTable().getConjuncts().size());
            List<String> hoistedConjuncts = aggPiece.getFlatTable().getConjuncts()
                    .stream().map(Op::toString).collect(Collectors.toList());
            traits.setHoistedConjuncts(hoistedConjuncts);
        });
        pieceInfo.setTraits(traits);
        return pieceInfo;
    }

    public static PlanPieceInfo fromLegacyMV(MaterializedViewPlus mvPlus, PlanPiece piece,
                                             Map<String, FQTable> fqTableMap) {
        piece = AggregatePolicies.perfectMatch(piece);
        String originalQuery = mvPlus.getCreateMaterializedViewSql();
        QueryGenerateContext context = QueryGenerateContext.of(false, true);
        String query = QueryGenerator.generate(piece, context).getSubquery().getResult();
        PlanPieceInfo pieceInfo = from(originalQuery, query, piece, fqTableMap);
        PieceTraits traits = pieceInfo.getTraits();
        traits.setLegacyMV(LegacyMVInfo.from(mvPlus));
        return pieceInfo;
    }

    public static PlanPieceInfo from(PlanPiece piece, AggregatePolicy policy,
                                     Map<String, FQTable> fqTableMap) {
        PlanPiece originalPiece = AggregatePolicies.perfectMatch(piece);
        QueryGenerateContext context = QueryGenerateContext.of(false, true);
        String originalQuery = QueryGenerator.generate(originalPiece, context).getSubquery().getResult();
        String query = piece.cast(AggregatePiece.class)
                .flatMap(policy::convert)
                .map(AggregatePolicies::perfectMatch)
                .map(aggPiece -> QueryGenerator.generate(aggPiece, context).getSubquery().getResult())
                .orElse(null);
        return from(originalQuery, query, piece, fqTableMap);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public void setOriginalQuery(String originalQuery) {
        this.originalQuery = originalQuery;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Category getCategory() {
        return category;
    }

    public void setCategory(Category category) {
        this.category = category;
    }

    public void setCategory(String category) {
        this.category = Category.valueOf(category);
    }

    public PieceTraits getTraits() {
        return traits;
    }

    public void setTraits(PieceTraits traits) {
        this.traits = traits;
    }

    public void setTraits(String traitsJson) {
        this.traits = new Gson().fromJson(traitsJson, PieceTraits.class);
    }

    public enum Category {
        KERNEL,
        CLOSURE,
        QUERY,
        MV
    }
}
