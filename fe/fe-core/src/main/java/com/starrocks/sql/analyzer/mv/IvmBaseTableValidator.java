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

package com.starrocks.sql.analyzer.mv;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Enterprise-only validation of a cloud-native base table's suitability for IVM. Kept in its own
 * file (not in the community-shared {@code IVMAnalyzer}) so upstream syncs of {@code IVMAnalyzer}
 * never conflict with this logic; {@code IVMAnalyzer} only calls {@link #validate}.
 *
 * <p>Two policies, both rooted in the append-only CDC delta stream:
 * <ul>
 *   <li>PRIMARY KEY / UNIQUE KEY bases are rejected: their replace/upsert semantics can't be
 *       maintained by an append-only delta (a key update emits an append, so the MV would keep
 *       the stale pre-update row alongside the new one).
 *   <li>An AGGREGATE KEY base MV must be a strict rollup: CDC delta on AGG_KEYS emits raw
 *       pre-merge rowset rows, so MV state matches a SELECT-from-base only when each MV group
 *       maps to whole post-merge base groups and each aggregate is invariant under base merging.
 * </ul>
 */
public final class IvmBaseTableValidator {

    private IvmBaseTableValidator() {
    }

    /**
     * Maps each base column aggregate type to the MV aggregate that is both delta-rollup-safe
     * (its state-union is invariant under base merging) AND end-to-end supported by IVM. Limited
     * to SUM / MAX / MIN: BITMAP_UNION / HLL_UNION / PERCENTILE_UNION are delta-safe too, but
     * IVM's combinator path ({@code IVMAnalyzer.IVM_SUPPORTED_AGG_FUNCTIONS}) does not cover them,
     * so advertising them here would only defer the rejection to a more confusing error. Row-
     * counting aggregates (COUNT, AVG, NDV) and REPLACE-family columns are excluded because CDC
     * delta carries pre-merge events whose count/last-value diverges from the base view.
     */
    private static final Map<AggregateType, String> COMPATIBLE_MV_AGG_BY_BASE_AGG_TYPE =
            ImmutableMap.<AggregateType, String>builder()
                    .put(AggregateType.SUM, FunctionSet.SUM)
                    .put(AggregateType.MAX, FunctionSet.MAX)
                    .put(AggregateType.MIN, FunctionSet.MIN)
                    .build();

    private static final Set<String> DELTA_ROLLUP_AGGS_FOR_AGG_BASE =
            ImmutableSet.copyOf(COMPATIBLE_MV_AGG_BY_BASE_AGG_TYPE.values());

    private static final Predicate<OlapTable> IS_AGG_KEYS = t -> t.getKeysType() == KeysType.AGG_KEYS;
    private static final Predicate<OlapTable> IS_REPLACE_FAMILY =
            t -> t.getKeysType() == KeysType.PRIMARY_KEYS || t.getKeysType() == KeysType.UNIQUE_KEYS;

    public static void validate(SelectRelation selectRelation) {
        Relation inner = selectRelation.getRelation();

        // Reject anywhere in the relation tree: a PK/UNIQUE join input is as unmaintainable as a sole base.
        OlapTable replaceBase = findAnyCloudNativeBase(inner, IS_REPLACE_FAMILY);
        if (replaceBase != null) {
            throw new SemanticException(
                    "IVM on cloud-native %s base '%s' is not supported: the CDC delta stream is " +
                            "append-only and cannot maintain replace/upsert semantics, so the MV " +
                            "would retain stale rows after a key update",
                    replaceBase.getKeysType(), replaceBase.getName());
        }

        OlapTable aggBase = findDirectCloudNativeAggBase(inner);
        if (aggBase == null) {
            OlapTable nested = findAnyCloudNativeBase(inner, IS_AGG_KEYS);
            if (nested != null) {
                throw new SemanticException(
                        "IVM on cloud-native AGGREGATE KEY base '%s' is only supported when the base " +
                                "table is the sole FROM source (no JOIN, UNION, or subquery)",
                        nested.getName());
            }
            return;
        }

        if (CollectionUtils.isEmpty(selectRelation.getGroupBy())) {
            throw new SemanticException(
                    "IVM on cloud-native AGGREGATE KEY base '%s' requires GROUP BY: the CDC delta " +
                            "stream emits raw pre-merge rowset rows, so a projection MV would store " +
                            "duplicates that the base table merges away on read",
                    aggBase.getName());
        }

        // Classify by base column NAME, not SlotRef.getColumn(): at analysis time the AST
        // SlotRef's slot descriptor is not yet bound (getColumn() is null — that binding happens
        // during planning), but getColumnName() already carries the resolved base column name
        // (qualifiers stripped, GROUP BY aliases resolved to the underlying column). Within the
        // single-AGG-base envelope (JOIN/UNION/subquery already rejected) names are unambiguous.
        Set<String> keyColumnNames = aggBase.getBaseSchema().stream()
                .filter(Column::isKey)
                .map(c -> c.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        for (Expr groupBy : selectRelation.getGroupBy()) {
            if (!(groupBy instanceof SlotRef)) {
                throw new SemanticException(
                        "IVM on cloud-native AGGREGATE KEY base '%s' only supports GROUP BY plain " +
                                "column references, got: %s",
                        aggBase.getName(), groupBy);
            }
            String colName = ((SlotRef) groupBy).getColumnName().toLowerCase(Locale.ROOT);
            if (!keyColumnNames.contains(colName)) {
                throw new SemanticException(
                        "IVM on cloud-native AGGREGATE KEY base '%s' requires GROUP BY columns to be " +
                                "a subset of the aggregate-key columns %s; '%s' is not an aggregate-key column",
                        aggBase.getName(), keyColumnNames, colName);
            }
        }

        Map<String, AggregateType> valueColumnAggTypes = aggBase.getBaseSchema().stream()
                .filter(c -> !c.isKey())
                .collect(Collectors.toMap(
                        c -> c.getName().toLowerCase(Locale.ROOT),
                        c -> c.getAggregationType() == null ? AggregateType.NONE : c.getAggregationType()));

        // A WHERE predicate on a value column is evaluated on raw pre-merge delta rows but on the
        // post-merge value in the base read, so the two diverge. Key-column predicates are safe
        // (every raw row for a key carries that key verbatim).
        Expr where = selectRelation.getPredicate();
        if (where != null) {
            List<SlotRef> refs = Lists.newArrayList();
            where.collect(SlotRef.class, refs);
            for (SlotRef ref : refs) {
                String colName = ref.getColumnName().toLowerCase(Locale.ROOT);
                if (valueColumnAggTypes.containsKey(colName)) {
                    throw new SemanticException(
                            "IVM on cloud-native AGGREGATE KEY base '%s' does not support a WHERE predicate on " +
                                    "value column '%s': the predicate is evaluated on raw pre-merge delta rows, " +
                                    "not the post-merge base value",
                            aggBase.getName(), colName);
                }
            }
        }

        for (FunctionCallExpr aggFunc : selectRelation.getAggregate()) {
            validateAggOnAggKeyBase(aggFunc, aggBase, valueColumnAggTypes);
        }
    }

    private static void validateAggOnAggKeyBase(FunctionCallExpr aggFunc, OlapTable aggBase,
                                                Map<String, AggregateType> valueColumnAggTypes) {
        String mvAggName = aggFunc.getFunctionName().toLowerCase(Locale.ROOT);
        if (!DELTA_ROLLUP_AGGS_FOR_AGG_BASE.contains(mvAggName)) {
            throw new SemanticException(
                    "IVM on cloud-native AGGREGATE KEY base '%s' does not support aggregate '%s'; " +
                            "AGG_KEYS base only accepts delta-rollup-compatible aggregates %s",
                    aggBase.getName(), mvAggName, DELTA_ROLLUP_AGGS_FOR_AGG_BASE);
        }
        if (aggFunc.getChildren().size() != 1 || !(aggFunc.getChildren().get(0) instanceof SlotRef)) {
            throw new SemanticException(
                    "IVM on cloud-native AGGREGATE KEY base '%s' requires aggregate over a single " +
                            "plain column reference, got: %s",
                    aggBase.getName(), aggFunc);
        }
        String colName = ((SlotRef) aggFunc.getChildren().get(0)).getColumnName().toLowerCase(Locale.ROOT);
        AggregateType baseColAggType = valueColumnAggTypes.get(colName);
        if (baseColAggType == null) {
            throw new SemanticException(
                    "IVM on cloud-native AGGREGATE KEY base '%s' does not support aggregate on " +
                            "aggregate-key column '%s'",
                    aggBase.getName(), colName);
        }
        if (baseColAggType.isReplaceFamily()) {
            throw new SemanticException(
                    "IVM on cloud-native AGGREGATE KEY base '%s' does not support aggregate on " +
                            "REPLACE/REPLACE_IF_NOT_NULL column '%s': replace semantics is order-dependent",
                    aggBase.getName(), colName);
        }
        if (!mvAggName.equals(COMPATIBLE_MV_AGG_BY_BASE_AGG_TYPE.get(baseColAggType))) {
            throw new SemanticException(
                    "IVM on cloud-native AGGREGATE KEY base '%s' requires MV aggregate to match the " +
                            "base column's aggregation type: got %s(%s) but base column '%s' has aggregation %s",
                    aggBase.getName(), mvAggName, colName, colName, baseColAggType);
        }
    }

    private static OlapTable findDirectCloudNativeAggBase(Relation relation) {
        if (!(relation instanceof TableRelation)) {
            return null;
        }
        return asCloudNativeBase(((TableRelation) relation).getTable(), IS_AGG_KEYS);
    }

    private static OlapTable findAnyCloudNativeBase(Relation relation, Predicate<OlapTable> match) {
        if (relation == null) {
            return null;
        }
        if (relation instanceof TableRelation) {
            return asCloudNativeBase(((TableRelation) relation).getTable(), match);
        }
        if (relation instanceof JoinRelation) {
            JoinRelation join = (JoinRelation) relation;
            OlapTable left = findAnyCloudNativeBase(join.getLeft(), match);
            return left != null ? left : findAnyCloudNativeBase(join.getRight(), match);
        }
        if (relation instanceof SubqueryRelation) {
            return findAnyCloudNativeBase(
                    ((SubqueryRelation) relation).getQueryStatement().getQueryRelation(), match);
        }
        if (relation instanceof UnionRelation) {
            for (QueryRelation child : ((UnionRelation) relation).getRelations()) {
                OlapTable found = findAnyCloudNativeBase(child, match);
                if (found != null) {
                    return found;
                }
            }
            return null;
        }
        if (relation instanceof SelectRelation) {
            return findAnyCloudNativeBase(((SelectRelation) relation).getRelation(), match);
        }
        return null;
    }

    private static OlapTable asCloudNativeBase(Table table, Predicate<OlapTable> match) {
        if (!(table instanceof OlapTable)) {
            return null;
        }
        OlapTable olap = (OlapTable) table;
        if (!olap.isCloudNativeTableOrMaterializedView()) {
            return null;
        }
        return match.test(olap) ? olap : null;
    }
}
