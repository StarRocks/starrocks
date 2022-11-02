// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamOperator;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Infer key for each operator from the query plan
 */
public class KeyInference extends OptExpressionVisitor<KeyInference.KeyPropertySet, ExecPlan> {

    private static final KeyInference INSTANCE = new KeyInference();

    private KeyInference() {
    }

    public static KeyPropertySet infer(OptExpression optExpr, ExecPlan ctx) {
        KeyPropertySet keySet = optExpr.getOp().accept(INSTANCE, optExpr, ctx);
        return keySet;
    }

    @Override
    public KeyPropertySet visit(OptExpression optExpression, ExecPlan ctx) {
        throw new NotImplementedException("Operator not supported:" + optExpression);
    }

    @Override
    public KeyPropertySet visitPhysicalFilter(OptExpression optExpression, ExecPlan ctx) {
        return visit(optExpression.inputAt(0), ctx);
    }

    @Override
    public KeyPropertySet visitPhysicalProject(OptExpression optExpression, ExecPlan ctx) {
        KeyPropertySet keySet = visit(optExpression.inputAt(0), ctx);
        PhysicalProjectOperator project = (PhysicalProjectOperator) optExpression.getOp();
        ColumnRefSet projectColumns = new ColumnRefSet(project.getOutputColumns());

        List<KeyProperty> projectKeys =
                keySet.getKeys().stream().filter(key -> key.columns.containsAll(projectColumns)).collect(Collectors.toList());
        return new KeyPropertySet(projectKeys);
    }

    @Override
    public KeyPropertySet visitPhysicalOlapScan(OptExpression optExpression, ExecPlan ctx) {
        PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getOp();
        Table table = scan.getTable();
        Preconditions.checkState(table.isOlapTable());
        OlapTable olapTable = (OlapTable) table;

        List<ColumnRefOperator> keyColumnRefs;
        boolean unique = olapTable.getKeysType().equals(KeysType.PRIMARY_KEYS);
        if (scan.getProjection() == null) {
            keyColumnRefs = scan.getRealOutputColumns().stream()
                    .filter(x -> olapTable.getColumn(x.getName()).isKey())
                    .collect(Collectors.toList());
        } else {
            // TODO(murphy) support monotonic project expression
            keyColumnRefs = new ArrayList<>();
            Projection projection = scan.getProjection();
            for (ColumnRefOperator columnRef : projection.getOutputColumns()) {
                if (columnRef.getUsedColumns().cardinality() == 1) {
                    ScalarOperator ref = projection.getColumnRefMap().get(columnRef).getChild(0);
                    if (ref instanceof ColumnRefOperator) {
                        Column realColumn = scan.getColRefToColumnMetaMap().get(ref);
                        if (realColumn != null && realColumn.isKey()) {
                            keyColumnRefs.add(columnRef);
                        }
                    }
                }
            }
        }

        KeyProperty key = KeyProperty.of(new ColumnRefSet(keyColumnRefs), unique);
        KeyPropertySet res = new KeyPropertySet();
        res.addKeys(key);
        return res;
    }

    @Override
    public KeyPropertySet visitPhysicalStreamScan(OptExpression optExpression, ExecPlan ctx) {
        PhysicalStreamScanOperator scan = (PhysicalStreamScanOperator) optExpression.getOp();
        Table table = scan.getTable();
        Preconditions.checkState(table.isOlapTable());
        OlapTable olapTable = (OlapTable) table;

        boolean unique = olapTable.getKeysType().equals(KeysType.PRIMARY_KEYS);
        List<ColumnRefOperator> keyColumnRefs = scan.getOutputColumns().stream()
                .filter(x -> olapTable.getColumn(x.getName()).isKey())
                .collect(Collectors.toList());
        KeyProperty key = KeyProperty.of(new ColumnRefSet(keyColumnRefs), unique);
        scan.addKeyProperty(key);

        return scan.getKeyPropertySet();
    }

    @Override
    public KeyPropertySet visitPhysicalStreamJoin(OptExpression optExpression, ExecPlan ctx) {
        PhysicalStreamOperator lhsOp = (PhysicalStreamOperator) optExpression.inputAt(0).getOp();
        PhysicalStreamOperator rhsOp = (PhysicalStreamOperator) optExpression.inputAt(1).getOp();
        PhysicalStreamJoinOperator join = (PhysicalStreamJoinOperator) optExpression.getOp();

        if (!join.getJoinType().isInnerJoin()) {
            throw new NotImplementedException("Only INNER JOIN is supported");
        }

        // Join unique keys come from:
        // 1. Combination of left unique-keys and right unique-keys
        // 2. Left unique keys if right join columns are unique on join conjuncts
        // 3. Right unique keys if left join columns are unique on join conjuncts
        KeyPropertySet lhsKeySet = lhsOp.getKeyPropertySet();
        KeyPropertySet rhsKeySet = rhsOp.getKeyPropertySet();
        JoinHelper joinHelper =
                JoinHelper.of(join, optExpression.getChildOutputColumns(0), optExpression.getChildOutputColumns(1));
        List<Integer> lhsJoinColumns = joinHelper.getLeftOnColumns();
        List<Integer> rhsJoinColumns = joinHelper.getRightOnColumns();
        KeyPropertySet resKeySet = new KeyPropertySet();

        boolean rhsUnique = rhsKeySet.getKeys().stream().anyMatch(key -> key.columns.containsAll(rhsJoinColumns));
        boolean lhsUnique = lhsKeySet.getKeys().stream().anyMatch(key -> key.columns.containsAll(lhsJoinColumns));
        if (!lhsKeySet.empty() && rhsUnique) {
            resKeySet.addKeys(lhsKeySet.getKeys());
        }
        if (!rhsKeySet.empty() && lhsUnique) {
            resKeySet.addKeys(rhsKeySet.getKeys());
        }

        for (KeyProperty lhsKey : lhsKeySet.getKeys()) {
            for (KeyProperty rhsKey : rhsKeySet.getKeys()) {
                KeyProperty joinUniqueKey = KeyProperty.combine(lhsKey, rhsKey);
                resKeySet.addKeys(joinUniqueKey);
            }
        }

        join.setKeyPropertySet(resKeySet);
        return resKeySet;
    }

    @Override
    public KeyPropertySet visitPhysicalStreamAgg(OptExpression optExpression, ExecPlan ctx) {
        Operator input = optExpression.inputAt(0).getOp();
        PhysicalStreamAggOperator agg = (PhysicalStreamAggOperator) optExpression.getOp();

        if (CollectionUtils.isNotEmpty(agg.getGroupBys())) {
            KeyProperty keyProperty = KeyProperty.ofUnique(new ColumnRefSet(agg.getGroupBys()));
            agg.addKeyProperty(keyProperty);
        } else {
            throw new NotImplementedException("StreamAggregation without group by is not supported");
        }

        return agg.getKeyPropertySet();
    }

    public static class KeyProperty {
        public final boolean unique;
        public final ColumnRefSet columns;

        public KeyProperty(ColumnRefSet columns, boolean unique) {
            this.columns = columns;
            this.unique = unique;
        }

        public static KeyProperty of(ColumnRefSet columns) {
            return new KeyProperty(columns, false);
        }

        public static KeyProperty of(ColumnRefSet columns, boolean unique) {
            return new KeyProperty(columns, unique);
        }

        public static KeyProperty ofUnique(ColumnRefSet columns) {
            return new KeyProperty(columns, true);
        }

        public static KeyProperty combine(KeyProperty lhs, KeyProperty rhs) {
            ColumnRefSet columns = new ColumnRefSet();
            columns.union(lhs.columns);
            columns.union(rhs.columns);
            return new KeyProperty(columns, lhs.unique || rhs.unique);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyProperty that = (KeyProperty) o;
            return unique == that.unique && Objects.equals(columns, that.columns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(unique, columns);
        }
    }

    public static class KeyPropertySet {
        private List<KeyProperty> keySet = new ArrayList<>();

        public KeyPropertySet() {
        }

        public KeyPropertySet(List<KeyProperty> keys) {
            this.keySet = keys;
        }

        public boolean empty() {
            return CollectionUtils.isEmpty(keySet);
        }

        public List<KeyProperty> getKeys() {
            return keySet;
        }

        public void addKeys(List<KeyProperty> keys) {
            this.keySet.addAll(keys);
        }

        public void addKeys(KeyProperty key) {
            this.keySet.add(key);
        }
    }

}
