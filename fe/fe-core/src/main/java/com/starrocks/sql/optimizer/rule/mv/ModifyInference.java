// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamOperator;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamScanOperator;
import org.apache.commons.lang3.NotImplementedException;

import java.util.EnumSet;

/**
 * Infer the ModifyOp for operator, include INSERT/UPDATE/DELETE
 */
public class ModifyInference extends OptExpressionVisitor<ModifyInference.ModifyOp, Void> {

    private static final ModifyInference INSTANCE = new ModifyInference();

    public static ModifyOp infer(OptExpression optExpr) {
        return optExpr.getOp().accept(INSTANCE, optExpr, null);
    }

    @Override
    public ModifyOp visit(OptExpression optExpression, Void ctx) {
        throw new NotImplementedException("Operator not supported: " + optExpression);
    }

    @Override
    public ModifyOp visitPhysicalFilter(OptExpression optExpression, Void ctx) {
        return visit(optExpression.inputAt(0), ctx);
    }

    @Override
    public ModifyOp visitPhysicalProject(OptExpression optExpression, Void ctx) {
        return visit(optExpression.inputAt(0), ctx);
    }

    @Override
    public ModifyOp visitPhysicalStreamScan(OptExpression optExpression, Void ctx) {
        PhysicalStreamScanOperator scan = (PhysicalStreamScanOperator) optExpression.getOp();
        Table table = scan.getTable();

        if (!table.isOlapTable()) {
            throw new NotImplementedException("Only OLAP table is supported");
        }
        OlapTable olapTable = (OlapTable) table;
        // TODO(murphy) read from user property, support custom the ModifyOp behavior
        ModifyOp res;
        if (olapTable.getKeysType().equals(KeysType.DUP_KEYS)) {
            res = ModifyOp.INSERT_ONLY;
        } else {
            res = ModifyOp.ALL;
        }
        scan.setModifyOp(res);
        return res;
    }

    @Override
    public ModifyOp visitPhysicalStreamJoin(OptExpression optExpression, Void ctx) {
        visit(optExpression.inputAt(0), ctx);
        visit(optExpression.inputAt(1), ctx);
        PhysicalStreamOperator lhs = (PhysicalStreamOperator) optExpression.inputAt(0).getOp();
        PhysicalStreamOperator rhs = (PhysicalStreamOperator) optExpression.inputAt(0).getOp();
        PhysicalStreamJoinOperator join = (PhysicalStreamJoinOperator) optExpression.getOp();

        // TODO(murphy) support outer join
        if (!join.getJoinType().isInnerJoin()) {
            throw new NotImplementedException("Only INNER JOIN is supported");
        }
        ModifyOp joinModify = ModifyOp.union(lhs.getModifyOp(), rhs.getModifyOp());
        join.setModifyOp(joinModify);
        return joinModify;
    }

    @Override
    public ModifyOp visitPhysicalStreamAgg(OptExpression optExpression, Void ctx) {
        visit(optExpression.inputAt(0), ctx);
        PhysicalStreamOperator inputOp = (PhysicalStreamOperator) optExpression.inputAt(0).getOp();
        PhysicalStreamAggOperator agg = (PhysicalStreamAggOperator) optExpression.inputAt(0).getOp();

        // TODO(murphy) optimize for non-retractable scenario
        ModifyOp inputModify = inputOp.getModifyOp();
        ModifyOp res;
        if (inputModify.isInsertOnly()) {
            res = ModifyOp.UPSERT;
        } else {
            res = ModifyOp.ALL;
        }
        agg.setModifyOp(res);
        return res;
    }

    public static class ModifyOp {
        private final EnumSet<ModifyKind> modifySet;
        private final EnumSet<UpdateKind> updateSet;

        public static final ModifyOp NONE = new ModifyOp(ModifyKind.NONE, UpdateKind.NONE);
        public static final ModifyOp INSERT_ONLY = new ModifyOp(ModifyKind.INSERT_ONLY, UpdateKind.NONE);
        public static final ModifyOp UPSERT = new ModifyOp(ModifyKind.INSERT_ONLY, UpdateKind.ONLY_AFTER);
        public static final ModifyOp ALL = new ModifyOp(ModifyKind.ALL, UpdateKind.ALL);

        public ModifyOp(EnumSet<ModifyKind> modifySet, EnumSet<UpdateKind> updateSet) {
            this.modifySet = modifySet;
            this.updateSet = updateSet;
        }

        public boolean isInsertOnly() {
            return modifySet.stream().allMatch(x -> x.equals(ModifyKind.INSERT));
        }

        public boolean needUpdateBefore() {
            return modifySet.contains(ModifyKind.UPDATE) && updateSet.contains(UpdateKind.UPDATE_BEFORE);
        }

        public static ModifyOp union(ModifyOp lhs, ModifyOp rhs) {
            EnumSet<ModifyKind> modifySet = lhs.modifySet.clone();
            lhs.modifySet.addAll(rhs.modifySet);
            EnumSet<UpdateKind> updateSet = lhs.updateSet.clone();
            lhs.updateSet.addAll(rhs.updateSet);
            return new ModifyOp(modifySet, updateSet);
        }

    }

    public static enum ModifyKind {
        INSERT,
        UPDATE,
        DELETE;

        public static final EnumSet<ModifyKind> ALL = EnumSet.of(INSERT, UPDATE, DELETE);
        public static final EnumSet<ModifyKind> NONE = EnumSet.noneOf(INSERT.getDeclaringClass());
        public static final EnumSet<ModifyKind> INSERT_ONLY = EnumSet.of(INSERT);

    }

    public static enum UpdateKind {
        UPDATE_BEFORE,
        UPDATE_AFTER;

        public static final EnumSet<UpdateKind> NONE = EnumSet.noneOf(UPDATE_BEFORE.getDeclaringClass());
        public static final EnumSet<UpdateKind> ALL = EnumSet.of(UPDATE_BEFORE, UPDATE_AFTER);
        public static final EnumSet<UpdateKind> ONLY_AFTER = EnumSet.of(UPDATE_AFTER);
    }

}
