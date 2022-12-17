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


package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamJoinOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamScanOperator;
import org.apache.commons.lang3.NotImplementedException;

import java.util.EnumSet;
import java.util.Objects;

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
        return infer(optExpression.inputAt(0));
    }

    @Override
    public ModifyOp visitPhysicalProject(OptExpression optExpression, Void ctx) {
        return visit(optExpression.inputAt(0), ctx);
    }

    // TODO(murphy) read from user property, support custom the ModifyOp behavior
    private ModifyOp visitOlapTable(Table table) {
        if (!table.isOlapTable()) {
            throw new NotImplementedException("Only OLAP table is supported");
        }
        OlapTable olapTable = (OlapTable) table;
        ModifyOp res;
        if (olapTable.getKeysType().equals(KeysType.DUP_KEYS)) {
            res = ModifyOp.INSERT_ONLY;
        } else {
            res = ModifyOp.ALL;
        }
        return res;
    }

    @Override
    public ModifyOp visitPhysicalOlapScan(OptExpression optExpression, Void ctx) {
        PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getOp();
        return visitOlapTable(scan.getTable());
    }

    @Override
    public ModifyOp visitPhysicalStreamScan(OptExpression optExpression, Void ctx) {
        PhysicalStreamScanOperator scan = (PhysicalStreamScanOperator) optExpression.getOp();
        ModifyOp op = visitOlapTable(scan.getTable());
        scan.setModifyOp(op);
        return op;
    }

    @Override
    public ModifyOp visitPhysicalStreamJoin(OptExpression optExpression, Void ctx) {
        ModifyOp lhsOp = infer(optExpression.inputAt(0));
        ModifyOp rhsOp = infer(optExpression.inputAt(1));
        PhysicalStreamJoinOperator join = (PhysicalStreamJoinOperator) optExpression.getOp();

        // TODO(murphy) support outer join
        if (!join.getJoinType().isInnerJoin()) {
            throw new NotImplementedException("Only INNER JOIN is supported");
        }
        ModifyOp joinModify = ModifyOp.union(lhsOp, rhsOp);
        join.setModifyOp(joinModify);
        return joinModify;
    }

    @Override
    public ModifyOp visitPhysicalStreamAgg(OptExpression optExpression, Void ctx) {
        ModifyOp inputModify = infer(optExpression.inputAt(0));
        PhysicalStreamAggOperator agg = (PhysicalStreamAggOperator) optExpression.getOp();

        // TODO(murphy) optimize for non-retractable scenario
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
        public static final ModifyOp UPSERT = new ModifyOp(ModifyKind.UPSERT, UpdateKind.ONLY_AFTER);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ModifyOp modifyOp = (ModifyOp) o;
            return Objects.equals(modifySet, modifyOp.modifySet) && Objects.equals(updateSet, modifyOp.updateSet);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modifySet, updateSet);
        }

        @Override
        public String toString() {
            return "ModifyOp{" +
                    "modifySet=" + modifySet +
                    ", updateSet=" + updateSet +
                    '}';
        }
    }

    public static enum ModifyKind {
        INSERT,
        UPDATE,
        DELETE;

        public static final EnumSet<ModifyKind> ALL = EnumSet.of(INSERT, UPDATE, DELETE);
        public static final EnumSet<ModifyKind> NONE = EnumSet.noneOf(INSERT.getDeclaringClass());
        public static final EnumSet<ModifyKind> INSERT_ONLY = EnumSet.of(INSERT);
        public static final EnumSet<ModifyKind> UPSERT = EnumSet.of(INSERT, UPDATE);
    }

    public static enum UpdateKind {
        UPDATE_BEFORE,
        UPDATE_AFTER;

        public static final EnumSet<UpdateKind> NONE = EnumSet.noneOf(UPDATE_BEFORE.getDeclaringClass());
        public static final EnumSet<UpdateKind> ALL = EnumSet.of(UPDATE_BEFORE, UPDATE_AFTER);
        public static final EnumSet<UpdateKind> ONLY_AFTER = EnumSet.of(UPDATE_AFTER);
    }

}
