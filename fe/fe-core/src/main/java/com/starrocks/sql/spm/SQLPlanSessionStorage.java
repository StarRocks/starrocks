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
package com.starrocks.sql.spm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class SQLPlanSessionStorage implements SQLPlanStorage {
    private final List<BaselinePlan> baselinePlans = Lists.newArrayList();

    public List<BaselinePlan> getBaselines(Expr where) {
        if (where == null) {
            return baselinePlans;
        }

        List<BaselinePlan> bps = Lists.newArrayList();
        Map<String, ScalarOperator> valuesMappings = Maps.newHashMap();
        for (BaselinePlan baselinePlan : baselinePlans) {
            valuesMappings.put("id", ConstantOperator.createBigint(baselinePlan.getId()));
            valuesMappings.put("global", ConstantOperator.createBoolean(baselinePlan.isGlobal()));
            valuesMappings.put("enable", ConstantOperator.createBoolean(baselinePlan.isEnable()));
            valuesMappings.put("bindsqldigest", ConstantOperator.createVarchar(baselinePlan.getBindSqlDigest()));
            valuesMappings.put("bindsqlhash", ConstantOperator.createBigint(baselinePlan.getBindSqlHash()));
            valuesMappings.put("bindsql", ConstantOperator.createVarchar(baselinePlan.getBindSql()));
            valuesMappings.put("plansql", ConstantOperator.createVarchar(baselinePlan.getPlanSql()));
            valuesMappings.put("costs", ConstantOperator.createDouble(baselinePlan.getCosts()));
            valuesMappings.put("queryms", ConstantOperator.createDouble(baselinePlan.getQueryMs()));
            valuesMappings.put("source", ConstantOperator.createVarchar(baselinePlan.getSource()));
            valuesMappings.put("updatetime", ConstantOperator.createDatetime(baselinePlan.getUpdateTime()));

            ScalarOperator p = SqlToScalarOperatorTranslator.translateWithSlotRef(where,
                    slotRef -> valuesMappings.get(slotRef.getColumnName().toLowerCase()));
            ScalarOperatorRewriter re = new ScalarOperatorRewriter();
            ScalarOperator result = re.rewrite(p, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            if (!result.isConstantRef()) {
                throw UnsupportedException.unsupportedException("doesn't support predicate: " + where.toMySql());
            } else if (result.isConstantTrue()) {
                bps.add(baselinePlan);
            }
        }
        return bps;
    }

    @Override
    public void storeBaselinePlan(List<BaselinePlan> plans) {
        for (BaselinePlan plan : plans) {
            plan.setId(GlobalStateMgr.getCurrentState().getNextId());
            baselinePlans.add(plan);
        }
    }

    public List<BaselinePlan> findBaselinePlan(String sqlDigest, long hash) {
        return baselinePlans.stream().filter(p -> p.getBindSqlHash() == hash && p.getBindSqlDigest().equals(sqlDigest))
                .collect(Collectors.toList());
    }

    public void dropBaselinePlan(List<Long> baseLineIds) {
        baselinePlans.removeIf(p -> baseLineIds.contains(p.getId()));
    }

    public void dropAllBaselinePlans() {
        baselinePlans.clear();
    }

    @Override
    public void controlBaselinePlan(boolean isEnable, List<Long> baseLineIds) {
        for (BaselinePlan plan : baselinePlans) {
            if (baseLineIds.contains(plan.getId())) {
                plan.setEnable(isEnable);
            }
        }
    }
}
