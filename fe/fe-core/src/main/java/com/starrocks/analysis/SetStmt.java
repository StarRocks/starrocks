// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.sql.ast.AstVisitor;

import java.util.List;

public class SetStmt extends StatementBase {
    private final List<SetVar> setVars;

    public SetStmt(List<SetVar> setVars) {
        this.setVars = setVars;
    }

    public List<SetVar> getSetVars() {
        return setVars;
    }

    @Override
    public boolean needAuditEncryption() {
        for (SetVar var : setVars) {
            if (var instanceof SetPassVar) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("SET ");
        int idx = 0;
        for (SetVar var : setVars) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append(var.toSql());
            idx++;
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (setVars != null) {
            for (SetVar var : setVars) {
                if (var instanceof SetPassVar) {
                    return RedirectStatus.FORWARD_WITH_SYNC;
                } else if (var.getType() == SetType.GLOBAL) {
                    return RedirectStatus.FORWARD_WITH_SYNC;
                }
            }
        }
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return setVars.stream().noneMatch(var -> var instanceof SetTransaction);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetStatement(this, context);
    }
}

