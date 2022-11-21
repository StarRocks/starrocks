// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SetVar.java

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

import com.google.common.base.Strings;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.UserResource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.system.HeartbeatFlags;

// change one variable.
public class SetVar {

    private String variable;
    private Expr value;
    private SetType type;
    private LiteralExpr result;

    public SetVar() {
    }

    public SetVar(SetType type, String variable, Expr value) {
        this.type = type;
        this.variable = variable;
        this.value = value;
        if (value instanceof LiteralExpr) {
            this.result = (LiteralExpr) value;
        }
    }

    public SetVar(String variable, Expr value) {
        this.type = SetType.DEFAULT;
        this.variable = variable;
        this.value = value;
        if (value instanceof LiteralExpr) {
            this.result = (LiteralExpr) value;
        }
    }

    public String getVariable() {
        return variable;
    }

    public LiteralExpr getValue() {
        return result;
    }

    public SetType getType() {
        return type;
    }

    public void setType(SetType type) {
        this.type = type;
    }

    // Value can be null. When value is null, means to set variable to DEFAULT.
    public void analyze(Analyzer analyzer) throws UserException {
        if (type == null) {
            type = SetType.DEFAULT;
        }

        if (Strings.isNullOrEmpty(variable)) {
            throw new AnalysisException("No variable name in set statement.");
        }

        if (type == SetType.GLOBAL) {
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ADMIN");
            }
        }

        if (value == null) {
            return;
        }

        // For the case like "set character_set_client = utf8", we change SlotRef to StringLiteral.
        if (value instanceof SlotRef) {
            value = new StringLiteral(((SlotRef) value).getColumnName());
        }

        try {
            value.analyze(analyzer);
        } catch (AnalysisException e) {
            throw new AnalysisException("Set statement only support constant expr.");
        }

        if (!value.isConstant()) {
            throw new AnalysisException("Set statement only support constant expr.");
        }

        final Expr literalExpr = value.getResultValue();
        if (!(literalExpr instanceof LiteralExpr)) {
            throw new AnalysisException("Set statement doesn't support computing expr:" + literalExpr.toSql());
        }

        result = (LiteralExpr) literalExpr;

        // Need to check if group is valid
        if (variable.equalsIgnoreCase(SessionVariable.RESOURCE_VARIABLE)) {
            if (!UserResource.isValidGroup(result.getStringValue())) {
                throw new AnalysisException("Invalid resource group, now we support {low, normal, high}.");
            }
        }

        if (variable.equalsIgnoreCase(GlobalVariable.DEFAULT_ROWSET_TYPE)) {
            if (result != null && !HeartbeatFlags.isValidRowsetType(result.getStringValue())) {
                throw new AnalysisException("Invalid rowset type, now we support {alpha, beta}.");
            }
        }

        if (getVariable().equalsIgnoreCase("prefer_join_method")) {
            String value = getValue().getStringValue();
            if (!value.equalsIgnoreCase("broadcast") && !value.equalsIgnoreCase("shuffle")) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, "prefer_join_method", value);
            }
        }

        // Check variable load_mem_limit value is valid
        if (getVariable().equalsIgnoreCase(SessionVariable.LOAD_MEM_LIMIT)) {
            String value = getValue().getStringValue();
            try {
                long load_mem_limit = Long.parseLong(value);
                if (load_mem_limit < 0) {
                    throw new AnalysisException(SessionVariable.LOAD_MEM_LIMIT + " must be equal or greater than 0.");
                }
            } catch (NumberFormatException ex) {
                throw new AnalysisException(SessionVariable.LOAD_MEM_LIMIT + " is not a number");
            }
        }

        // Check variable time_zone value is valid
        if (getVariable().equalsIgnoreCase(SessionVariable.TIME_ZONE)) {
            this.value = new StringLiteral(TimeUtils.checkTimeZoneValidAndStandardize(getValue().getStringValue()));
            this.result = (LiteralExpr) this.value;
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.EXEC_MEM_LIMIT)) {
            this.value = new StringLiteral(Long.toString(ParseUtil.analyzeDataVolumn(getValue().getStringValue())));
            this.result = (LiteralExpr) this.value;
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.SQL_SELECT_LIMIT)) {
            String value = getValue().getStringValue();
            try {
                long selectLimit = Long.parseLong(value);
                if (selectLimit < 0) {
                    throw new AnalysisException(SessionVariable.SQL_SELECT_LIMIT + " must be equal or greater than 0.");
                }
            } catch (NumberFormatException ex) {
                throw new AnalysisException(SessionVariable.SQL_SELECT_LIMIT + " is not a number");
            }
        }

        if (getVariable().equalsIgnoreCase(SessionVariable.QUERY_TIMEOUT)) {
            String value = getValue().getStringValue();
            try {
                long queryTimeout = Long.parseLong(value);
                if (queryTimeout <= 0) {
                    throw new AnalysisException(SessionVariable.QUERY_TIMEOUT + " must be equal or greater than 0.");
                }
                if (queryTimeout > SessionVariable.MAX_QUERY_TIMEOUT) {
                    throw new AnalysisException(SessionVariable.QUERY_TIMEOUT +
                            String.format(" must be equal or smaller than %d.", SessionVariable.MAX_QUERY_TIMEOUT));
                }
            } catch (NumberFormatException ex) {
                throw new AnalysisException(SessionVariable.SQL_SELECT_LIMIT + " is not a number");
            }
        }
    }

    public String toSql() {
        return type.toSql() + " " + variable + " = " + value.toSql();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
