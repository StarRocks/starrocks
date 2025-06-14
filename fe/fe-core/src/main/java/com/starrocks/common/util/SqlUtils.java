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

package com.starrocks.common.util;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetTransaction;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import org.apache.commons.lang3.StringUtils;

public class SqlUtils {

    private static final int SQL_PREFIX_LENGTH = 128;

    public static String escapeUnquote(String ident) {
        return ident.replaceAll("``", "`");
    }

    public static String getIdentSql(String ident) {
        StringBuilder sb = new StringBuilder();
        sb.append('`');
        for (char ch : ident.toCharArray()) {
            if (ch == '`') {
                sb.append("``");
            } else {
                sb.append(ch);
            }
        }
        sb.append('`');
        return sb.toString();
    }

    /**
     * Return the prefix of a sql if it's too long
     */
    public static String sqlPrefix(String sql) {
        if (StringUtils.isEmpty(sql) || sql.length() < SQL_PREFIX_LENGTH) {
            return sql;
        }
        return sql.substring(0, SQL_PREFIX_LENGTH) + "...";
    }

    /**
     *  Pre-query SQL is the SQL sent before the user queries, such as select @@xxx (JDBC) and set query_timeout=xxx.
     *  Do not close the connection after such query during graceful exit
     *  because user queries will be sent on this connection after such query.
     * @param parsedStmt parseStmt
     * @return true/false
     */
    public static boolean isPreQuerySQL(StatementBase parsedStmt) {
        if (parsedStmt instanceof QueryStatement queryStatement) {
            if (queryStatement.getQueryRelation() != null
                    && (queryStatement.getQueryRelation() instanceof SelectRelation selectRelation)) {
                if (selectRelation.getSelectList() != null && !selectRelation.getSelectList().getItems().isEmpty()) {
                    Expr itemExpr = selectRelation.getSelectList().getItems().get(0).getExpr();
                    if (itemExpr != null) {
                        if (itemExpr instanceof VariableExpr) {
                            return true;
                        } else if (itemExpr instanceof InformationFunction informationFunction) {
                            return informationFunction.getFuncType().equalsIgnoreCase("connection_id")
                                    || informationFunction.getFuncType().equalsIgnoreCase("session_id");
                        }
                    }
                }
            }
        }

        if (parsedStmt instanceof SetStmt setStmt) {
            return setStmt.getSetListItems().stream().anyMatch(item ->
                    (item instanceof SetNamesVar
                            || item instanceof SetTransaction
                            || item instanceof SystemVariable));
        }

        return false;
    }
}
