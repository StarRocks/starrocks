// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowUserPropertyStmt.java

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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.UserException;
import com.starrocks.common.proc.UserPropertyProcNode;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// Show Property Stmt
//  syntax:
//      SHOW PROPERTY [FOR user] [LIKE key pattern]
public class ShowUserPropertyStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowUserPropertyStmt.class);

    private String user;
    private String pattern;

    public ShowUserPropertyStmt(String user, String pattern) {
        this.user = user;
        this.pattern = pattern;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPatter() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(user)) {
            user = analyzer.getQualifiedUser();
            // user can see itself's property, no need to check privs
        } else {
            user = ClusterNamespace.getFullName(user);

            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }

        pattern = Strings.emptyToNull(pattern);
    }

    public List<List<String>> getRows() throws AnalysisException {
        List<List<String>> rows = GlobalStateMgr.getCurrentState().getAuth().getUserProperties(user);

        if (pattern == null) {
            return rows;
        }

        List<List<String>> result = Lists.newArrayList();
        PatternMatcher matcher = PatternMatcher.createMysqlPattern(pattern,
                CaseSensibility.USER.getCaseSensibility());
        for (List<String> row : rows) {
            String key = row.get(0).split("\\" + SetUserPropertyVar.DOT_SEPARATOR)[0];
            if (matcher.match(key)) {
                result.add(row);
            }
        }

        return result;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String col : UserPropertyProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowUserPropertyStmt(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW PROPERTY FOR '");
        sb.append(user);
        sb.append("'");

        if (pattern != null) {
            sb.append(" LIKE '");
            sb.append(pattern);
            sb.append("'");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
