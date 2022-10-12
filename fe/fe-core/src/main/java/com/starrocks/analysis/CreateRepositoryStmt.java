// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateRepositoryStmt.java

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
import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;

public class CreateRepositoryStmt extends DdlStmt {
    private boolean isReadOnly;
    private String name;
    private String brokerName;
    private String location;
    private Map<String, String> properties;
    private boolean hasBroker;

    public CreateRepositoryStmt(boolean isReadOnly, String name, String brokerName, String location,
                                Map<String, String> properties) {
        this.isReadOnly = isReadOnly;
        this.name = name;
        this.brokerName = brokerName;
        this.location = location;
        this.properties = properties;
        if (brokerName == null) {
            hasBroker = false;
        } else {
            hasBroker = true;
        }
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public String getName() {
        return name;
    }

    public boolean hasBroker() {
        return hasBroker;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getLocation() {
        return location;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        FeNameFormat.checkCommonName("repository", name);

        if (hasBroker) {
            if (Strings.isNullOrEmpty(brokerName)) {
                throw new AnalysisException("You must specify the broker of the repository");
            }
        }

        if (Strings.isNullOrEmpty(location)) {
            throw new AnalysisException("You must specify a location on the repository");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (isReadOnly) {
            sb.append("READ_ONLY ");
        }
        sb.append("REPOSITORY `").append(name).append("` ").append("WITH BROKER `").append(brokerName).append("` ");
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false)).append(")");
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRepositoryStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
