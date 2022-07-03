// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AlterDatabaseRename.java

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
import com.starrocks.analysis.CompoundPredicate.Operator;
import com.starrocks.catalog.Catalog;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;

public class AlterDatabaseRename extends DdlStmt {
    private String dbName;
    private String newDbName;

    public AlterDatabaseRename(String dbName, String newDbName) {
        this.dbName = dbName;
        this.newDbName = newDbName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getNewDbName() {
        return newDbName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new AnalysisException("Database name is not set");
        }

        dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), dbName,
                PrivPredicate.of(PrivBitSet.of(Privilege.ADMIN_PRIV,
                        Privilege.ALTER_PRIV),
                        Operator.OR))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getQualifiedUser(), dbName);
        }

        if (Strings.isNullOrEmpty(newDbName)) {
            throw new AnalysisException("New database name is not set");
        }

        FeNameFormat.checkDbName(newDbName);

        newDbName = ClusterNamespace.getFullName(getClusterName(), newDbName);
    }

    @Override
    public String toSql() {
        return "ALTER DATABASE " + dbName + " RENAME " + newDbName;
    }

}
