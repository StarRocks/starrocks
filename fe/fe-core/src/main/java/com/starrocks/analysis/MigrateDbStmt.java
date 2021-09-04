// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/MigrateDbStmt.java

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

import com.starrocks.catalog.Catalog;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;

@Deprecated
public class MigrateDbStmt extends DdlStmt {

    private ClusterName src;
    private ClusterName dest;
    private String srcCluster;
    private String destCluster;
    private String srcDb;
    private String destDb;

    MigrateDbStmt(ClusterName src, ClusterName dest) {
        this.src = src;
        this.dest = dest;
    }

    public String getSrcCluster() {
        return srcCluster;
    }

    public String getDestCluster() {
        return destCluster;
    }

    public String getSrcDb() {
        return srcDb;
    }

    public String getDestDb() {
        return destDb;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (Config.disable_cluster_feature) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "MIGRATION CLUSTER");
        }

        src.analyze(analyzer);
        dest.analyze(analyzer);

        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    "ADMIN");
        }

        srcCluster = src.getCluster();
        srcDb = ClusterNamespace.getFullName(srcCluster, src.getDb());
        destCluster = dest.getCluster();
        destDb = ClusterNamespace.getFullName(destCluster, dest.getDb());
    }

    @Override
    public String toSql() {
        return "MIGRATE DATABASE " + srcCluster + "." + srcDb + " " + destCluster + "." + destDb;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
