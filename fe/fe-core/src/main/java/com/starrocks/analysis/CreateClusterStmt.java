// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateClusterStmt.java

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

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;

import java.util.Map;

@Deprecated
public class CreateClusterStmt extends DdlStmt {
    public static String CLUSTER_INSTANCE_NUM = "instance_num";
    public static String CLUSTER_SUPERMAN_PASSWORD = "password";
    public static String CLUSTER_SUPERUSER_NAME = "superuser";

    private String clusterName;
    private boolean ifNotExists;
    private int instanceNum;
    private Map<String, String> properties;
    private byte[] scramblePassword;
    private String passwd;

    public CreateClusterStmt() {

    }

    public CreateClusterStmt(String clusterName, Map<String, String> properties, String passwd) {
        this.clusterName = clusterName;
        this.properties = properties;
        this.passwd = passwd;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "CREATE CLUSTER");
    }

    @Override
    public String toSql() {
        final String sql = "CREATE CLUSTER " + clusterName + " PROPERTIES(\"instance_num\"=" + "\"" + instanceNum
                + "\")" + "IDENTIFIED BY '" + passwd + "'";
        return sql;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public int getInstanceNum() {
        return instanceNum;
    }

    public byte[] getPassword() {
        return scramblePassword;
    }

}
