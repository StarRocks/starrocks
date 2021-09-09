// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AlterClusterClause.java

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

import com.starrocks.alter.AlterOpType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import org.apache.commons.lang.NotImplementedException;

import java.util.Map;

@Deprecated
public class AlterClusterClause extends AlterClause {
    private AlterClusterType type;
    private Map<String, String> properties;
    private int instanceNum;
    private String password;

    public AlterClusterClause(AlterClusterType type, Map<String, String> properties) {
        super(AlterOpType.ALTER_OTHER);
        this.type = type;
        this.properties = properties;
        instanceNum = 0;
        password = "";
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Config.disable_cluster_feature) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "ALTER CLUSTER");
        }

        if (properties == null || properties.size() == 0) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_PARAMETER);
        }

        if (!properties.containsKey(CreateClusterStmt.CLUSTER_INSTANCE_NUM)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_INSTANCE_NUM);
        }

        instanceNum = Integer.valueOf(properties.get(CreateClusterStmt.CLUSTER_INSTANCE_NUM));
        password = properties.get(CreateClusterStmt.CLUSTER_SUPERMAN_PASSWORD);
    }

    @Override
    public String toSql() {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    public int getInstanceNum() {
        return instanceNum;
    }

    public String getPassword() {
        return password;
    }
}
