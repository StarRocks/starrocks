// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AlterLoadErrorUrlClause.java

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
import com.starrocks.common.util.PrintableMap;
import com.starrocks.load.LoadErrorHub;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// FORMAT:
//   ALTER SYSTEM SET LOAD ERRORS HUB properties("type" = "xxx");

@Deprecated
public class AlterLoadErrorUrlClause extends AlterClause {
    private static final Logger LOG = LogManager.getLogger(AlterLoadErrorUrlClause.class);

    private Map<String, String> properties;
    private LoadErrorHub.Param param;

    public AlterLoadErrorUrlClause(Map<String, String> properties) {
        super(AlterOpType.ALTER_OTHER);
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        throw new AnalysisException("Don't support Load errors hub");
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES(");
        PrintableMap<String, String> printableMap = new PrintableMap<>(properties, "=", true, true, true);
        sb.append(printableMap.toString());
        sb.append(")");
        return sb.toString();
    }

}
