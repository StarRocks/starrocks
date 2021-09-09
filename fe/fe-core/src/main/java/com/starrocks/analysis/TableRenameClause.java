// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/TableRenameClause.java

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
import com.starrocks.alter.AlterOpType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;

import java.util.Map;

// rename table
public class TableRenameClause extends AlterTableClause {
    private String newTableName;

    public TableRenameClause(String newTableName) {
        super(AlterOpType.RENAME);
        this.newTableName = newTableName;
        this.needTableStable = false;
    }

    public String getNewTableName() {
        return newTableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(newTableName)) {
            throw new AnalysisException("New Table name is not set");
        }

        FeNameFormat.checkTableName(newTableName);
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public String toSql() {
        return "RENAME " + newTableName;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
