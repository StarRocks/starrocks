// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DropIndexClause.java

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
import com.starrocks.common.UserException;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

public class DropIndexClause extends AlterTableClause {
    private final String indexName;
    private final TableName tableName;
    private boolean alter;

    public DropIndexClause(String indexName, TableName tableName, boolean alter) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.indexName = indexName;
        this.tableName = tableName;
        this.alter = alter;
    }

    public String getIndexName() {
        return indexName;
    }

    public TableName getTableName() {
        return tableName;
    }

    public boolean isAlter() {
        return alter;
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (StringUtils.isEmpty(indexName)) {
            throw new AnalysisException("index name is excepted");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP INDEX ").append(indexName);
        if (!alter) {
            stringBuilder.append(" ON ").append(tableName.toSql());
        }
        return stringBuilder.toString();
    }
}
