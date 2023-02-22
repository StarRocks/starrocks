// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.google.common.collect.Maps;
import com.starrocks.analysis.LabelName;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

/**
 * ALTER LOAD FOR db.label
 * PROPERTIES(
 * ...
 * )
 */
public class AlterLoadStmt extends DdlStmt {

    private final LabelName labelName;
    private final Map<String, String> jobProperties;

    // save analyzed job properties.
    // analyzed data source properties are saved in dataSourceProperties.
    private final Map<String, String> analyzedJobProperties = Maps.newHashMap();

    public AlterLoadStmt(LabelName labelName, Map<String, String> jobProperties) {
        this(labelName, jobProperties, NodePosition.ZERO);
    }

    public AlterLoadStmt(LabelName labelName, Map<String, String> jobProperties, NodePosition pos) {
        super(pos);
        this.labelName = labelName;
        this.jobProperties = jobProperties != null ? jobProperties : Maps.newHashMap();
    }

    public String getDbName() {
        return labelName.getDbName();
    }

    public void setDbName(String dbName) {
        labelName.setDbName(dbName);
    }

    public String getLabel() {
        return labelName.getLabelName();
    }

    public Map<String, String> getJobProperties() {
        return jobProperties;
    }

    public Map<String, String> getAnalyzedJobProperties() {
        return analyzedJobProperties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterLoadStatement(this, context);
    }
}
