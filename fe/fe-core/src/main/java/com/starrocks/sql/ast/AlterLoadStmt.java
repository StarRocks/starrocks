// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Maps;
import com.starrocks.analysis.LabelName;

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
