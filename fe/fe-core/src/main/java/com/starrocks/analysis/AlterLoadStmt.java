// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;
import java.util.Optional;

/**
 * ALTER LOAD FOR db.label
 * PROPERTIES(
 * ...
 * )
 */
public class AlterLoadStmt extends DdlStmt {

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(LoadStmt.PRIORITY)
            .build();

    private final LabelName labelName;
    private final Map<String, String> jobProperties;

    // save analyzed job properties.
    // analyzed data source properties are saved in dataSourceProperties.
    private Map<String, String> analyzedJobProperties = Maps.newHashMap();

    /**
     * @param labelName
     * @param jobProperties
     */
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

    public Map<String, String> getAnalyzedJobProperties() {
        return analyzedJobProperties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        labelName.analyze(analyzer);
        FeNameFormat.checkCommonName(NAME_TYPE, labelName.getLabelName());
        // check routine load job properties include desired concurrent number etc.
        checkJobProperties();

        if (analyzedJobProperties.isEmpty()) {
            throw new AnalysisException("No properties are specified");
        }
    }

    public void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        if (jobProperties.containsKey(LoadStmt.PRIORITY)) {
            final String priorityProperty = jobProperties.get(LoadStmt.PRIORITY);
            if (priorityProperty != null) {
                if (LoadPriority.priorityByName(priorityProperty) == null) {
                    throw new AnalysisException(LoadStmt.PRIORITY + " should in HIGHEST/HIGH/NORMAL/LOW/LOWEST");
                }
            }
            analyzedJobProperties.put(LoadStmt.PRIORITY, priorityProperty);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterLoadStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

}
