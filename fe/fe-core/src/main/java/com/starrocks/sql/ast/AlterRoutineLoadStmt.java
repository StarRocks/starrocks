// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.load.RoutineLoadDesc;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ALTER ROUTINE LOAD FOR db.label
 * PROPERTIES(
 * ...
 * )
 * FROM kafka (
 * ...
 * )
 */
public class AlterRoutineLoadStmt extends DdlStmt {

    private static final String NAME_TYPE = "ROUTINE LOAD NAME";

    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)
            .add(CreateRoutineLoadStmt.MAX_BATCH_SIZE_PROPERTY)
            .add(CreateRoutineLoadStmt.JSONPATHS)
            .add(CreateRoutineLoadStmt.JSONROOT)
            .add(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY)
            .add(LoadStmt.STRICT_MODE)
            .add(LoadStmt.TIMEZONE)
            .build();

    private LabelName labelName;
    private final List<ParseNode> loadPropertyList;
    private RoutineLoadDesc routineLoadDesc;
    private final Map<String, String> jobProperties;
    private final RoutineLoadDataSourceProperties dataSourceProperties;

    // save analyzed job properties.
    // analyzed data source properties are saved in dataSourceProperties.
    private Map<String, String> analyzedJobProperties = Maps.newHashMap();

    public AlterRoutineLoadStmt(LabelName labelName, List<ParseNode> loadPropertyList,
                                Map<String, String> jobProperties,
                                RoutineLoadDataSourceProperties dataSourceProperties) {
        this.labelName = labelName;
        this.loadPropertyList = loadPropertyList;
        this.jobProperties = jobProperties != null ? jobProperties : Maps.newHashMap();
        this.dataSourceProperties = dataSourceProperties;
    }

    public String getDbName() {
        return labelName.getDbName();
    }

    public String getLabel() {
        return labelName.getLabelName();
    }

    public void setLabelName(LabelName labelName) {
        this.labelName = labelName;
    }

    public LabelName getLabelName() {
        return this.labelName;
    }

    public Map<String, String> getAnalyzedJobProperties() {
        return analyzedJobProperties;
    }

    public boolean hasDataSourceProperty() {
        return dataSourceProperties.hasAnalyzedProperties();
    }

    public RoutineLoadDataSourceProperties getDataSourceProperties() {
        return dataSourceProperties;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc;
    }

    public void setRoutineLoadDesc(RoutineLoadDesc routineLoadDesc) {
        this.routineLoadDesc = routineLoadDesc;
    }

    public List<ParseNode> getLoadPropertyList() {
        return loadPropertyList;
    }

    public void checkJobProperties() throws UserException {
        Optional<String> optional = jobProperties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)) {
            long desiredConcurrentNum = ((Long) Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY),
                    -1, CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PRED,
                    CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY + " should > 0")).intValue();
            analyzedJobProperties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY,
                    String.valueOf(desiredConcurrentNum));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)) {
            long maxErrorNum = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PRED,
                    CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY + " should >= 0");
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY,
                    String.valueOf(maxErrorNum));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
            double maxFilterRatio;
            try {
                maxFilterRatio = Double.valueOf(jobProperties.get(
                                    CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY));
            } catch (NumberFormatException exception) {
                throw new UserException("Incorrect format of max_filter_ratio", exception);
            }
            if (maxFilterRatio < 0.0 || maxFilterRatio > 1.0) {
                throw new UserException(
                    CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY + " must between 0.0 and 1.0.");
            }
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_FILTER_RATIO_PROPERTY,
                    String.valueOf(maxFilterRatio));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)) {
            long maxBatchIntervalS = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY + " should >= 5");
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY,
                    String.valueOf(maxBatchIntervalS));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)) {
            long maxBatchRows = Util.getLongPropertyOrDefault(
                    jobProperties.get(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY),
                    -1, CreateRoutineLoadStmt.MAX_BATCH_ROWS_PRED,
                    CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY + " should > 200000");
            analyzedJobProperties.put(CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY,
                    String.valueOf(maxBatchRows));
        }

        if (jobProperties.containsKey(LoadStmt.STRICT_MODE)) {
            boolean strictMode = Boolean.valueOf(jobProperties.get(LoadStmt.STRICT_MODE));
            analyzedJobProperties.put(LoadStmt.STRICT_MODE, String.valueOf(strictMode));
        }

        if (jobProperties.containsKey(LoadStmt.TIMEZONE)) {
            String timezone = TimeUtils.checkTimeZoneValidAndStandardize(jobProperties.get(LoadStmt.TIMEZONE));
            analyzedJobProperties.put(LoadStmt.TIMEZONE, timezone);
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.JSONPATHS)) {
            analyzedJobProperties
                    .put(CreateRoutineLoadStmt.JSONPATHS, jobProperties.get(CreateRoutineLoadStmt.JSONPATHS));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.JSONROOT)) {
            analyzedJobProperties
                    .put(CreateRoutineLoadStmt.JSONROOT, jobProperties.get(CreateRoutineLoadStmt.JSONROOT));
        }

        if (jobProperties.containsKey(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY)) {
            boolean stripOuterArray = Boolean.valueOf(jobProperties.get(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY));
            analyzedJobProperties.put(CreateRoutineLoadStmt.STRIP_OUTER_ARRAY, String.valueOf(stripOuterArray));
        }
    }

    public void checkDataSourceProperties() throws AnalysisException {
        dataSourceProperties.analyze();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterRoutineLoadStatement(this, context);
    }

}
