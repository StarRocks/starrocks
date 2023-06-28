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
import com.starrocks.sql.parser.NodePosition;

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
            .add(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND)
            .add(CreateRoutineLoadStmt.TASK_CONSUME_SECOND)
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
        this(labelName, loadPropertyList, jobProperties, dataSourceProperties, NodePosition.ZERO);
    }

    public AlterRoutineLoadStmt(LabelName labelName, List<ParseNode> loadPropertyList,
                                Map<String, String> jobProperties,
                                RoutineLoadDataSourceProperties dataSourceProperties,
                                NodePosition pos) {
        super(pos);
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

        if (jobProperties.containsKey(CreateRoutineLoadStmt.TASK_CONSUME_SECOND) &&
                jobProperties.containsKey(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND)) {
            long taskConsumeSecond;
            try {
                taskConsumeSecond = Long.valueOf(jobProperties.get(CreateRoutineLoadStmt.TASK_CONSUME_SECOND));
            } catch (NumberFormatException exception) {
                throw new UserException("Incorrect format of task_consume_second", exception);
            }
            if (taskConsumeSecond <= 0) {
                throw new UserException(
                        CreateRoutineLoadStmt.TASK_CONSUME_SECOND + " must be greater than 0");
            }

            long taskTimeoutSecond;
            try {
                taskTimeoutSecond = Long.valueOf(jobProperties.get(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND));
            } catch (NumberFormatException exception) {
                throw new UserException("Incorrect format of task_timeout_second", exception);
            }
            if (taskTimeoutSecond <= 0) {
                throw new UserException(
                        CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND + " must be greater than 0");
            }

            if (taskConsumeSecond >= taskTimeoutSecond) {
                throw new UserException("task_timeout_second must be larger than task_consume_second");
            }
            analyzedJobProperties.put(CreateRoutineLoadStmt.TASK_CONSUME_SECOND,
                    String.valueOf(taskConsumeSecond));
            analyzedJobProperties.put(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND,
                    String.valueOf(taskTimeoutSecond));
        } else if (jobProperties.containsKey(CreateRoutineLoadStmt.TASK_CONSUME_SECOND)) {
            long taskConsumeSecond;
            try {
                taskConsumeSecond = Long.valueOf(jobProperties.get(CreateRoutineLoadStmt.TASK_CONSUME_SECOND));
            } catch (NumberFormatException exception) {
                throw new UserException("Incorrect format of task_consume_second", exception);
            }
            if (taskConsumeSecond <= 0) {
                throw new UserException(
                        CreateRoutineLoadStmt.TASK_CONSUME_SECOND + " must be greater than 0");
            }

            long taskTimeoutSecond = taskConsumeSecond * CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND_TASK_CONSUME_SECOND_RATIO;
            analyzedJobProperties.put(CreateRoutineLoadStmt.TASK_CONSUME_SECOND,
                    String.valueOf(taskConsumeSecond));
            analyzedJobProperties.put(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND,
                    String.valueOf(taskTimeoutSecond));
        } else if (jobProperties.containsKey(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND)) {
            long taskTimeoutSecond;
            try {
                taskTimeoutSecond = Long.valueOf(jobProperties.get(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND));
            } catch (NumberFormatException exception) {
                throw new UserException("Incorrect format of task_timeout_second", exception);
            }
            if (taskTimeoutSecond <= 0) {
                throw new UserException(
                        CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND + " must be greater than 0");
            }

            long taskConsumeSecond = taskTimeoutSecond / CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND_TASK_CONSUME_SECOND_RATIO;
            analyzedJobProperties.put(CreateRoutineLoadStmt.TASK_CONSUME_SECOND,
                    String.valueOf(taskConsumeSecond));
            analyzedJobProperties.put(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND,
                    String.valueOf(taskTimeoutSecond));
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
