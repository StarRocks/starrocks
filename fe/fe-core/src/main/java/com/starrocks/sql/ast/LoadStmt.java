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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/LoadStmt.java

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

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.LabelName;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.Load;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// LOAD statement, load files into tables.
//
// syntax:
//      LOAD LABEL load_label
//          (data_desc, ...)
//          [broker_desc]
//          [BY cluster]
//          [resource_desc]
//      [PROPERTIES (key1=value1, )]
//
//      load_label:
//          db_name.label_name
//
//      data_desc:
//          DATA INFILE ('file_path', ...)
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [COLUMNS TERMINATED BY separator ]
//          [FORMAT AS CSV]
//          [
//              (
//                  "trim_space"="xx",
//                  "enclose"="x",
//                  "escape"="x",
//                  "skip_header"="2"
//              )
//          ]
//          [(col1, ...)]
//          [SET (k1=f1(xx), k2=f2(xx))]
//
//      broker_desc:
//          WITH BROKER name
//          (key2=value2, ...)
//
//      resource_desc:
//          WITH RESOURCE name
//          (key3=value3, ...)
public class LoadStmt extends DdlStmt {
    public static final String TIMEOUT_PROPERTY = "timeout";
    public static final String MAX_FILTER_RATIO_PROPERTY = "max_filter_ratio";
    public static final String LOAD_DELETE_FLAG_PROPERTY = "load_delete_flag";
    public static final String LOAD_MEM_LIMIT = "load_mem_limit";
    public static final String CLUSTER_PROPERTY = "cluster";
    private static final String VERSION = "version";
    public static final String STRICT_MODE = "strict_mode";
    public static final String TIMEZONE = "timezone";
    public static final String PARTIAL_UPDATE = "partial_update";
    public static final String PRIORITY = "priority";
    public static final String MERGE_CONDITION = "merge_condition";
    public static final String CASE_SENSITIVE = "case_sensitive";
    public static final String LOG_REJECTED_RECORD_NUM = "log_rejected_record_num";
    public static final String SPARK_LOAD_SUBMIT_TIMEOUT = "spark_load_submit_timeout";

    public static final String PARTIAL_UPDATE_MODE = "partial_update_mode";

    // for load data from Baidu Object Store(BOS)
    public static final String BOS_ENDPOINT = "bos_endpoint";
    public static final String BOS_ACCESSKEY = "bos_accesskey";
    public static final String BOS_SECRET_ACCESSKEY = "bos_secret_accesskey";

    // mini load params
    public static final String KEY_IN_PARAM_COLUMNS = "columns";
    public static final String KEY_IN_PARAM_SET = "set";
    public static final String KEY_IN_PARAM_HLL = "hll";
    public static final String KEY_IN_PARAM_COLUMN_SEPARATOR = "column_separator";
    public static final String KEY_IN_PARAM_LINE_DELIMITER = "line_delimiter";
    public static final String KEY_IN_PARAM_PARTITIONS = "partitions";
    public static final String KEY_IN_PARAM_FORMAT_TYPE = "format";
    private final LabelName label;
    private final List<DataDescription> dataDescriptions;
    private final BrokerDesc brokerDesc;
    private final String cluster;
    private final ResourceDesc resourceDesc;
    private final Map<String, String> properties;
    private String user;
    private EtlJobType etlJobType = EtlJobType.UNKNOWN;

    private String version = "v2";

    // properties set
    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(TIMEOUT_PROPERTY)
            .add(MAX_FILTER_RATIO_PROPERTY)
            .add(LOAD_DELETE_FLAG_PROPERTY)
            .add(LOAD_MEM_LIMIT)
            .add(CLUSTER_PROPERTY)
            .add(STRICT_MODE)
            .add(VERSION)
            .add(TIMEZONE)
            .add(PARTIAL_UPDATE)
            .add(PRIORITY)
            .add(CASE_SENSITIVE)
            .add(LOG_REJECTED_RECORD_NUM)
            .add(PARTIAL_UPDATE_MODE)
            .add(SPARK_LOAD_SUBMIT_TIMEOUT)
            .build();

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions, BrokerDesc brokerDesc,
                    String cluster, Map<String, String> properties) {
        this(label, dataDescriptions, brokerDesc, cluster, properties, NodePosition.ZERO);
    }

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions, BrokerDesc brokerDesc,
                    String cluster, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.label = label;
        this.dataDescriptions = dataDescriptions;
        this.brokerDesc = brokerDesc;
        this.cluster = cluster;
        this.resourceDesc = null;
        this.properties = properties;
        this.user = null;
    }

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions, ResourceDesc resourceDesc,
                    Map<String, String> properties) {
        this(label, dataDescriptions, resourceDesc, properties, NodePosition.ZERO);
    }

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions, ResourceDesc resourceDesc,
                    Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.label = label;
        this.dataDescriptions = dataDescriptions;
        this.brokerDesc = null;
        this.cluster = null;
        this.resourceDesc = resourceDesc;
        this.properties = properties;
        this.user = null;
    }

    public LabelName getLabel() {
        return label;
    }

    public List<DataDescription> getDataDescriptions() {
        return dataDescriptions;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public String getCluster() {
        return cluster;
    }

    public ResourceDesc getResourceDesc() {
        return resourceDesc;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public EtlJobType getEtlJobType() {
        return etlJobType;
    }

    public void setEtlJobType(EtlJobType etlJobType) {
        this.etlJobType = etlJobType;
    }

    public static void checkProperties(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            return;
        }

        for (Entry<String, String> entry : properties.entrySet()) {
            if (!PROPERTIES_SET.contains(entry.getKey())) {
                throw new DdlException(entry.getKey() + " is invalid property");
            }
        }

        final String loadMemProperty = properties.get(LOAD_MEM_LIMIT);
        if (loadMemProperty != null) {
            try {
                final long loadMem = Long.parseLong(loadMemProperty);
                if (loadMem < 0) {
                    throw new DdlException(LOAD_MEM_LIMIT + " must be equal or greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(LOAD_MEM_LIMIT + " is not a number.");
            }
        }

        // timeout
        final String timeoutLimitProperty = properties.get(TIMEOUT_PROPERTY);
        if (timeoutLimitProperty != null) {
            try {
                final int timeoutLimit = Integer.parseInt(timeoutLimitProperty);
                if (timeoutLimit < 0) {
                    throw new DdlException(TIMEOUT_PROPERTY + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(TIMEOUT_PROPERTY + " is not a number.");
            }
        }

        // spark load wait yarn timeout
        final String sparkLoadSubmitTimeoutProperty = properties.get(SPARK_LOAD_SUBMIT_TIMEOUT);
        if (sparkLoadSubmitTimeoutProperty != null) {
            try {
                final long sparkLoadSubmitTimeout = Long.parseLong(sparkLoadSubmitTimeoutProperty);
                if (sparkLoadSubmitTimeout < 0) {
                    throw new DdlException(SPARK_LOAD_SUBMIT_TIMEOUT + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(SPARK_LOAD_SUBMIT_TIMEOUT + " is not a number.");
            }
        }

        // max filter ratio
        final String maxFilterRadioProperty = properties.get(MAX_FILTER_RATIO_PROPERTY);
        if (maxFilterRadioProperty != null) {
            try {
                double maxFilterRatio = Double.valueOf(maxFilterRadioProperty);
                if (maxFilterRatio < 0.0 || maxFilterRatio > 1.0) {
                    throw new DdlException(MAX_FILTER_RATIO_PROPERTY + " must between 0.0 and 1.0.");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(MAX_FILTER_RATIO_PROPERTY + " is not a number.");
            }
        }

        // version
        final String versionProperty = properties.get(VERSION);
        if (versionProperty != null) {
            if (!versionProperty.equalsIgnoreCase(Load.VERSION)) {
                throw new DdlException(VERSION + " must be " + Load.VERSION);
            }
        }

        // strict mode
        final String strictModeProperty = properties.get(STRICT_MODE);
        if (strictModeProperty != null) {
            if (!strictModeProperty.equalsIgnoreCase("true")
                    && !strictModeProperty.equalsIgnoreCase("false")) {
                throw new DdlException(STRICT_MODE + " is not a boolean");
            }
        }

        // time zone
        final String timezone = properties.get(TIMEZONE);
        if (timezone != null) {
            properties.put(TIMEZONE, TimeUtils.checkTimeZoneValidAndStandardize(
                    properties.getOrDefault(LoadStmt.TIMEZONE, TimeUtils.DEFAULT_TIME_ZONE)));
        }

        // load priority
        final String priorityProperty = properties.get(PRIORITY);
        if (priorityProperty != null) {
            if (LoadPriority.priorityByName(priorityProperty) == null) {
                throw new DdlException(PRIORITY + " should in HIGHEST/HIGH/NORMAL/LOW/LOWEST");
            }
        }

        // log rejected record num
        final String logRejectedRecordNumProperty = properties.get(LOG_REJECTED_RECORD_NUM);
        if (logRejectedRecordNumProperty != null) {
            try {
                final long logRejectedRecordNum = Long.parseLong(logRejectedRecordNumProperty);
                if (logRejectedRecordNum < -1) {
                    throw new DdlException(LOG_REJECTED_RECORD_NUM + " must be equal or greater than -1");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(LOG_REJECTED_RECORD_NUM + " is not a number.");
            }
        }
    }

    @Override
    public boolean needAuditEncryption() {
        if (brokerDesc != null || resourceDesc != null) {
            return true;
        }
        return false;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLoadStatement(this, context);
    }
}
