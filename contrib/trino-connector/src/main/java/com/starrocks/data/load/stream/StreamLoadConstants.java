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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/StarRocksFE.java

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

package com.starrocks.data.load.stream;

import java.time.format.DateTimeFormatter;

public class StreamLoadConstants
{
    private StreamLoadConstants()
    {}

    public static final String RESULT_STATUS_OK = "OK";
    public static final String RESULT_STATUS_SUCCESS = "Success";
    public static final String RESULT_STATUS_FAILED = "Fail";
    public static final String RESULT_STATUS_LABEL_EXISTED = "Label Already Exists";
    public static final String RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT = "Publish Timeout";
    public static final String LABEL_STATE_VISIBLE = "VISIBLE";
    public static final String LABEL_STATE_COMMITTED = "COMMITTED";
    public static final String LABEL_STATE_PREPARED = "PREPARED";
    public static final String LABEL_STATE_PREPARE = "PREPARE";
    public static final String LABEL_STATE_ABORTED = "ABORTED";
    public static final String LABEL_STATE_UNKNOWN = "UNKNOWN";
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final String TABLE_MODEL_PRIMARY_KEYS = "PRIMARY_KEYS";
}
