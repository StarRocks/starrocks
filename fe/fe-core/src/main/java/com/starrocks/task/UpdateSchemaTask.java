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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/CheckConsistencyTask.java

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

package com.starrocks.task;

import com.starrocks.thrift.TOlapTableColumnParam;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUpdateSchemaReq;

import java.util.List;

public class UpdateSchemaTask extends AgentTask {

    private Long schemaId;
    private Long schemaVersion;
    private List<Long> tablets;
    private TOlapTableColumnParam columnParam;

    public UpdateSchemaTask(long backendId, long dbId,
                            long tableId, long indexId, List<Long> tablets,
                            long schemaId, long schemaVersion,
                            TOlapTableColumnParam columnParam) {
        super(null, backendId, TTaskType.UPDATE_SCHEMA, dbId, tableId, -1, indexId, -1);

        this.schemaId = schemaId;
        this.schemaVersion = schemaVersion;
        this.tablets = tablets;
        this.columnParam = columnParam;
    }

    public TUpdateSchemaReq toThrift() {
        TUpdateSchemaReq req = new TUpdateSchemaReq();
        req.setIndex_id(indexId);
        req.setSchema_id(schemaId);
        req.setSchema_version(schemaVersion);
        req.setTablet_ids(tablets);
        req.setColumn_param(columnParam);
        return req;
    }
}