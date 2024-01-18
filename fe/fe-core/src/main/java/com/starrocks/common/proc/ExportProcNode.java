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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/ExportProcNode.java

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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.load.ExportMgr;

import java.util.List;

// TODO(lingbin): think if need a sub node to show unfinished instances
public class ExportProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("QueryId").add("State").add("Progress")
            .add("TaskInfo").add("Path")
            .add("CreateTime").add("StartTime").add("FinishTime")
            .add("Timeout").add("ErrorMsg")
            .build();

    private static final int LIMIT = 2000;

    private ExportMgr exportMgr;
    private Database db;

    public ExportProcNode(ExportMgr exportMgr, Database db) {
        this.exportMgr = exportMgr;
        this.db = db;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(exportMgr);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<String>> jobInfos = exportMgr.getExportJobInfosByIdOrState(db.getId(), 0, null, null, null, LIMIT);
        result.setRows(jobInfos);
        return result;
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }
}
