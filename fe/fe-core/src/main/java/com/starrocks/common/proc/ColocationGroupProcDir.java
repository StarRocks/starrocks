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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/ColocationGroupProcDir.java

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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

/*
 * show proc "/colocation_group";
 */
public class ColocationGroupProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("GroupId").add("GroupName").add("TableIds").add("TableNames")
            .add("BucketsNum").add("ReplicationNum").add("DistCols").add("IsStable").build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String groupIdStr) throws AnalysisException {
        String[] parts = groupIdStr.split("\\.");
        if (parts.length != 2) {
            throw new AnalysisException("Invalid group id: " + groupIdStr);
        }

        long dbId = -1;
        long grpId = -1;
        try {
            dbId = Long.valueOf(parts[0]);
            grpId = Long.valueOf(parts[1]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid group id: " + groupIdStr);
        }

        GroupId groupId = new GroupId(dbId, grpId);
        ColocateTableIndex index = GlobalStateMgr.getCurrentColocateIndex();
        List<List<Long>> beSeqs = index.getBackendsPerBucketSeq(groupId);
        return new ColocationGroupBackendSeqsProcNode(beSeqs);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        ColocateTableIndex index = GlobalStateMgr.getCurrentColocateIndex();
        List<List<String>> infos = index.getInfos();
        result.setRows(infos);
        return result;
    }
}
