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

package com.starrocks.http;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.http.meta.ColocateMetaService;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ColocateMetaServiceTest {

    @Test
    public void testUpdateBackendPerBucketSeq(@Mocked EditLog editLog) {
        new Expectations() {
            {
                editLog.logColocateBackendsPerBucketSeq((ColocatePersistInfo) any);
                minTimes = 0;

                editLog.logColocateMarkUnstable((ColocatePersistInfo) any);
                minTimes = 0;
            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(editLog);
        ColocateMetaService.BucketSeqAction action = new ColocateMetaService.BucketSeqAction(null);
        List<List<Long>> seqs = new ArrayList<>();
        seqs.add(Lists.newArrayList(1000L));
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(111, 222);
        action.updateBackendPerBucketSeq(groupId, seqs);
    }
}
