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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/EtlStatus.java

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
package com.starrocks.load;

import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EtlStatusTest {

    @Test
    public void testLoadStatistic() {
        EtlStatus.LoadStatistic loadStatistic = new EtlStatus.LoadStatistic();
        TUniqueId loadId = new TUniqueId(1000, 2000);
        Set<TUniqueId> fragmentIds = new HashSet<>();
        fragmentIds.add(new TUniqueId(10001, 20001));
        fragmentIds.add(new TUniqueId(10002, 20002));
        fragmentIds.add(new TUniqueId(10003, 20003));
        List<Long> relatedBackendIds = new ArrayList<>();
        relatedBackendIds.add(1002L);
        relatedBackendIds.add(1003L);
        relatedBackendIds.add(1002L);

        loadStatistic.initLoad(loadId, fragmentIds, relatedBackendIds);
        Assert.assertTrue(loadStatistic.toShowInfoStr().contains("1002(2),1003(1)"));

        TReportExecStatusParams tReportExecStatusParams = new TReportExecStatusParams();
        tReportExecStatusParams.setBackend_id(1002);
        tReportExecStatusParams.setFragment_instance_id(new  TUniqueId(10002, 20002));
        tReportExecStatusParams.setDone(true);
        tReportExecStatusParams.setQuery_id(loadId);
        loadStatistic.updateLoadProgress(tReportExecStatusParams);
        Assert.assertTrue(loadStatistic.toShowInfoStr().contains("1002(1),1003(1)"));
        tReportExecStatusParams.setBackend_id(1003);
        tReportExecStatusParams.setFragment_instance_id(new  TUniqueId(10003, 20003));
        loadStatistic.updateLoadProgress(tReportExecStatusParams);
        Assert.assertTrue(loadStatistic.toShowInfoStr().contains("1002(1)"));
    }
}