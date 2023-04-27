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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/external/elasticsearch/EsShardPartitionsTest.java

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

package com.starrocks.connector.elasticsearch;

import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EsShardPartitionsTest extends EsTestCase {

    @Test
    public void testPartition() throws Exception {
        EsTable esTable = (EsTable) GlobalStateMgr.getCurrentState()
                .getDb(GlobalStateMgrTestUtil.testDb1)
                .getTable(GlobalStateMgrTestUtil.testEsTableId1);
        EsShardPartitions esShardPartitions = EsShardPartitions.findShardPartitions("doe",
                loadJsonFromFile("data/es/test_search_shards.json"));
        EsTablePartitions esTablePartitions = EsTablePartitions.fromShardPartitions(esTable, esShardPartitions);
        assertNotNull(esTablePartitions);
        assertEquals(1, esTablePartitions.getUnPartitionedIndexStates().size());
        assertEquals(5, esTablePartitions.getEsShardPartitions("doe").getShardRoutings().size());
    }
}
