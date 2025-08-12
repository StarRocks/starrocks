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

package com.starrocks.persist;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicaPersistInfoTest {
    @Test
    public void testGet() throws Exception {
        ReplicaPersistInfo info = ReplicaPersistInfo.createForLoad(0, 1, 2, 3, 4, 5, 7, 0, 8);
        Assertions.assertEquals(0, info.getTableId());
        Assertions.assertEquals(1, info.getPartitionId());
        Assertions.assertEquals(2, info.getIndexId());
        Assertions.assertEquals(3, info.getTabletId());
        Assertions.assertEquals(4, info.getReplicaId());
        Assertions.assertEquals(5, info.getVersion());
        Assertions.assertEquals(0, info.getDataSize());
        Assertions.assertEquals(8, info.getRowCount());
    }
}
