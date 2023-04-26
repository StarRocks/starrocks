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


package com.starrocks.analysis;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.TruncatePartitionClause;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TruncatePartitionClauseTest {
    
    @Test
    public void testInitTruncatePartitionClause() {

        TruncatePartitionClause clause1 = new TruncatePartitionClause(AlterOpType.TRUNCATE_PARTITION);
        Assert.assertEquals(AlterOpType.TRUNCATE_PARTITION, clause1.getOpType()); 

        PartitionNames partitionNames = new PartitionNames(true, Arrays.asList("p1"));
        TruncatePartitionClause clause2 = new TruncatePartitionClause(partitionNames);
        Assert.assertEquals("p1", clause2.getPartitionNames().getPartitionNames().get(0));
    }
}
