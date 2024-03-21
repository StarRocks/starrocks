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

package com.starrocks.catalog;

import com.starrocks.analysis.IntLiteral;
import org.junit.Assert;
import org.junit.Test;

public class HivePartitionKeyTest {

    @Test
    public void testEquals() throws Exception {
        PartitionKey hivePartitionKey = new HivePartitionKey();
        hivePartitionKey.getKeys().add(new IntLiteral(5));
        hivePartitionKey.getKeys().add(new IntLiteral(6));
        hivePartitionKey.getTypes().add(PrimitiveType.INT);
        hivePartitionKey.getTypes().add(PrimitiveType.INT);

        PartitionKey anotherHivePartitionKey = new HivePartitionKey();
        anotherHivePartitionKey.getKeys().add(new IntLiteral(5));
        hivePartitionKey.getTypes().add(PrimitiveType.INT);

        Assert.assertNotEquals(hivePartitionKey, anotherHivePartitionKey);
        Assert.assertEquals(hivePartitionKey, hivePartitionKey);

        anotherHivePartitionKey = new HudiPartitionKey();
        Assert.assertNotEquals(hivePartitionKey, anotherHivePartitionKey);

        hivePartitionKey = new HivePartitionKey();
        hivePartitionKey.getKeys().add(new IntLiteral("6", Type.INT));
        hivePartitionKey.getTypes().add(PrimitiveType.INT);
        anotherHivePartitionKey = new HivePartitionKey();
        anotherHivePartitionKey.getKeys().add(new IntLiteral("06", Type.INT));
        anotherHivePartitionKey.getTypes().add(PrimitiveType.INT);
        Assert.assertNotEquals(hivePartitionKey, anotherHivePartitionKey);
    }
}
