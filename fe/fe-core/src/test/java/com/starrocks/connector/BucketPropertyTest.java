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

package com.starrocks.connector;

import com.starrocks.catalog.Column;
import com.starrocks.thrift.TBucketFunction;
import com.starrocks.thrift.TBucketProperty;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BucketPropertyTest {

    @Test
    public void testSatisfy() {
        BucketProperty bp1 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, new Column("c1", IntegerType.INT));
        BucketProperty bp2 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, new Column("c2", IntegerType.INT));
        Assertions.assertTrue(bp1.satisfy(bp2));

        BucketProperty bp3 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 20, new Column("c1", IntegerType.INT));
        Assertions.assertFalse(bp1.satisfy(bp3));

        BucketProperty bp4 = new BucketProperty(TBucketFunction.NATIVE, 10, new Column("c1", IntegerType.INT));
        Assertions.assertFalse(bp1.satisfy(bp4));
    }

    @Test
    public void testToString() {
        BucketProperty bp = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, new Column("c1", IntegerType.INT));
        Assertions.assertEquals("MURMUR3_X86_32, 10", bp.toString());
    }

    @Test
    public void testGetters() {
        Column c = new Column("c1", IntegerType.INT);
        BucketProperty bp = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, c);
        Assertions.assertEquals(TBucketFunction.MURMUR3_X86_32, bp.getBucketFunction());
        Assertions.assertEquals(10, bp.getBucketNum());
        Assertions.assertEquals(c, bp.getColumn());
    }

    @Test
    public void testEquals() {
        Column c1 = new Column("c1", IntegerType.INT);
        BucketProperty bp1 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, c1);
        BucketProperty bp2 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, c1);
        BucketProperty bp3 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 20, c1);
        Column c2 = new Column("c2", IntegerType.INT);
        BucketProperty bp4 = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, c2);

        Assertions.assertEquals(bp1, bp1);
        Assertions.assertEquals(bp1, bp2);
        Assertions.assertNotEquals(bp1, bp3);
        Assertions.assertNotEquals(bp1, bp4);
        Assertions.assertNotEquals(bp1, null);
        Assertions.assertNotEquals(bp1, new Object());
    }

    @Test
    public void testToThrift() {
        Column c = new Column("c1", IntegerType.INT);
        BucketProperty bp = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, c);
        TBucketProperty tbp = bp.toThrift();
        Assertions.assertEquals(TBucketFunction.MURMUR3_X86_32, tbp.getBucket_func());
        Assertions.assertEquals(10, tbp.getBucket_num());
    }
}