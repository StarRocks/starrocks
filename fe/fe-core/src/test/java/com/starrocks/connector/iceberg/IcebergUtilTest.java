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

package com.starrocks.connector.iceberg;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IcebergUtilTest {

    @Test
    public void testFileName() {
        assertEquals("1.orc", IcebergUtil.fileName("hdfs://tans/user/hive/warehouse/max-test/1.orc"));
        assertEquals("2.orc", IcebergUtil.fileName("cos://tans/user/hive/warehouse/max-test/2.orc"));
        assertEquals("3.orc", IcebergUtil.fileName("s3://tans/user/hive/warehouse/max-test/3.orc"));
        assertEquals("4.orc", IcebergUtil.fileName("gs://tans/user/hive/warehouse/max-test/4.orc"));
    }
}