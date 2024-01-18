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

package com.starrocks.fs;

import com.starrocks.common.exception.UserException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HdfsUtilTest {
    @Test
    public void testColumnFromPath() {
        try {
            // normal case
            List<String> columns = HdfsUtil.parseColumnsFromPath("hdfs://key1=val1/some_path/key2=val2/key3=val3/*", Arrays.asList("key3", "key2", "key1"));
            Assert.assertEquals(3, columns.size());
            Assert.assertEquals("val3", columns.get(0));
            Assert.assertEquals("val2", columns.get(1));
            Assert.assertEquals("val1", columns.get(2));

            // invalid path
            Assert.assertThrows(UserException.class, () ->
                    HdfsUtil.parseColumnsFromPath("invalid_path", Arrays.asList("key3", "key2", "key1")));

            // missing key of columns from path
            Assert.assertThrows(UserException.class, () ->
                    HdfsUtil.parseColumnsFromPath("hdfs://key1=val1/some_path/key3=val3/*", Arrays.asList("key3", "key2", "key1")));
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }
}
