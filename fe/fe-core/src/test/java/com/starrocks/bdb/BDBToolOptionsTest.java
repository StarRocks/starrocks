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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/bdb/BDBToolOptionsTest.java

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

package com.starrocks.bdb;

import com.starrocks.common.FeConstants;
import com.starrocks.journal.bdbje.BDBToolOptions;
import org.junit.Assert;
import org.junit.Test;

public class BDBToolOptionsTest {

    @Test
    public void test() {
        BDBToolOptions options = new BDBToolOptions(true, "", false, "", "", 0, 0);
        Assert.assertFalse(options.hasFromKey());
        Assert.assertFalse(options.hasEndKey());
        Assert.assertEquals(FeConstants.META_VERSION, options.getMetaVersion());

        options = new BDBToolOptions(false, "12345", false, "12345", "12456", 35, 0);
        Assert.assertTrue(options.hasFromKey());
        Assert.assertTrue(options.hasEndKey());
        Assert.assertNotSame(FeConstants.META_VERSION, options.getMetaVersion());
    }

}
