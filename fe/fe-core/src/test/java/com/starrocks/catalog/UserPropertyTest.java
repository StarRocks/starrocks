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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/UserPropertyTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.structure.Pair;
import com.starrocks.mysql.privilege.UserProperty;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class UserPropertyTest {

    @Test
    public void testUpdate() throws DdlException {
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.create("MAX_USER_CONNECTIONS", "100"));

        UserProperty userProperty = new UserProperty();
        userProperty.update(properties, false);
        Assert.assertEquals(100, userProperty.getMaxConn());

        // fetch property
        List<List<String>> rows = userProperty.fetchProperty();
        for (List<String> row : rows) {
            String key = row.get(0);
            String value = row.get(1);

            if (key.equalsIgnoreCase("max_user_connections")) {
                Assert.assertEquals("100", value);
            }
        }

        properties.get(0).second = "1025";
        Assert.assertThrows("max_user_connections is not valid, must less than qe_max_connection 1024",
                DdlException.class, () -> userProperty.update(properties, false));
    }
}
