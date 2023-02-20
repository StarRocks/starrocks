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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/external/elasticsearch/VersionPhaseTest.java

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

package com.starrocks.external.elasticsearch;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.ExceptionChecker;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class VersionPhaseTest extends EsTestCase {

    @Test
    public void testWorkFlow(@Injectable EsRestClient client) throws Exception {
        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", Type.BIGINT);
        columns.add(k1);
        EsTable esTableBefore7X = fakeEsTable("fake", "test", "doc", columns);
        SearchContext context = new SearchContext(esTableBefore7X);

        new Expectations(client) {
            {
                client.version();
                minTimes = 0;
                result = EsMajorVersion.V_6_X;
            }
        };
        VersionPhase versionPhase = new VersionPhase(client);
        ExceptionChecker.expectThrowsNoException(() -> versionPhase.preProcess(context));
        ExceptionChecker.expectThrowsNoException(() -> versionPhase.execute(context));
        assertTrue(context.version().on(EsMajorVersion.V_6_X));
    }

}
