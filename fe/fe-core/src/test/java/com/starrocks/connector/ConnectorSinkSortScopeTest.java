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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorSinkSortScopeTest {

    @Test
    public void testFromName() {
        Assertions.assertEquals(ConnectorSinkSortScope.NONE, ConnectorSinkSortScope.fromName("none"));
        Assertions.assertEquals(ConnectorSinkSortScope.NONE, ConnectorSinkSortScope.fromName("NONE"));
        Assertions.assertEquals(ConnectorSinkSortScope.NONE, ConnectorSinkSortScope.fromName("None"));

        Assertions.assertEquals(ConnectorSinkSortScope.FILE, ConnectorSinkSortScope.fromName("file"));
        Assertions.assertEquals(ConnectorSinkSortScope.FILE, ConnectorSinkSortScope.fromName("FILE"));
        Assertions.assertEquals(ConnectorSinkSortScope.FILE, ConnectorSinkSortScope.fromName("File"));

        Assertions.assertEquals(ConnectorSinkSortScope.HOST, ConnectorSinkSortScope.fromName("host"));
        Assertions.assertEquals(ConnectorSinkSortScope.HOST, ConnectorSinkSortScope.fromName("HOST"));
        Assertions.assertEquals(ConnectorSinkSortScope.HOST, ConnectorSinkSortScope.fromName("Host"));
    }

    @Test
    public void testFromNameNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ConnectorSinkSortScope.fromName(null);
        });
    }

    @Test
    public void testFromNameInvalid() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ConnectorSinkSortScope.fromName("invalid");
        });
    }

    @Test
    public void testScopeName() {
        Assertions.assertEquals("none", ConnectorSinkSortScope.NONE.scopeName());
        Assertions.assertEquals("file", ConnectorSinkSortScope.FILE.scopeName());
        Assertions.assertEquals("host", ConnectorSinkSortScope.HOST.scopeName());
    }
}
