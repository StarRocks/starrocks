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

package com.starrocks.connector.adbc;

import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ADBCConnectorTypeTest {

    @Test
    public void testConnectorTypeADBCExists() {
        ConnectorType adbc = ConnectorType.ADBC;
        assertNotNull(adbc);
        assertEquals("adbc", adbc.getName());
    }

    @Test
    public void testConnectorTypeIsSupportAdbc() {
        assertTrue(ConnectorType.isSupport("adbc"));
    }

    @Test
    public void testConnectorTypeFromAdbc() {
        ConnectorType type = ConnectorType.from("adbc");
        assertEquals(ConnectorType.ADBC, type);
    }

    @Test
    public void testConnectorTypeSupportTypeSetContainsADBC() {
        assertTrue(ConnectorType.SUPPORT_TYPE_SET.contains(ConnectorType.ADBC));
    }

    @Test
    public void testTableTypeADBCExists() {
        Table.TableType adbc = Table.TableType.ADBC;
        assertNotNull(adbc);
    }

    @Test
    public void testIsADBCTable() {
        Table table = new Table(Table.TableType.ADBC);
        assertTrue(table.isADBCTable());
        assertFalse(table.isJDBCTable());
    }

    @Test
    public void testIsNotADBCTable() {
        Table table = new Table(Table.TableType.JDBC);
        assertFalse(table.isADBCTable());
    }
}
