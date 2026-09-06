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

import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.optimizer.dump.DesensitizedSQLBuilder;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ADBCTableTest {
    private static ADBCTable table() {
        return new ADBCTable(1, "events", List.of(new Column("id", IntegerType.INT)), "analytics", "adbc0",
                Map.of("type", "adbc", "driver", "adbc_driver_flightsql",
                        "uri", "grpc://admin:uri-secret@localhost:8815?token=query-secret",
                        "username", "private-user", "password", "password-secret",
                        "adbc.flight.sql.authorization_header", "Bearer header-secret"));
    }

    private static void assertCredentialsHidden(String ddl) {
        assertFalse(ddl.contains("secret"), ddl);
        assertFalse(ddl.contains("private-user"), ddl);
        assertFalse(ddl.contains("localhost"), ddl);
        assertTrue(ddl.contains("adbc_driver_flightsql"), ddl);
    }

    @Test
    public void testProfileTableNameIsQualifiedOnce() {
        assertEquals("adbc0.analytics.events", table().getQualifiedTableName());
    }

    @Test
    public void testShowCreateHidesAllConnectionPropertiesWhenRequested() {
        List<String> ddl = new ArrayList<>();
        AstToStringBuilder.getDdlStmt(table(), ddl, null, null, false, true);
        assertCredentialsHidden(ddl.get(0));
    }

    @Test
    public void testDesensitizedDumpHidesAllConnectionProperties() {
        String ddl = DesensitizedSQLBuilder.desensitizeTableDef(Pair.create("analytics", table()),
                Map.of("analytics", "0", "events", "1", "id", "2"));
        assertCredentialsHidden(ddl);
    }

    @Test
    public void testDisplayRedactionDoesNotAlterBackendCredentials() {
        ADBCTable table = table();
        table.getDisplayProperties(true);
        assertEquals("password-secret", table.toThrift(List.of()).getAdbcTable().getAdbc_options().get("password"));
        List<String> ddl = new ArrayList<>();
        AstToStringBuilder.getDdlStmt(table, ddl, null, null, false, false);
        assertTrue(ddl.get(0).contains("password-secret"));
    }

    private static AccessController mockCatalogAccessController() {
        AccessController controller = mock(AccessController.class);
        AccessControlProvider provider = new AccessControlProvider(null, mock(AccessController.class));
        provider.setAccessControl("adbc0", controller);
        new MockUp<Authorizer>() {
            @Mock
            public AccessControlProvider getInstance() {
                return provider;
            }
        };
        return controller;
    }

    @Test
    public void testShowCreateAuthorizationUsesCatalogPrivileges() throws AccessDeniedException {
        AccessController controller = mockCatalogAccessController();
        ConnectContext context = new ConnectContext();
        Authorizer.checkAnyActionOnTableLikeObject(context, "analytics", table());
        verify(controller).checkAnyActionOnTable(context, new TableName("adbc0", "analytics", "events"));
    }

    @Test
    public void testShowCreateAuthorizationPreservesAccessDenial() throws AccessDeniedException {
        AccessController controller = mockCatalogAccessController();
        ConnectContext context = new ConnectContext();
        TableName name = new TableName("adbc0", "analytics", "events");
        doThrow(new AccessDeniedException()).when(controller).checkAnyActionOnTable(context, name);
        assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkAnyActionOnTableLikeObject(context, "analytics", table()));
        verify(controller).checkAnyActionOnTable(context, name);
    }
}
