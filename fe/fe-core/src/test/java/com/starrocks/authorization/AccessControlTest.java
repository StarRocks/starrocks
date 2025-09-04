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
package com.starrocks.authorization;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.FunctionName;
import com.starrocks.sql.ast.expression.TableName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AccessControlTest {
    @Test
    public void testBasic() {
        AccessController accessControl = new AccessController() {
        };

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity("test", "%"));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkSystemAction(context, PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkCatalogAction(context, "catalog",
                        PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnCatalog(context, "catalog"));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkDbAction(context, "catalog", "db", PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnDb(context, "catalog", "db"));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkTableAction(context,
                        new TableName("db", "table"), PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnTable(context, new TableName("db", "table")));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyTable(context, "catalog", "db"));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkViewAction(context,
                        new TableName("db", "view"), PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnView(context, new TableName("db", "view")));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkMaterializedViewAction(context, new TableName("db", "mv"), PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnMaterializedView(context, new TableName("db", "mv")));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyMaterializedView(context, "db"));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkFunctionAction(context, null,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false),
                        PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnFunction(context, "db",
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false)));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyFunction(context, "db"));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkGlobalFunctionAction(context,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false),
                        PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnGlobalFunction(context,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false)));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkActionInDb(context, "db", PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkResourceAction(context, "resource", PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnResource(context, "resource"));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkResourceGroupAction(context,
                        "resource group", PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkStorageVolumeAction(context,
                        "resource group", PrivilegeType.USAGE));

        Assertions.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnStorageVolume(context, "storage volume"));
    }
}
