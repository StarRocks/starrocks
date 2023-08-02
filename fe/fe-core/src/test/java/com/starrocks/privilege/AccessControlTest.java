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
package com.starrocks.privilege;

import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.UserIdentity;
import org.junit.Assert;
import org.junit.Test;

public class AccessControlTest {
    @Test
    public void testBasic() {
        AccessControl accessControl = new AccessControl() {
        };

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkSystemAction(new UserIdentity("test", "%"), null, PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkCatalogAction(new UserIdentity("test", "%"), null, "catalog",
                        PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnCatalog(new UserIdentity("test", "%"), null, "catalog"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkDbAction(new UserIdentity("test", "%"), null, "catalog", "db", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnDb(new UserIdentity("test", "%"), null, "catalog", "db"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkTableAction(new UserIdentity("test", "%"), null,
                        new TableName("db", "table"), PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnTable(new UserIdentity("test", "%"), null, new TableName("db", "table")));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyTable(new UserIdentity("test", "%"), null, "catalog", "db"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkViewAction(new UserIdentity("test", "%"), null,
                        new TableName("db", "view"), PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnView(new UserIdentity("test", "%"), null, new TableName("db", "view")));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkMaterializedViewAction(new UserIdentity("test", "%"),
                        null, new TableName("db", "mv"), PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnMaterializedView(new UserIdentity("test", "%"), null, new TableName("db", "mv")));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyMaterializedView(new UserIdentity("test", "%"), null, "db"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkFunctionAction(new UserIdentity("test", "%"), null, null,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false),
                        PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnFunction(new UserIdentity("test", "%"), null, "db",
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false)));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyFunction(new UserIdentity("test", "%"), null, "db"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkGlobalFunctionAction(new UserIdentity("test", "%"), null,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false),
                        PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnGlobalFunction(new UserIdentity("test", "%"), null,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false)));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkActionInDb(new UserIdentity("test", "%"), null, "db", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkResourceAction(new UserIdentity("test", "%"), null, "resource", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnResource(new UserIdentity("test", "%"), null, "resource"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkResourceGroupAction(new UserIdentity("test", "%"), null,
                        "resource group", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkStorageVolumeAction(new UserIdentity("test", "%"), null,
                        "resource group", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnStorageVolume(new UserIdentity("test", "%"),
                        null, "storage volume"));
    }
}
