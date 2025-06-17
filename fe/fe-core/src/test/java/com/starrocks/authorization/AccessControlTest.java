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
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import org.junit.Assert;
import org.junit.Test;

public class AccessControlTest {
    @Test
    public void testBasic() {
        AccessController accessControl = new AccessController() {
        };

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity("test", "%"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkSystemAction(context, PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkCatalogAction(context, "catalog",
                        PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnCatalog(context, "catalog"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkDbAction(context, "catalog", "db", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnDb(context, "catalog", "db"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkTableAction(context,
                        new TableName("db", "table"), PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnTable(context, new TableName("db", "table")));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyTable(context, "catalog", "db"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkViewAction(context,
                        new TableName("db", "view"), PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnView(context, new TableName("db", "view")));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkMaterializedViewAction(context, new TableName("db", "mv"), PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnMaterializedView(context, new TableName("db", "mv")));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyMaterializedView(context, "db"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkFunctionAction(context, null,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false),
                        PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnFunction(context, "db",
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false)));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnAnyFunction(context, "db"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkGlobalFunctionAction(context,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false),
                        PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnGlobalFunction(context,
                        new ScalarFunction(new FunctionName("db", "fn"), Lists.newArrayList(Type.INT), Type.INT, false)));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkActionInDb(context, "db", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkResourceAction(context, "resource", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnResource(context, "resource"));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkResourceGroupAction(context,
                        "resource group", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkStorageVolumeAction(context,
                        "resource group", PrivilegeType.USAGE));

        Assert.assertThrows(AccessDeniedException.class, () ->
                accessControl.checkAnyActionOnStorageVolume(context, "storage volume"));
    }
}
