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
package com.starrocks.privilege.cauthz;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Config;
import com.starrocks.privilege.AccessControlProvider;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessController;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.cauthz.MockedCauthzAuthorizer;
import com.starrocks.privilege.cauthz.starrocks.CauthzStarRocksAccessController;
import com.starrocks.privilege.cauthz.starrocks.CauthzStarRocksResource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class CauthzInterfaceTest {
    ConnectContext connectContext;
    StarRocksAssert starRocksAssert;     

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("db").useDatabase("db").withTable("CREATE TABLE `t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test 
    public void testAccessControlProvider() {
        AccessControlProvider accessControlProvider = new AccessControlProvider(null, new NativeAccessController());

        accessControlProvider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());

        accessControlProvider.setAccessControl(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, 
                new CauthzStarRocksAccessController());

        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME) instanceof CauthzStarRocksAccessController);

        accessControlProvider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());

        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME) instanceof NativeAccessController);
    }

    @Test 
    public void testPermission() {
        Config.cauthz_authorization_class_name = MockedCauthzAuthorizer.class.getName();
        new MockUp<MockedCauthzAuthorizer>() {
            @Mock
            public boolean authorize(CauthzStarRocksAccessRequest request) {
                return true;
            }
        };

        CauthzStarRocksAccessController cauthzStarRocksAccessController = new CauthzStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);

        try {
            cauthzStarRocksAccessController.hasPermission(
                    CauthzStarRocksResource.builder().setCatalog("default_catalog").setDatabase("db")
                                        .setTable("t1").build(), UserIdentity.ROOT, PrivilegeType.SELECT);
        } catch (Exception e) {
            Assert.fail();
        }

        new MockUp<MockedCauthzAuthorizer>() {
            @Mock
            public boolean authorize(CauthzStarRocksAccessRequest request) {
                return false;
            }
        };

        try {
            cauthzStarRocksAccessController.hasPermission(
                    CauthzStarRocksResource.builder().setCatalog("default_catalog").setDatabase("db")
                                        .setTable("t1").build(), UserIdentity.ROOT, PrivilegeType.SELECT);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof AccessDeniedException);
        }
    }
}
