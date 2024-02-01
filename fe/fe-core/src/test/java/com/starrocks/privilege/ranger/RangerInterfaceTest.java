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
package com.starrocks.privilege.ranger;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.privilege.AccessControlProvider;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessController;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.privilege.ranger.starrocks.RangerStarRocksResource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RangerInterfaceTest {

    @Before
    public void setUp() throws Exception {
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }

            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };
    }

    @Test
    public void testMaskingNull() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };
        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");
        List<Column> columns = Lists.newArrayList(new Column("v1", Type.INT));

        RangerStarRocksAccessController connectScheduler = new RangerStarRocksAccessController();
        try {
            connectScheduler.getColumnMaskingPolicy(connectContext, tableName, columns);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testAccessControlProvider() {
        AccessControlProvider accessControlProvider = new AccessControlProvider(null,
                new NativeAccessController());
        accessControlProvider.removeAccessControl("hive");

        accessControlProvider.setAccessControl("hive", new NativeAccessController());
        accessControlProvider.setAccessControl("hive", new RangerStarRocksAccessController());
        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof RangerStarRocksAccessController);
        accessControlProvider.removeAccessControl("hive");

        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof NativeAccessController);

        accessControlProvider.setAccessControl("hive", new RangerStarRocksAccessController());
        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof RangerStarRocksAccessController);
        accessControlProvider.setAccessControl("hive", new NativeAccessController());
        Assert.assertTrue(accessControlProvider.getAccessControlOrDefault("hive")
                instanceof NativeAccessController);
    }

    @Test
    public void testKerberos() throws IOException {
        new Expectations() {
            {
                UserGroupInformation.loginUserFromKeytab((String) any, (String) any);
                minTimes = 0;
            }
        };

        Config.ranger_spnego_kerberos_principal = "HTTP/172.26.92.195@EXAMPLE.COM";
        Config.ranger_spnego_kerberos_keytab = this.getClass().getResource("/").getPath() + "rangadmin.keytab";
        Config.ranger_kerberos_krb5_conf = this.getClass().getResource("/").getPath() + "krb5.conf";

        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        Config.ranger_spnego_kerberos_principal = "";
        Config.ranger_spnego_kerberos_keytab = "";
    }

    @Test
    public void testMaskingExpr() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setMaskType(RangerPolicy.MASK_TYPE_NULL);
                return result;
            }
        };

        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");
        List<Column> columns = Lists.newArrayList(new Column("v1", Type.INT));

        Map<String, Expr> e = rangerStarRocksAccessController.getColumnMaskingPolicy(connectContext, tableName, columns);
        Assert.assertTrue(new ArrayList<>(e.values()).get(0) instanceof NullLiteral);


        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setMaskType(RangerPolicy.MASK_TYPE_CUSTOM);
                result.setMaskedValue("v + 1");
                return result;
            }
        };

        e = rangerStarRocksAccessController.getColumnMaskingPolicy(connectContext, tableName, columns);
        Assert.assertTrue(new ArrayList<>(e.values()).get(0) instanceof ArithmeticExpr);
    }

    @Test
    public void testRowAccessExpr() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setFilterExpr("v1 = 1");
                return result;
            }
        };
        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        TableName tableName = new TableName("db", "tbl");

        Expr rowFilter = rangerStarRocksAccessController.getRowAccessPolicy(connectContext, tableName);
        Assert.assertTrue(rowFilter instanceof BinaryPredicate);
    }

    @Test
    public void testPermission() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };
        RangerStarRocksAccessController rangerStarRocksAccessController = new RangerStarRocksAccessController();

        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);

        try {
            rangerStarRocksAccessController.hasPermission(
                    RangerStarRocksResource.builder().setSystem().build(), UserIdentity.ROOT, PrivilegeType.OPERATE);
        } catch (Exception e) {
            Assert.fail();
        }

        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        Assert.assertThrows(AccessDeniedException.class, () -> rangerStarRocksAccessController.hasPermission(
                RangerStarRocksResource.builder().setSystem().build(), UserIdentity.ROOT, PrivilegeType.OPERATE));
    }
}
