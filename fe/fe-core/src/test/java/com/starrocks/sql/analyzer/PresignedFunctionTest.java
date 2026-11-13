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

package com.starrocks.sql.analyzer;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.thrift.TFunction;
import com.starrocks.type.FileType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class PresignedFunctionTest {
    private static final String VOLUME = "presign_vol";
    private static final String OTHER_VOLUME = "presign_vol_other";
    private static final String DISABLED_VOLUME = "presign_vol_disabled";
    private static final String HDFS_VOLUME = "presign_vol_hdfs";
    private static final String ACCESS_KEY_PROPERTY = "aws.s3.access_key";
    private static final String AI_PROVIDER = "presign_chat";

    private static UserIdentity unprivilegedUser;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        createVolume(VOLUME, "S3", List.of("s3://presign-bucket/media"), s3Properties("presign_ak_1"), true);
        createVolume(OTHER_VOLUME, "S3", List.of("s3://presign-bucket/other"), s3Properties("presign_ak_2"), true);
        createVolume(DISABLED_VOLUME, "S3", List.of("s3://presign-bucket/off"), s3Properties("presign_ak_3"), false);
        createVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, "S3", List.of("s3://presign-bucket/builtin"),
                s3Properties("presign_ak_4"), true);
        Map<String, String> hdfsProperties = new HashMap<>();
        hdfsProperties.put("dfs.nameservices", "ha_cluster");
        hdfsProperties.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        hdfsProperties.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "127.0.0.1:8020");
        hdfsProperties.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "127.0.0.2:8020");
        createVolume(HDFS_VOLUME, "HDFS", List.of("hdfs://ha_cluster/media"), hdfsProperties, true);

        try {
            GlobalStateMgr.getCurrentState().getAIProviderMgr().createProvider(AI_PROVIDER, AIProviderType.CHAT,
                    Map.of("endpoint", "https://models.example.test/v1/chat/completions", "model", "presign-model",
                            "api_key", "presign-api-key"), "");
        } catch (Exception e) {
            // Another test class sharing the cluster may have created it already.
        }

        ConnectContext context = AnalyzeTestUtil.getConnectContext();
        CreateUserStmt createUser = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE USER 'presign_user' IDENTIFIED BY ''", context);
        AuthenticationMgr authenticationMgr = context.getGlobalStateMgr().getAuthenticationMgr();
        authenticationMgr.createUser(createUser);
        UserRef user = createUser.getUser();
        unprivilegedUser = new UserIdentity(user.getUser(), user.getHost());
    }

    private static Map<String, String> s3Properties(String accessKey) {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.region", "us-east-1");
        properties.put("aws.s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        properties.put("aws.s3.use_aws_sdk_default_behavior", "false");
        properties.put("aws.s3.use_instance_profile", "false");
        properties.put(ACCESS_KEY_PROPERTY, accessKey);
        properties.put("aws.s3.secret_key", accessKey + "_secret");
        return properties;
    }

    private static void createVolume(String name, String type, List<String> locations, Map<String, String> properties,
                                     boolean enabled) throws Exception {
        try {
            GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                    .createStorageVolume(name, type, locations, properties, Optional.of(enabled), "");
        } catch (AlreadyExistsException e) {
            // Another test class sharing the cluster may have created it already.
        }
    }

    private static String accessKeyOf(Function function) {
        TFunction thrift = function.toThrift();
        Assertions.assertTrue(thrift.isSetCloud_configuration());
        return thrift.getCloud_configuration().getCloud_properties().get(ACCESS_KEY_PROPERTY);
    }

    // The first presign call in |expr| or below it.
    private static FunctionCallExpr findPresignCall(Expr expr) {
        if (expr instanceof FunctionCallExpr call && AnalyzerUtils.isPresignedBuiltin(call.getFn())) {
            return call;
        }
        for (Expr child : expr.getChildren()) {
            FunctionCallExpr found = findPresignCall(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static void switchUser(ConnectContext context, UserIdentity user) throws Exception {
        context.setCurrentUserIdentity(user);
        context.setCurrentRoleIds(context.getGlobalStateMgr().getAuthorizationMgr().getRoleIdsByUser(user));
        context.setQualifiedUser(user.getUser());
    }

    // Every overload, two storage volumes, a NULL expiration time, and presign calls nested in other functions,
    // followed from analysis to the plan sent to the BE.
    @Test
    public void testEndToEnd() throws Exception {
        String sql = "select get_presigned_url('presign_vol', ta), "
                + "get_presigned_url('presign_vol_other', ta, NULL), "
                + "get_presigned_file('presign_vol_other', to_file(named_struct('uri', ta)), td), "
                + "fl_get_uri(get_presigned_file('presign_vol', to_file(named_struct('uri', ta)))), "
                + "ai_complete(prompt => 'describe', file => get_presigned_file('presign_vol_other', "
                + "to_file(named_struct('uri', ta))), provider => '" + AI_PROVIDER + "') "
                + "from tall";
        QueryStatement statement = (QueryStatement) analyzeSuccess(sql);
        List<Expr> outputs = ((SelectRelation) statement.getQueryRelation()).getOutputExpression();
        List<FunctionCallExpr> calls = outputs.stream().map(PresignedFunctionTest::findPresignCall).toList();
        // Overload, argument count, return type and the access key of the storage volume each call is bound to.
        Object[][] expected = {
                {FunctionSet.GET_PRESIGNED_URL, 2, VarcharType.VARCHAR, "presign_ak_1"},
                {FunctionSet.GET_PRESIGNED_URL, 3, VarcharType.VARCHAR, "presign_ak_2"},
                {FunctionSet.GET_PRESIGNED_FILE, 3, FileType.FILE, "presign_ak_2"},
                {FunctionSet.GET_PRESIGNED_FILE, 2, FileType.FILE, "presign_ak_1"},
                {FunctionSet.GET_PRESIGNED_FILE, 2, FileType.FILE, "presign_ak_2"}};
        for (int i = 0; i < expected.length; ++i) {
            FunctionCallExpr call = calls.get(i);
            Assertions.assertNotNull(call, "output " + i);
            Function fn = call.getFn();
            Assertions.assertEquals(expected[i][0], fn.getFunctionName().getFunction(), "output " + i);
            Assertions.assertEquals(expected[i][1], fn.getNumArgs(), "output " + i);
            Assertions.assertEquals(expected[i][2], call.getType(), "output " + i);
            Assertions.assertEquals(expected[i][3], accessKeyOf(fn), "output " + i);
            Assertions.assertTrue(call.isNullable(), "output " + i);
            if (fn.getNumArgs() == 3) {
                Assertions.assertEquals(IntegerType.BIGINT, fn.getArgs()[2], "output " + i);
            }
        }
        Assertions.assertNotSame(calls.get(0).getFn(), calls.get(3).getFn());

        // The shared builtin is never bound, and copying a bound function keeps its configuration.
        Function builtin = ExprUtils.getBuiltinFunction(FunctionSet.GET_PRESIGNED_URL,
                new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR}, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(builtin);
        Assertions.assertNull(builtin.getCloudConfiguration());
        Assertions.assertEquals("presign_ak_1", accessKeyOf(calls.get(0).getFn().copy()));

        // The calls are not folded away, and the configurations reach the BE with the plan, including the one of the
        // call nested in the AI function. The disabled storage volume is not referenced.
        ConnectContext context = AnalyzeTestUtil.getConnectContext();
        String plan = UtFrameUtils.getFragmentPlan(context, sql);
        Assertions.assertTrue(plan.contains("get_presigned_url"), plan);
        Assertions.assertTrue(plan.contains("get_presigned_file"), plan);
        String thrift = UtFrameUtils.getPlanThriftString(context, sql);
        Assertions.assertTrue(thrift.contains("presign_ak_1"), thrift);
        Assertions.assertTrue(thrift.contains("presign_ak_2"), thrift);
        Assertions.assertFalse(thrift.contains("presign_ak_3"), thrift);
    }

    // Each call fails at analysis with the given message; an empty message accepts any failure. Only the checks
    // of the presign functions are listed here, not those of the generic function lookup.
    @Test
    public void testRejectedCalls() {
        String needsLiteral = "requires the storage volume name to be a non-NULL string literal";
        String[][] cases = {
                {"select get_presigned_url(NULL, 's3://b/k')", needsLiteral},
                {"select get_presigned_url(concat('presign', '_vol'), 's3://b/k')", needsLiteral},
                {"select get_presigned_file(ta, to_file(named_struct('uri', ta))) from tall", needsLiteral},
                {"select get_presigned_url('presign_vol_missing', 's3://b/k')",
                        "storage volume 'presign_vol_missing' does not exist"},
                {"select get_presigned_url('" + DISABLED_VOLUME + "', 's3://b/k')",
                        "storage volume '" + DISABLED_VOLUME + "' is disabled"},
                {"select get_presigned_url('" + HDFS_VOLUME + "', 's3://b/k')", "only supports S3 storage volumes"},
                {"select get_presigned_url('" + StorageVolumeMgr.BUILTIN_STORAGE_VOLUME + "', 's3://b/k')",
                        "cannot use the builtin storage volume"},
                {"select get_presigned_url('presign_vol', tc) from tall", "requires a string URI as its second argument"},
                {"select get_presigned_url('presign_vol', to_file(named_struct('uri', 'a')))", ""},
                {"select get_presigned_file('presign_vol', 's3://b/k')", ""},
                {"select get_presigned_url('presign_vol', 's3://b/k', '60')",
                        "requires an integer expiration time in seconds"},
                {"select get_presigned_url('presign_vol', 's3://b/k', cast(60 as largeint))",
                        "requires an integer expiration time in seconds"},
                // The storage volume is bound by the positional lookup, so named arguments must stay unsupported.
                {"select get_presigned_url(storage_volume => 'presign_vol', target => 's3://b/k')", ""}};
        for (String[] testCase : cases) {
            if (testCase[1].isEmpty()) {
                analyzeFail(testCase[0]);
            } else {
                analyzeFail(testCase[0], testCase[1]);
            }
        }
    }

    @Test
    public void testUsagePrivilege() throws Exception {
        ConnectContext context = AnalyzeTestUtil.getConnectContext();
        String denied = "Access denied";
        try {
            switchUser(context, unprivilegedUser);
            analyzeFail("select get_presigned_url('presign_vol', 's3://b/k')", denied);

            switchUser(context, UserIdentity.ROOT);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "GRANT USAGE ON STORAGE VOLUME presign_vol TO USER 'presign_user'", context), context);

            // USAGE is granted per storage volume.
            switchUser(context, unprivilegedUser);
            analyzeSuccess("select get_presigned_url('presign_vol', 's3://b/k')");
            analyzeFail("select get_presigned_url('" + OTHER_VOLUME + "', 's3://b/k')", denied);
        } finally {
            switchUser(context, UserIdentity.ROOT);
        }
    }

    // PrepareStmtPlanner does not cache the plans of statements with presign calls.
    @Test
    public void testPrepareStatementDetection() {
        Assertions.assertTrue(AnalyzerUtils.containsPresignedFunction(
                analyzeSuccess("select get_presigned_url('presign_vol', ta) from tall where ta = 'a'")));
        Assertions.assertTrue(AnalyzerUtils.containsPresignedFunction(analyzeSuccess(
                "select ta from tall where fl_get_uri(get_presigned_file('presign_vol', to_file(named_struct('uri', ta)))) "
                        + "is not null")));
        Assertions.assertFalse(AnalyzerUtils.containsPresignedFunction(
                analyzeSuccess("select ta from tall where ta = 'a'")));

        // Only the builtins are presign calls; a user function that shares the name is not.
        SqlFunction namesake = new SqlFunction(new FunctionName("test", FunctionSet.GET_PRESIGNED_URL),
                new Type[] {IntegerType.INT}, IntegerType.INT, new String[] {"x"}, "x + 1");
        Assertions.assertFalse(AnalyzerUtils.isPresignedBuiltin(namesake));
        Assertions.assertFalse(AnalyzerUtils.isPresignedBuiltin(null));
    }
}
