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

package com.starrocks.qe;

import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.DictionaryMgr;
import com.starrocks.common.Config;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.lake.LakeMetaVersionNotFoundException;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.RangeColocateUnalignedException;
import com.starrocks.planner.ScanNode;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TAIProvider;
import com.starrocks.thrift.TAIProviderProtocol;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TStatusCode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.fail;

public class ExecuteExceptionHandlerTest extends PlanTestBase {

    private boolean oldDictionaryVersionRetry;

    // The retry is off by default. Most dictionary tests here test the retry itself.
    @BeforeEach
    public void enableDictionaryVersionRetry() {
        oldDictionaryVersionRetry = Config.enable_dictionary_version_retry;
        Config.enable_dictionary_version_retry = true;
    }

    @AfterEach
    public void restoreDictionaryVersionRetry() {
        Config.enable_dictionary_version_retry = oldDictionaryVersionRetry;
    }

    @Test
    public void testHandleRemoteFileNotFoundException_1() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assertions.assertThrows(RemoteFileNotFoundException.class,
                () -> ExecuteExceptionHandler.handle(new RemoteFileNotFoundException("mock"), retryContext));
    }

    @Test
    public void testHandleRemoteFileNotFoundException_2() throws Exception {
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        String sql = "select * from hive0.tpch.customer_view";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        try {
            ExecuteExceptionHandler.handle(new RemoteFileNotFoundException("mock"), retryContext);
        } catch (Exception e) {
            fail("should not throw any exception");
        }
    }

    @Test
    public void testHandleRpcException() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        ExceptionChecker.expectThrowsNoException(() ->
                ExecuteExceptionHandler.handle(new RpcException("mock"), retryContext));
        // execPlan is built
        Assertions.assertNotEquals(retryContext.getExecPlan(), execPlan);
    }

    @Test
    public void testHandleLakeMetaVersionNotFoundException() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assertions.assertEquals(execPlan, retryContext.getExecPlan());

        // Retrying the existing fragments would reuse scan ranges pointing at the unreadable version,
        // so the handler has to plan the statement again instead.
        ExceptionChecker.expectThrowsNoException(() -> ExecuteExceptionHandler.handle(
                new LakeMetaVersionNotFoundException("lake tablet metadata version not found, tablet_id=10001, "
                        + "partition_id=10002, version=144847: Not found"), retryContext));

        Assertions.assertNotEquals(retryContext.getExecPlan(), execPlan);
    }

    @Test
    public void testHandleRangeColocateUnalignedException() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);

        // The plan's tablet -> bucket assignment is stale; only a new plan can dispatch the query.
        ExceptionChecker.expectThrowsNoException(() -> ExecuteExceptionHandler.handle(
                new RangeColocateUnalignedException("range colocate group 1 has a stale bucket assignment in "
                        + "physical partition 2"), retryContext));

        Assertions.assertNotEquals(execPlan, retryContext.getExecPlan());
    }

    @Test
    public void testOtherIllegalStateExceptionIsNotRetried() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);

        Assertions.assertThrows(IllegalStateException.class, () -> ExecuteExceptionHandler.handle(
                new IllegalStateException("unrelated"), retryContext));
        Assertions.assertEquals(execPlan, retryContext.getExecPlan());
    }

    @Test
    public void testLakeMetaVersionNotFoundIsRetryable() {
        Assertions.assertTrue(
                ExecuteExceptionHandler.isRetryableStatus(TStatusCode.LAKE_META_VERSION_NOT_FOUND));
        // A generic NOT_FOUND stays non-retryable: only the dedicated status says the scan version,
        // not the tablet, is the problem.
        Assertions.assertFalse(ExecuteExceptionHandler.isRetryableStatus(TStatusCode.NOT_FOUND));
        Assertions.assertFalse(ExecuteExceptionHandler.isRetryableStatus(TStatusCode.CANCELLED));
    }

    /**
     * The retry log reports the version the fresh plan picked for the partition BE could not read,
     * which is the batch's final version once published. This pins that lookup: it must search the
     * plan's OlapScanNodes by physical partition id, and degrade to empty -- logged as "unknown" --
     * rather than throw when the fresh plan no longer scans that partition.
     *
     * <p>The map is seeded here rather than taken from planning: scan ranges are assigned during
     * scheduling, so a plain getExecPlan() leaves scanPartitionVersions empty. The key space itself
     * is guaranteed inside OlapScanNode.addScanRangeLocations(), which puts physicalPartition.getId()
     * into both this map and TInternalScanRange.partition_id -- the id BE echoes back to us.
     */
    @Test
    public void testFindScanVersionLooksUpByPhysicalPartitionId() throws Exception {
        ExecPlan execPlan = getExecPlan("select * from t0");
        OlapScanNode scanNode = null;
        for (ScanNode node : execPlan.getScanNodes()) {
            if (node instanceof OlapScanNode) {
                scanNode = (OlapScanNode) node;
                break;
            }
        }
        Assertions.assertNotNull(scanNode);
        scanNode.getScanPartitionVersions().put(10002L, 144849L);

        Assertions.assertEquals(OptionalLong.of(144849L), ExecuteExceptionHandler.findScanVersion(execPlan, 10002L));
        Assertions.assertEquals(OptionalLong.empty(), ExecuteExceptionHandler.findScanVersion(execPlan, -1L));
        Assertions.assertEquals(OptionalLong.empty(), ExecuteExceptionHandler.findScanVersion(null, 10002L));
    }

    @Test
    public void testRpcRetryRefreshesProviderConfigurationAfterAlter() throws Exception {
        AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        String name = "rpc_retry_provider";
        manager.createProvider(name, AIProviderType.CHAT, providerProperties("original"), "");
        try {
            String sql = "select ai_complete(provider => '" + name
                    + "', model => 'explicit-model', prompt => ai_complete(provider => '" + name
                    + "', model => 'explicit-model', prompt => k1)) from t7";
            StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            ExecPlan original = getExecPlan(sql);
            assertProviderConfiguration(original, "original");
            ExecuteExceptionHandler.RetryContext retry =
                    new ExecuteExceptionHandler.RetryContext(0, original, connectContext, statement);

            manager.alterProvider(name, providerProperties("altered"), false);
            ExecuteExceptionHandler.handle(new RpcException("mock provider retry"), retry);

            Assertions.assertNotSame(original, retry.getExecPlan());
            ExecPlan altered = retry.getExecPlan();
            assertProviderConfiguration(altered, "altered");
            assertProviderConfiguration(original, "original");
            assertProviderConfiguration(getExecPlan(sql), "altered");

            manager.alterProvider(name, Map.of("protocol", "anthropic"), false);
            retry.setRetryTime(1);
            ExecuteExceptionHandler.handle(new RpcException("mock protocol retry"), retry);
            Assertions.assertNotSame(altered, retry.getExecPlan());
            assertProviderConfiguration(retry.getExecPlan(), "altered", TAIProviderProtocol.ANTHROPIC);
            assertProviderConfiguration(altered, "altered");
            assertProviderConfiguration(original, "original");
            assertProviderConfiguration(getExecPlan(sql), "altered", TAIProviderProtocol.ANTHROPIC);
        } finally {
            manager.dropProvider(name, true);
        }
    }

    @Test
    public void testSchemaRetryRefreshesProviderConfigurationAfterRecreationAndFailsAfterDrop() throws Exception {
        AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        String name = "schema_retry_provider";
        String originalId = manager.createProvider(name, AIProviderType.CHAT, providerProperties("original"), "");
        try {
            String sql = "select ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => k1) from t7";
            StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            ExecPlan original = getExecPlan(sql);
            assertProviderConfiguration(original, "original");
            ExecuteExceptionHandler.RetryContext retry =
                    new ExecuteExceptionHandler.RetryContext(0, original, connectContext, statement);

            manager.dropProvider(name, false);
            String recreatedId = manager.createProvider(name, AIProviderType.CHAT, providerProperties("recreated"), "");
            Assertions.assertNotEquals(originalId, recreatedId);
            ExecuteExceptionHandler.handle(new StarRocksException("invalid field name"), retry);

            Assertions.assertNotSame(original, retry.getExecPlan());
            ExecPlan recreated = retry.getExecPlan();
            assertProviderConfiguration(recreated, "recreated");
            assertProviderConfiguration(original, "original");
            assertProviderConfiguration(getExecPlan(sql), "recreated");

            manager.dropProvider(name, false);
            retry.setRetryTime(1);
            StarRocksException failure = new StarRocksException("invalid field name");
            Assertions.assertSame(failure, Assertions.assertThrows(StarRocksException.class,
                    () -> ExecuteExceptionHandler.handle(failure, retry)));
            Assertions.assertSame(recreated, retry.getExecPlan());
            assertProviderConfiguration(recreated, "recreated");
            assertProviderConfiguration(original, "original");
            SemanticException error = Assertions.assertThrows(SemanticException.class, () -> getExecPlan(sql));
            Assertions.assertEquals("AI provider '" + name + "' does not exist", error.getDetailMsg());
        } finally {
            manager.dropProvider(name, true);
        }
    }

    @Test
    public void testLakeMetaRetryRefreshesProviderConfigurationAfterAlterAndFailsAfterDrop() throws Exception {
        AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        String name = "lake_meta_retry_provider";
        manager.createProvider(name, AIProviderType.CHAT, providerProperties("original"), "");
        try {
            String sql = "select ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => k1) from t7";
            StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            ExecPlan original = getExecPlan(sql);
            ExecuteExceptionHandler.RetryContext retry =
                    new ExecuteExceptionHandler.RetryContext(0, original, connectContext, statement);

            manager.alterProvider(name, providerProperties("altered"), false);
            ExecuteExceptionHandler.handle(new LakeMetaVersionNotFoundException("mock lake metadata retry"), retry);

            ExecPlan altered = retry.getExecPlan();
            Assertions.assertNotSame(original, altered);
            assertProviderConfiguration(altered, "altered");
            assertProviderConfiguration(original, "original");

            manager.dropProvider(name, false);
            retry.setRetryTime(1);
            LakeMetaVersionNotFoundException failure = new LakeMetaVersionNotFoundException("mock lake metadata retry");
            Assertions.assertSame(failure, Assertions.assertThrows(LakeMetaVersionNotFoundException.class,
                    () -> ExecuteExceptionHandler.handle(failure, retry)));
            Assertions.assertSame(altered, retry.getExecPlan());
            assertProviderConfiguration(altered, "altered");
            assertProviderConfiguration(original, "original");
        } finally {
            manager.dropProvider(name, true);
        }
    }

    private static Map<String, String> providerProperties(String version) {
        return Map.of("endpoint", "https://" + version + ".example.test/v1/chat/completions",
                "model", version + "-model", "api_key", version + "-test-key", "timeout_ms", "1200");
    }

    private static void assertProviderConfiguration(ExecPlan plan, String version) {
        assertProviderConfiguration(plan, version, TAIProviderProtocol.OPENAI);
    }

    private static void assertProviderConfiguration(ExecPlan plan, String version, TAIProviderProtocol protocol) {
        List<Map<String, TAIProvider>> configurations = plan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .map(node -> node.getAi_project_node().getAi_provider_mapping())
                .toList();
        Assertions.assertFalse(configurations.isEmpty());
        configurations.forEach(configs -> {
            Assertions.assertEquals(Set.of(configs.values().iterator().next().getId()), configs.keySet());
            TAIProvider configuration = configs.values().iterator().next();
            Assertions.assertEquals(version + "-model", configuration.getParams().get("model"));
            Assertions.assertEquals(version + "-test-key", configuration.getParams().get("api_key"));
            Assertions.assertEquals("https://" + version + ".example.test/v1/chat/completions",
                    configuration.getParams().get("endpoint"));
            Assertions.assertEquals(protocol, configuration.getProtocol());
            Assertions.assertEquals("1200", configuration.getParams().get("timeout_ms"));
        });
    }

    @Test
    public void testHandleUseException_1() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        try {
            ExecuteExceptionHandler.handle(new StarRocksException("invalid field name"), retryContext);
            Assertions.assertTrue(retryContext.getExecPlan() != execPlan);
        } catch (Exception e) {
            fail("should not throw any exception");
        }
    }

    @Test
    public void testHandleUseException_2() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assertions.assertThrows(StarRocksException.class,
                () -> ExecuteExceptionHandler.handle(new StarRocksException("other exception"), retryContext));
    }

    @Test
    public void testHandleUseException_3() throws Exception {
        // cancel with backend not alive, should retry
        String sql = "select * from t1";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assertions.assertEquals(retryContext.getExecPlan(), execPlan);

        ExceptionChecker.expectThrowsNoException(() -> ExecuteExceptionHandler.handle(new StarRocksException(
                InternalErrorCode.CANCEL_NODE_NOT_ALIVE_ERR, FeConstants.BACKEND_NODE_NOT_FOUND_ERROR), retryContext));

        Assertions.assertNotEquals(retryContext.getExecPlan(), execPlan);
    }

    @Test
    public void testRetryOnDictionaryVersionNotFound() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        // The text a CN returns when it already switched to a newer dictionary version, also with the typo fixed.
        String[] messages = {
                "open dictionary expression failed can not found dictionary by verison: 98101, "
                        + "dictionary maybe dropped or updated, dictionary id: 5 backend [id=24305]",
                "open dictionary expression failed can not found dictionary by version: 98101, "
                        + "dictionary maybe dropped or updated, dictionary id: 5 backend [id=24305]",
                // A changed BE text with no version or id still retries, only without the wait.
                "open dictionary expression failed can not found dictionary by verison"};
        for (String message : messages) {
            ExecPlan execPlan = getExecPlan(sql);
            ExecuteExceptionHandler.RetryContext retryContext =
                    new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
            ExceptionChecker.expectThrowsNoException(
                    () -> ExecuteExceptionHandler.handle(new StarRocksException(message), retryContext));
            Assertions.assertNotEquals(retryContext.getExecPlan(), execPlan);
        }
    }

    @Test
    public void testNoRetryOnOtherDictionaryError() throws Exception {
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        Assertions.assertThrows(StarRocksException.class, () -> ExecuteExceptionHandler.handle(
                new StarRocksException("open dictionary expression failed, there is no cache for dictionary: 5"),
                retryContext));
    }

    private static final String VERSION_NOT_FOUND_MESSAGE =
            "open dictionary expression failed can not found dictionary by verison: 98100, "
                    + "dictionary maybe dropped or updated, dictionary id: 5 backend [id=24305]";

    private static Dictionary newCommittingDictionary(long lastSuccessVersion) {
        Dictionary dictionary = new Dictionary(5, "dict", "t", "default_catalog", "testDb",
                List.of("k"), List.of("v"), null);
        dictionary.setFinished(System.currentTimeMillis(), lastSuccessVersion);
        dictionary.setRefreshing(System.currentTimeMillis());
        dictionary.setCommitting();
        return dictionary;
    }

    private static Thread finishLater(Dictionary dictionary, long version) {
        Thread finisher = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            dictionary.setFinished(System.currentTimeMillis(), version);
        });
        finisher.start();
        return finisher;
    }

    @Test
    public void testRetryWaitsForTheDictionaryInTheError() throws Exception {
        Dictionary dictionary = newCommittingDictionary(98100);
        AtomicLong askedId = new AtomicLong(-1);
        new MockUp<DictionaryMgr>() {
            @Mock
            public Dictionary tryGetDictionaryById(long dictionaryId, long timeoutMs) {
                askedId.set(dictionaryId);
                return dictionary;
            }
        };
        long oldWaitMs = Config.dictionary_version_retry_wait_ms;
        Config.dictionary_version_retry_wait_ms = 10000;
        try {
            String sql = "select * from t0";
            StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            ExecPlan execPlan = getExecPlan(sql);
            ExecuteExceptionHandler.RetryContext retryContext =
                    new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
            Thread finisher = finishLater(dictionary, 98101);
            long start = System.currentTimeMillis();
            ExceptionChecker.expectThrowsNoException(() -> ExecuteExceptionHandler.handle(
                    new StarRocksException(VERSION_NOT_FOUND_MESSAGE), retryContext));
            long elapsed = System.currentTimeMillis() - start;
            finisher.join();
            // The id and the version come from the error text: a wrong parse does not wait for the finisher.
            Assertions.assertEquals(5, askedId.get());
            Assertions.assertTrue(elapsed >= 100, "waited " + elapsed + " ms");
            Assertions.assertTrue(elapsed < 2000, "waited " + elapsed + " ms");
            Assertions.assertNotEquals(retryContext.getExecPlan(), execPlan);
        } finally {
            Config.dictionary_version_retry_wait_ms = oldWaitMs;
        }
    }

    @Test
    public void testNoWaitOnDictionaryVersionNotFoundAfterRowsWereSent() throws Exception {
        new MockUp<MysqlChannel>() {
            @Mock
            public boolean isSend() {
                return true;
            }
        };
        new MockUp<DictionaryMgr>() {
            @Mock
            public Dictionary tryGetDictionaryById(long dictionaryId, long timeoutMs) {
                return newCommittingDictionary(98100);
            }
        };
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        long start = System.currentTimeMillis();
        Assertions.assertThrows(StarRocksException.class, () -> ExecuteExceptionHandler.handle(
                new StarRocksException(VERSION_NOT_FOUND_MESSAGE), retryContext));
        Assertions.assertTrue(System.currentTimeMillis() - start < 1000);
        Assertions.assertEquals(retryContext.getExecPlan(), execPlan);
    }

    @Test
    public void testNoRetryWhenTheVersionDoesNotMove() throws Exception {
        // No refresh runs, as when a restarted CN has no copy: a new plan would fail the same way.
        new MockUp<DictionaryMgr>() {
            @Mock
            public Dictionary tryGetDictionaryById(long dictionaryId, long timeoutMs) {
                Dictionary idle = newCommittingDictionary(98100);
                idle.setCancelled();
                return idle;
            }
        };
        String sql = "select * from t0";
        StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan execPlan = getExecPlan(sql);
        ExecuteExceptionHandler.RetryContext retryContext =
                new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
        long start = System.currentTimeMillis();
        Assertions.assertThrows(StarRocksException.class, () -> ExecuteExceptionHandler.handle(
                new StarRocksException(VERSION_NOT_FOUND_MESSAGE), retryContext));
        Assertions.assertTrue(System.currentTimeMillis() - start < 2000);
        Assertions.assertEquals(retryContext.getExecPlan(), execPlan);
    }

    @Test
    public void testNoRetryWhenTheRetryIsDisabled() throws Exception {
        boolean oldEnabled = Config.enable_dictionary_version_retry;
        Config.enable_dictionary_version_retry = false;
        try {
            String sql = "select * from t0";
            StatementBase statementBase = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
            ExecPlan execPlan = getExecPlan(sql);
            ExecuteExceptionHandler.RetryContext retryContext =
                    new ExecuteExceptionHandler.RetryContext(0, execPlan, connectContext, statementBase);
            Assertions.assertThrows(StarRocksException.class, () -> ExecuteExceptionHandler.handle(
                    new StarRocksException(VERSION_NOT_FOUND_MESSAGE), retryContext));
            Assertions.assertEquals(retryContext.getExecPlan(), execPlan);
        } finally {
            Config.enable_dictionary_version_retry = oldEnabled;
        }
    }

    @Test
    public void testWaitForNewDictionaryVersionEndsWhenCommitFinishes() throws Exception {
        Dictionary dictionary = newCommittingDictionary(98100);
        Thread finisher = finishLater(dictionary, 98101);
        long start = System.currentTimeMillis();
        Assertions.assertTrue(ExecuteExceptionHandler.waitForNewDictionaryVersion(dictionary, 98100, true, 10000));
        finisher.join();
        Assertions.assertTrue(System.currentTimeMillis() - start < 2000);
        Assertions.assertEquals(98101, dictionary.getLastSuccessVersion());
    }

    @Test
    public void testWaitForNewDictionaryVersionIsBounded() throws Exception {
        Dictionary dictionary = newCommittingDictionary(98100);
        long start = System.currentTimeMillis();
        Assertions.assertFalse(ExecuteExceptionHandler.waitForNewDictionaryVersion(dictionary, 98100, true, 100));
        Assertions.assertTrue(System.currentTimeMillis() - start >= 100);
        Assertions.assertTrue(dictionary.isRefreshing());
    }

    @Test
    public void testWaitForNewDictionaryVersionSkipsWhenNotNeeded() throws Exception {
        long start = System.currentTimeMillis();
        // The FE already plans with a newer version than the failed query.
        Assertions.assertTrue(ExecuteExceptionHandler.waitForNewDictionaryVersion(
                newCommittingDictionary(98101), 98100, true, 10000));
        Assertions.assertTrue(ExecuteExceptionHandler.waitForNewDictionaryVersion(
                newCommittingDictionary(98101), 98100, false, 10000));
        // The FE does not know the dictionary, so the new plan reports that.
        Assertions.assertTrue(ExecuteExceptionHandler.waitForNewDictionaryVersion(null, 98100, true, 10000));
        // The leader sees that no refresh runs, so no newer version can come.
        Dictionary idle = newCommittingDictionary(98100);
        idle.setCancelled();
        Assertions.assertFalse(ExecuteExceptionHandler.waitForNewDictionaryVersion(idle, 98100, true, 10000));
        Assertions.assertTrue(System.currentTimeMillis() - start < 1000);
    }

    @Test
    public void testFollowerWaitsForAnIdleDictionaryToChangeVersion() throws Exception {
        // A follower that did not replay the refresh yet sees the old version as FINISHED.
        Dictionary dictionary = newCommittingDictionary(98100);
        dictionary.setFinished(System.currentTimeMillis(), 98100);
        Thread finisher = finishLater(dictionary, 98101);
        long start = System.currentTimeMillis();
        Assertions.assertTrue(ExecuteExceptionHandler.waitForNewDictionaryVersion(dictionary, 98100, false, 10000));
        long elapsed = System.currentTimeMillis() - start;
        finisher.join();
        Assertions.assertTrue(elapsed >= 100, "waited " + elapsed + " ms");
        Assertions.assertTrue(elapsed < 2000, "waited " + elapsed + " ms");
    }

    @Test
    public void testFollowerWaitsOnlyBrieflyForAnIdleDictionary() throws Exception {
        // No commit comes, as when a restarted CN has no copy: do not wait the full timeout.
        Dictionary finished = newCommittingDictionary(98100);
        finished.setFinished(System.currentTimeMillis(), 98100);
        for (Dictionary dictionary : List.of(finished, newLoadingDictionary(98100))) {
            long start = System.currentTimeMillis();
            Assertions.assertFalse(
                    ExecuteExceptionHandler.waitForNewDictionaryVersion(dictionary, 98100, false, 10000));
            long elapsed = System.currentTimeMillis() - start;
            Assertions.assertTrue(elapsed >= ExecuteExceptionHandler.DICTIONARY_REPLAY_GRACE_MS,
                    "waited " + elapsed + " ms");
            Assertions.assertTrue(elapsed < 2000, "waited " + elapsed + " ms");
        }
    }

    private static Dictionary newLoadingDictionary(long lastSuccessVersion) {
        Dictionary dictionary = new Dictionary(5, "dict", "t", "default_catalog", "testDb",
                List.of("k"), List.of("v"), null);
        dictionary.setFinished(System.currentTimeMillis(), lastSuccessVersion);
        dictionary.setRefreshing(System.currentTimeMillis());
        return dictionary;
    }

    @Test
    public void testLeaderDoesNotWaitWhileTheDataLoads() throws Exception {
        // No CN switches before COMMITTING, so the error means that the CN has no copy.
        Dictionary dictionary = newLoadingDictionary(98100);
        long start = System.currentTimeMillis();
        Assertions.assertFalse(ExecuteExceptionHandler.waitForNewDictionaryVersion(dictionary, 98100, true, 10000));
        Assertions.assertTrue(System.currentTimeMillis() - start < 1000);
        Assertions.assertTrue(dictionary.isRefreshing());
    }

    @Test
    public void testFollowerWaitsForTheCommitAfterItReplaysIt() throws Exception {
        // The follower still sees the data load, then replays COMMITTING within the grace and FINISHED after it.
        Dictionary dictionary = newLoadingDictionary(98100);
        Thread replayer = new Thread(() -> {
            try {
                Thread.sleep(100);
                dictionary.setCommitting();
                Thread.sleep(ExecuteExceptionHandler.DICTIONARY_REPLAY_GRACE_MS + 300);
                dictionary.setFinished(System.currentTimeMillis(), 98101);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        replayer.start();
        long start = System.currentTimeMillis();
        Assertions.assertTrue(ExecuteExceptionHandler.waitForNewDictionaryVersion(dictionary, 98100, false, 10000));
        long elapsed = System.currentTimeMillis() - start;
        replayer.join();
        Assertions.assertTrue(elapsed >= ExecuteExceptionHandler.DICTIONARY_REPLAY_GRACE_MS + 300,
                "waited " + elapsed + " ms");
        Assertions.assertTrue(elapsed < 5000, "waited " + elapsed + " ms");
    }

    @Test
    public void testFollowerWaitsTheFullTimeoutWhileTheRefreshCommits() throws Exception {
        // The follower replayed COMMITTING, so the commit can still run past the grace.
        Dictionary dictionary = newCommittingDictionary(98100);
        long start = System.currentTimeMillis();
        Assertions.assertFalse(ExecuteExceptionHandler.waitForNewDictionaryVersion(dictionary, 98100, false, 1000));
        Assertions.assertTrue(System.currentTimeMillis() - start >= 1000);
    }
}
