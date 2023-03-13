package com.starrocks.analysis;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.starrocks.common.ErrorCode.ERROR_NO_RG_ERROR;

public class ResourceGroupStmtTest {
    private static final Pattern idPattern = Pattern.compile("\\bid=(\\b\\d+\\b)");
    private static StarRocksAssert starRocksAssert;
    private String createRg1Sql = "create resource group rg1\n" +
            "to\n" +
            "    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.2.1/24'),\n" +
            "    (user='rg1_user2', query_type in ('select'), source_ip='192.168.3.1/24'),\n" +
            "    (user='rg1_user3', source_ip='192.168.4.1/24'),\n" +
            "    (user='rg1_user4')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '10',\n" +
            "    'mem_limit' = '20%',\n" +
            "    'concurrency_limit' = '11',\n" +
            "    'type' = 'normal'\n" +
            ");";
    private String createRg1IfNotExistsSql = "create resource group if not exists rg1\n" +
            "to\n" +
            "    (user='rg1_if_not_exists', role='rg1_role1', query_type in ('select'), source_ip='192.168.2.1/24')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '10',\n" +
            "    'mem_limit' = '20%',\n" +
            "    'concurrency_limit' = '11',\n" +
            "    'type' = 'normal'\n" +
            ");";

    private String createRg1OrReplaceSql = "create resource group or replace rg1\n" +
            "to\n" +
            "    (user='rg1_or_replace', role='rg1_role1', query_type in ('select'), source_ip='192.168.2.1/24')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '10',\n" +
            "    'mem_limit' = '20%',\n" +
            "    'concurrency_limit' = '11',\n" +
            "    'type' = 'normal'\n" +
            ");";

    private String createRg1IfNotExistsOrReplaceSql = "create resource group if not exists or replace rg1\n" +
            "to\n" +
            "    (user='rg1_if_not_exists_or_replace', role='rg1_role1', " +
            "query_type in ('select'), source_ip='192" +
            ".168.2.1/24')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '10',\n" +
            "    'mem_limit' = '20%',\n" +
            "    'concurrency_limit' = '11',\n" +
            "    'type' = 'normal'\n" +
            ");";

    private String createRg2Sql = "create resource group rg2\n" +
            "to\n" +
            "    (role='rg2_role1', query_type in ('select' ), source_ip='192.168.5.1/24'),\n" +
            "    (role='rg2_role2', source_ip='192.168.6.1/24'),\n" +
            "    (role='rg2_role3')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '30',\n" +
            "    'mem_limit' = '50%',\n" +
            "    'concurrency_limit' = '20',\n" +
            "    'type' = 'normal'\n" +
            ");";
    private String createRg3Sql = "create resource group rg3\n" +
            "to\n" +
            "    (query_type in ('select'), source_ip='192.168.6.1/24'),\n" +
            "    (query_type in ('select'))\n" +
            "with (\n" +
            "    'cpu_core_limit' = '32',\n" +
            "    'mem_limit' = '80%',\n" +
            "    'concurrency_limit' = '10',\n" +
            "    'type' = 'normal'\n" +
            ");";
    private String createRg4Sql = "create resource group rg4\n" +
            "to\n" +
            "     (source_ip='192.168.7.1/24')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '25',\n" +
            "    'mem_limit' = '80%',\n" +
            "    'concurrency_limit' = '10',\n" +
            "   'big_query_mem_limit'='1024',\n" +
            "   'big_query_scan_rows_limit'='1024',\n" +
            "   'big_query_cpu_second_limit'='1024',\n" +
            "    'type' = 'normal'\n" +
            ");";
    private String createRg5Sql = "create resource group rg5\n" +
            "to\n" +
            "     (`db`='db1')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '25',\n" +
            "    'mem_limit' = '80%',\n" +
            "    'concurrency_limit' = '10',\n" +
            "    'type' = 'normal'\n" +
            ");";
    private String createRg6Sql = "create resource group rg6\n" +
            "to\n" +
            "    (query_type in ('insert'), source_ip='192.168.6.1/24')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '32',\n" +
            "    'mem_limit' = '80%',\n" +
            "    'concurrency_limit' = '10',\n" +
            "    'type' = 'normal'\n" +
            ");";

    private String createRtRg1Sql = "create resource group rt_rg1\n" +
            "to\n" +
            "     (user='rt_rg_user')\n" +
            "with (\n" +
            "    'cpu_core_limit' = '25',\n" +
            "    'mem_limit' = '80%',\n" +
            "    'concurrency_limit' = '10',\n" +
            "    'type' = 'short_query'\n" +
            ");";

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        FeConstants.default_scheduler_interval_millisecond = 1;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withRole("rg1_role1");
        starRocksAssert.withUser("rg1_user1");
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("grant rg1_role1 to rg1_user1",
                ctx), ctx);
        List<String> databases = Arrays.asList("db1", "db2");
        for (String db : databases) {
            starRocksAssert.withDatabase(db);
        }
    }

    private static String rowsToString(List<List<String>> rows) {
        List<String> lines = rows.stream().map(
                row -> {
                    row.remove(1);
                    return String.join("|",
                            row.toArray(new String[0])).replaceAll("id=\\d+(,\\s+)?", "");
                }
        ).collect(Collectors.toList());
        return String.join("\n", lines.toArray(new String[0]));
    }

    private static String getClassifierId(String classifierStr) {
        Matcher mat = idPattern.matcher(classifierStr);
        Assert.assertTrue(mat.find());
        return mat.group(1);
    }

    private void createResourceGroups() throws Exception {
        String[] sqls = new String[] {createRg1Sql, createRg2Sql,
                createRg3Sql, createRg4Sql, createRg5Sql, createRg6Sql, createRtRg1Sql};
        for (String sql : sqls) {
            starRocksAssert.executeResourceGroupDdlSql(sql);
        }
    }

    private void dropResourceGroups() throws Exception {
        String[] rgNames = new String[] {"rg1", "rg2", "rg3", "rg4", "rg5", "rg6", "rt_rg1"};
        for (String name : rgNames) {
            starRocksAssert.executeResourceGroupDdlSql("DROP RESOURCE GROUP " + name);
        }
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        Assert.assertTrue(rows.toString(), rows.isEmpty());
    }

    private void assertResourceGroupNotExist(String wg) {
        Assert.assertThrows(ERROR_NO_RG_ERROR.formatErrorMsg(wg), AnalysisException.class,
                () -> starRocksAssert.executeResourceGroupShowSql("show resource group " + wg));
    }

    @Test
    public void testCreateResourceGroup() throws Exception {
        createResourceGroups();
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String result = rowsToString(rows);
        String expect = "" +
                "rg1|10|20.0%|0|0|0|11|NORMAL|(weight=4.459375, user=rg1_user1, " +
                "role=rg1_role1, query_type in " +
                "(SELECT), source_ip=192.168.2.1/24)\n" +
                "rg1|10|20.0%|0|0|0|11|NORMAL|(weight=3.459375, user=rg1_user2, " +
                "query_type in (SELECT), " +
                "source_ip=192.168.3.1/24)\n" +
                "rg1|10|20.0%|0|0|0|11|NORMAL|(weight=2.359375, user=rg1_user3, source_ip=192.168.4.1/24)\n" +
                "rg1|10|20.0%|0|0|0|11|NORMAL|(weight=1.0, user=rg1_user4)\n" +
                "rg2|30|50.0%|0|0|0|20|NORMAL|(weight=3.459375, role=rg2_role1, " +
                "query_type in (SELECT), " +
                "source_ip=192.168.5.1/24)\n" +
                "rg2|30|50.0%|0|0|0|20|NORMAL|(weight=2.359375, role=rg2_role2, source_ip=192.168.6.1/24)\n" +
                "rg2|30|50.0%|0|0|0|20|NORMAL|(weight=1.0, role=rg2_role3)\n" +
                "rg3|32|80.0%|0|0|0|10|NORMAL|(weight=2.459375, query_type in (SELECT), source_ip=192.168.6.1/24)\n" +
                "rg3|32|80.0%|0|0|0|10|NORMAL|(weight=1.1, query_type in (SELECT))\n" +
                "rg4|25|80.0%|1024|1024|1024|10|NORMAL|(weight=1.359375, source_ip=192.168.7.1/24)\n" +
                "rg5|25|80.0%|0|0|0|10|NORMAL|(weight=10.0, db='db1')\n" +
                "rg6|32|80.0%|0|0|0|10|NORMAL|(weight=2.459375, query_type in (INSERT), source_ip=192.168.6.1/24)\n" +
                "rt_rg1|25|80.0%|0|0|0|10|SHORT_QUERY|(weight=1.0, user=rt_rg_user)";
        Assert.assertEquals(result, expect);
        dropResourceGroups();
    }

    @Test
    public void testCreateDuplicateResourceGroup() throws Exception {
        starRocksAssert.executeResourceGroupDdlSql(createRg1Sql);
        starRocksAssert.executeResourceGroupDdlSql("DROP RESOURCE GROUP rg1");
        assertResourceGroupNotExist("rg1");

        starRocksAssert.executeResourceGroupDdlSql(createRg1IfNotExistsSql);
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("show resource group rg1");
        Assert.assertEquals(rows.size(), 1);
        String actual = rowsToString(rows);
        Assert.assertTrue(actual.contains("rg1_if_not_exists"));

        starRocksAssert.executeResourceGroupDdlSql(createRg1OrReplaceSql);
        rows = starRocksAssert.executeResourceGroupShowSql("show resource group rg1");
        Assert.assertEquals(rows.size(), 1);
        actual = rowsToString(rows);
        Assert.assertTrue(actual.contains("rg1_or_replace"));

        starRocksAssert.executeResourceGroupDdlSql(createRg1IfNotExistsSql);
        rows = starRocksAssert.executeResourceGroupShowSql("show resource group rg1");
        Assert.assertEquals(rows.size(), 1);
        actual = rowsToString(rows);
        Assert.assertTrue(actual.contains("rg1_or_replace"));

        starRocksAssert.executeResourceGroupDdlSql("DROP resource group rg1");
        assertResourceGroupNotExist("rg1");

        starRocksAssert.executeResourceGroupDdlSql(createRg1OrReplaceSql);
        rows = starRocksAssert.executeResourceGroupShowSql("show resource group rg1");
        Assert.assertEquals(rows.size(), 1);
        actual = rowsToString(rows);
        Assert.assertTrue(actual.contains("rg1_or_replace"));

        starRocksAssert.executeResourceGroupDdlSql(createRg1IfNotExistsOrReplaceSql);
        rows = starRocksAssert.executeResourceGroupShowSql("show resource group rg1");
        Assert.assertEquals(rows.size(), 1);
        actual = rowsToString(rows);
        System.out.println(actual);
        Assert.assertTrue(actual.contains("rg1_if_not_exists_or_replace"));

        starRocksAssert.executeResourceGroupDdlSql("DROP RESOURCE GROUP rg1");
    }

    @Test
    public void testCreateDuplicateResourceGroupFail() throws Exception {
        starRocksAssert.executeResourceGroupDdlSql(createRg1Sql);
        try {
            starRocksAssert.executeResourceGroupDdlSql(createRg1Sql);
            Assert.fail("should throw error");
        } catch (Exception ignored) {
        }
        starRocksAssert.executeResourceGroupDdlSql("DROP RESOURCE GROUP rg1");
    }

    @Test
    public void testCreateDResourceGroupWithIllegalProperty() throws Exception {
        String unknownPropertySql = "create resource group rg_unknown\n" +
                "to\n" +
                "    (user='rg1_user3', source_ip='192.168.4.1/24'),\n" +
                "    (user='rg1_user4')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '10',\n" +
                "    'mem_limit' = '20%',\n" +
                "    'concurrency_limit' = '11',\n" +
                "    'type' = 'normal', \n" +
                "    'unknown' = 'unknown'" +
                ");";
        Assert.assertThrows("Unknown property: unknown",
                SemanticException.class, () -> starRocksAssert.executeResourceGroupDdlSql(unknownPropertySql));

        String illegalTypeSql = "create resource group rg_unknown\n" +
                "to\n" +
                "    (user='rg1_user3', source_ip='192.168.4.1/24'),\n" +
                "    (user='rg1_user4')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '10',\n" +
                "    'mem_limit' = '20%',\n" +
                "    'concurrency_limit' = '11',\n" +
                "    'type' = 'illegal-type'" +
                ");";
        Assert.assertThrows("Only support 'normal' and 'short_query' type",
                SemanticException.class, () -> starRocksAssert.executeResourceGroupDdlSql(illegalTypeSql));

        String illegalDefaultTypeSql = "create resource group rg_unknown\n" +
                "to\n" +
                "    (user='rg1_user3', source_ip='192.168.4.1/24'),\n" +
                "    (user='rg1_user4')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '10',\n" +
                "    'mem_limit' = '20%',\n" +
                "    'concurrency_limit' = '11',\n" +
                "    'type' = 'default'" +
                ");";
        Assert.assertThrows("Only support 'normal' and 'short_query' type",
                SemanticException.class, () -> starRocksAssert.executeResourceGroupDdlSql(illegalDefaultTypeSql));
    }

    @Test
    public void testQueryType() throws Exception {
        String sql1 = "create resource group rg_insert\n" +
                "to (user='rg_user3', query_type in ('mv')) with ('cpu_core_limit' = '10', 'mem_limit' = '20%')";
        try {
            starRocksAssert.executeResourceGroupDdlSql(sql1);
            Assert.fail("should throw error");
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Unsupported query_type: 'mv'.",
                    e.getMessage());
        }

    }

    @Test
    public void testAlterResourceGroupDropManyClassifiers() throws Exception {
        createResourceGroups();
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String rgName = rows.get(0).get(0);
        List<String> classifierIds = rows.stream().filter(row -> row.get(0).equals(rgName))
                .map(row -> getClassifierId(row.get(row.size() - 1))).collect(Collectors.toList());
        rows = rows.stream().peek(row -> {
            if (row.get(0).equals(rgName)) {
                row.set(row.size() - 1, "(weight=0.0)");
            }
        }).distinct().collect(Collectors.toList());
        String ids = String.join(",", classifierIds);
        String alterSql = String.format("ALTER RESOURCE GROUP %s DROP (%s)", rgName, ids);
        starRocksAssert.executeResourceGroupDdlSql(alterSql);
        String expect = rowsToString(rows);
        rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String actual = rowsToString(rows);
        Assert.assertEquals(expect, actual);
        dropResourceGroups();
    }

    @Test
    public void testAlterResourceGroupDropOneClassifier() throws Exception {
        createResourceGroups();
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        Random rand = new Random();
        Map<String, Integer> classifierCount = new HashMap<>();
        rows.forEach(row -> classifierCount
                .compute(row.get(0), (rg, countClassifier) -> countClassifier == null ? 1 : countClassifier + 1));
        Iterator<List<String>> it = rows.iterator();
        while (it.hasNext()) {
            List<String> row = it.next();
            if (rand.nextBoolean()) {
                continue;
            }
            String rgName = row.get(0);
            String classifier = row.get(row.size() - 1);
            String id = getClassifierId(classifier);
            Integer count = classifierCount.computeIfPresent(rgName, (rg, countClassifier) -> countClassifier - 1);
            if (count != 0) {
                // not last classifier of the rg, remove it
                it.remove();
            } else {
                row.set(row.size() - 1, "(weight=0.0)");
            }
            String alterSql = String.format("ALTER RESOURCE GROUP %s DROP (%s)", rgName, id);
            starRocksAssert.executeResourceGroupDdlSql(alterSql);
        }
        String expect = rowsToString(rows);
        rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String actual = rowsToString(rows);
        Assert.assertEquals(expect, actual);
        dropResourceGroups();
    }

    @Test
    public void testAlterResourceGroupDropAllClassifier() throws Exception {
        createResourceGroups();
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String rgName = "rg2";
        rows = rows.stream().peek(row -> {
            if (row.get(0).equals(rgName)) {
                row.set(row.size() - 1, "(weight=0.0)");
            }
        }).distinct().collect(Collectors.toList());
        String alterSql = String.format("ALTER RESOURCE GROUP %s DROP ALL", rgName);
        starRocksAssert.executeResourceGroupDdlSql(alterSql);
        String expect = rowsToString(rows);
        rows = starRocksAssert.executeResourceGroupShowSql("show resource groups all");
        String actual = rowsToString(rows);
        Assert.assertEquals(expect, actual);
        dropResourceGroups();
    }

    @Test
    public void testAlterResourceGroupAddOneClassifier() throws Exception {
        createResourceGroups();
        String sql = "" +
                "ALTER RESOURCE GROUP rg1 \n" +
                "ADD \n" +
                "   (user='rg1_user5', role='rg1_role5', source_ip='192.168.4.1/16')";

        starRocksAssert.executeResourceGroupDdlSql(sql);
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("SHOW RESOURCE GROUP rg1");
        String result = rowsToString(rows);
        Assert.assertTrue(result.contains("rg1_user5"));
        dropResourceGroups();
    }

    @Test
    public void testAlterResourceGroupAddManyClassifiers() throws Exception {
        createResourceGroups();
        String sql = "" +
                "ALTER resource group rg2 \n" +
                "ADD \n" +
                "   (user='rg2_user4', role='rg2_role4', source_ip='192.168.4.1/16'),\n" +
                "   (role='rg2_role5', source_ip='192.168.5.1/16'),\n" +
                "   (source_ip='192.169.6.1/16');\n";

        starRocksAssert.executeResourceGroupDdlSql(sql);
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("SHOW RESOURCE GROUP rg2");
        String result = rowsToString(rows);
        Assert.assertTrue(result.contains("rg2_user4"));
        Assert.assertTrue(result.contains("rg2_role5"));
        Assert.assertTrue(result.contains("192.169.6.1/16"));
        dropResourceGroups();
    }

    @Test
    public void testShowUnknownOrNotExistWorkGroup() throws Exception {
        starRocksAssert.executeResourceGroupDdlSql(createRg1Sql);

        starRocksAssert.executeResourceGroupDdlSql("ALTER RESOURCE GROUP rg1 DROP ALL;");
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("show resource group rg1");
        String actual = rowsToString(rows);
        String expect = "rg1|10|20.0%|0|0|0|11|NORMAL|(weight=0.0)";
        Assert.assertEquals(actual, expect);

        starRocksAssert.executeResourceGroupDdlSql("DROP RESOURCE GROUP rg1");
        assertResourceGroupNotExist("rg1");
    }

    @Test
    public void testChooseResourceGroup() throws Exception {
        createResourceGroups();
        String qualifiedUser = "rg1_user1";
        String remoteIp = "192.168.2.4";
        starRocksAssert.getCtx().setQualifiedUser(qualifiedUser);
        starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity(qualifiedUser, "%"));
        starRocksAssert.getCtx().setRemoteIP(remoteIp);
        TWorkGroup wg = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroup(
                starRocksAssert.getCtx(),
                ResourceGroupClassifier.QueryType.SELECT,
                null);
        Assert.assertEquals("rg1", wg.getName());
        dropResourceGroups();
    }

    @Test
    public void testChooseInsertResourceGroup() throws Exception {
        createResourceGroups();
        String qualifiedUser = "rg1_user1";
        String remoteIp = "192.168.6.4";
        starRocksAssert.getCtx().setQualifiedUser(qualifiedUser);
        starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity(qualifiedUser, "%"));
        starRocksAssert.getCtx().setRemoteIP(remoteIp);
        TWorkGroup wg = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroup(
                starRocksAssert.getCtx(),
                ResourceGroupClassifier.QueryType.INSERT,
                null);
        Assert.assertEquals(wg.getName(), "rg6");
        dropResourceGroups();
    }

    @Test
    public void testChooseResourceGroupWithDb() throws Exception {
        createResourceGroups();
        String qualifiedUser = "rg1_user1";
        String remoteIp = "192.168.2.4";
        starRocksAssert.getCtx().setQualifiedUser(qualifiedUser);
        starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity(qualifiedUser, "%"));
        starRocksAssert.getCtx().setRemoteIP(remoteIp);
        {
            long dbId = GlobalStateMgr.getCurrentState().getDb("db1").getId();
            Set<Long> dbIds = ImmutableSet.of(dbId);
            TWorkGroup wg = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroup(
                    starRocksAssert.getCtx(),
                    ResourceGroupClassifier.QueryType.SELECT,
                    dbIds);
            Assert.assertEquals("rg5", wg.getName());
        }
        {
            long dbId = GlobalStateMgr.getCurrentState().getDb("db2").getId();
            Set<Long> dbIds = ImmutableSet.of(dbId);
            TWorkGroup wg = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroup(
                    starRocksAssert.getCtx(),
                    ResourceGroupClassifier.QueryType.SELECT,
                    dbIds);
            Assert.assertNotNull(wg);
            Assert.assertEquals("rg1", wg.getName());
        }
        {
            Set<Long> dbIds = ImmutableSet.of(
                    GlobalStateMgr.getCurrentState().getDb("db1").getId(),
                    GlobalStateMgr.getCurrentState().getDb("db2").getId());
            TWorkGroup wg = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroup(
                    starRocksAssert.getCtx(),
                    ResourceGroupClassifier.QueryType.SELECT,
                    dbIds);
            Assert.assertEquals("rg1", wg.getName());
        }
        dropResourceGroups();
    }

    @Test
    public void testShowVisibleResourceGroups() throws Exception {
        createResourceGroups();
        String qualifiedUser = "rg1_user1";
        String remoteIp = "192.168.2.4";
        starRocksAssert.getCtx().setQualifiedUser(qualifiedUser);
        starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity(qualifiedUser, "%"));
        starRocksAssert.getCtx().setRemoteIP(remoteIp);
        List<List<String>> rows = GlobalStateMgr.getCurrentState().getResourceGroupMgr().showAllResourceGroups(
                starRocksAssert.getCtx(), false);
        String result = rowsToString(rows);
        String expect = "" +
                "rg5|25|80.0%|0|0|0|10|NORMAL|(weight=10.0, db='db1')\n" +
                "rg1|10|20.0%|0|0|0|11|NORMAL|(weight=4.459375, user=rg1_user1, role=rg1_role1," +
                " query_type in " +
                "(SELECT), source_ip=192.168.2.1/24)\n" +
                "rg3|32|80.0%|0|0|0|10|NORMAL|(weight=1.1, query_type in (SELECT))";
        Assert.assertEquals(result, expect);
        dropResourceGroups();
    }

    @Test
    public void testAlterResourceGroupAlterProperties() throws Exception {
        createResourceGroups();
        String alterRg1Sql = "" +
                "ALTER resource group rg1 \n" +
                "WITH (\n" +
                "   'cpu_core_limit'='21'\n" +
                ")";

        String alterRg2Sql = "" +
                "ALTER resource group rg2 \n" +
                "WITH (\n" +
                "   'mem_limit'='37%'\n" +
                ")";

        String alterRg3Sql = "" +
                "ALTER resource group rg3 \n" +
                "WITH (\n" +
                "   'concurrency_limit'='23'\n" +
                ")";

        String alterRg4Sql = "" +
                "ALTER resource group rg4 \n" +
                "WITH (\n" +
                "   'mem_limit'='41%',\n" +
                "   'concurrency_limit'='23',\n" +
                "   'big_query_mem_limit'='1024',\n" +
                "   'big_query_scan_rows_limit'='1024',\n" +
                "   'big_query_cpu_second_limit'='1024',\n" +
                "   'cpu_core_limit'='13'\n" +
                ")";
        String[] sqls = new String[] {alterRg1Sql, alterRg2Sql, alterRg2Sql, alterRg3Sql, alterRg4Sql};
        for (String sql : sqls) {
            starRocksAssert.executeResourceGroupDdlSql(sql);
        }
        List<List<String>> rows = starRocksAssert.executeResourceGroupShowSql("SHOW RESOURCE GROUPS all");
        String result = rowsToString(rows);
        String expect = "" +
                "rg1|21|20.0%|0|0|0|11|NORMAL|(weight=4.459375, user=rg1_user1, role=rg1_role1," +
                " query_type in " +
                "(SELECT), source_ip=192.168.2.1/24)\n" +
                "rg1|21|20.0%|0|0|0|11|NORMAL|(weight=3.459375, user=rg1_user2, query_type in" +
                " (SELECT), " +
                "source_ip=192.168.3.1/24)\n" +
                "rg1|21|20.0%|0|0|0|11|NORMAL|(weight=2.359375, user=rg1_user3, source_ip=192.168.4.1/24)\n" +
                "rg1|21|20.0%|0|0|0|11|NORMAL|(weight=1.0, user=rg1_user4)\n" +
                "rg2|30|37.0%|0|0|0|20|NORMAL|(weight=3.459375, role=rg2_role1, query_type" +
                " in (SELECT), " +
                "source_ip=192.168.5.1/24)\n" +
                "rg2|30|37.0%|0|0|0|20|NORMAL|(weight=2.359375, role=rg2_role2, source_ip=192.168.6.1/24)\n" +
                "rg2|30|37.0%|0|0|0|20|NORMAL|(weight=1.0, role=rg2_role3)\n" +
                "rg3|32|80.0%|0|0|0|23|NORMAL|(weight=2.459375, query_type in (SELECT), source_ip=192.168.6.1/24)\n" +
                "rg3|32|80.0%|0|0|0|23|NORMAL|(weight=1.1, query_type in (SELECT))\n" +
                "rg4|13|41.0%|1024|1024|1024|23|NORMAL|(weight=1.359375, source_ip=192.168.7.1/24)\n" +
                "rg5|25|80.0%|0|0|0|10|NORMAL|(weight=10.0, db='db1')\n" +
                "rg6|32|80.0%|0|0|0|10|NORMAL|(weight=2.459375, query_type in (INSERT), source_ip=192.168.6.1/24)\n" +
                "rt_rg1|25|80.0%|0|0|0|10|SHORT_QUERY|(weight=1.0, user=rt_rg_user)";
        Assert.assertEquals(result, expect);
        dropResourceGroups();
    }

    @Test
    public void testIllegalClassifier() {
        // case[0] is the DDL SQL.
        // case[1] is the expected error message.
        String[][] cases = {
                {
                        "create resource group rg1\n" +
                                "to\n" +
                                "     (`unsupported-key`='value')\n" +
                                "with (\n" +
                                "    'cpu_core_limit' = '25',\n" +
                                "    'mem_limit' = '80%',\n" +
                                "    'concurrency_limit' = '10',\n" +
                                "    'type' = 'normal'\n" +
                                ");",
                        "Unsupported classifier specifier"
                },
                {
                        "create resource group rg1\n" +
                                "to\n" +
                                "     (`unsupported-key` in ('select'))\n" +
                                "with (\n" +
                                "    'cpu_core_limit' = '25',\n" +
                                "    'mem_limit' = '80%',\n" +
                                "    'concurrency_limit' = '10',\n" +
                                "    'type' = 'normal'\n" +
                                ");",
                        "Unsupported classifier specifier"
                },
                {
                        "create resource group rg1\n" +
                                "to\n" +
                                "     (`query_type` in ('unsupported-query-type'))\n" +
                                "with (\n" +
                                "    'cpu_core_limit' = '25',\n" +
                                "    'mem_limit' = '80%',\n" +
                                "    'concurrency_limit' = '10',\n" +
                                "    'type' = 'normal'\n" +
                                ");",
                        "Unsupported query_type"
                },
        };

        for (String[] c : cases) {
            Assert.assertThrows(c[1], SemanticException.class, () -> starRocksAssert.executeResourceGroupDdlSql(c[0]));
        }
    }

    @Test
    public void testCreateShortQueryResourceGroup() throws Exception {
        String createRtRg1ReplaceSql = "create resource group if not exists or replace rg1\n" +
                "to\n" +
                "     (`db`='db1')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '25',\n" +
                "    'mem_limit' = '80%',\n" +
                "    'concurrency_limit' = '10',\n" +
                "    'type' = 'short_query'\n" +
                ");";

        String createNormalRg1ReplaceSql = "create resource group if not exists or replace rg1\n" +
                "to\n" +
                "     (`db`='db1')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '25',\n" +
                "    'mem_limit' = '80%',\n" +
                "    'concurrency_limit' = '10',\n" +
                "    'type' = 'normal'\n" +
                ");";

        String createRtRg2ReplaceSql = "create resource group if not exists or replace rg2\n" +
                "to\n" +
                "     (`db`='db1')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '25',\n" +
                "    'mem_limit' = '80%',\n" +
                "    'concurrency_limit' = '10',\n" +
                "    'type' = 'short_query'\n" +
                ");";

        String createNormalRg2ReplaceSql = "create resource group if not exists or replace rg2\n" +
                "to\n" +
                "     (`db`='db1')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '25',\n" +
                "    'mem_limit' = '80%',\n" +
                "    'concurrency_limit' = '10',\n" +
                "    'type' = 'normal'\n" +
                ");";

        String alterRg1ToNormalTypeSql = "ALTER resource group rg1\n" +
                "WITH ('type' = 'normal')";

        // Create realtime rg1.
        starRocksAssert.executeResourceGroupDdlSql(createRtRg1ReplaceSql);

        // Fail to modify type.
        Assert.assertThrows("type of ResourceGroup is immutable",
                SemanticException.class, () -> starRocksAssert.executeResourceGroupDdlSql(alterRg1ToNormalTypeSql));

        // Create normal rg2 and fail to replace it with realtime rg2.
        starRocksAssert.executeResourceGroupDdlSql(createNormalRg2ReplaceSql);
        Assert.assertThrows("There can be only one short_query RESOURCE_GROUP (rg1)",
                DdlException.class, () -> starRocksAssert.executeResourceGroupDdlSql(createRtRg2ReplaceSql));

        // Replace realtime rg1 with normal rg1, and create realtime rg2.
        starRocksAssert.executeResourceGroupDdlSql(createNormalRg1ReplaceSql);
        starRocksAssert.executeResourceGroupDdlSql(createRtRg2ReplaceSql);

        // Replace realtime rg2 with normal rg2, and create realtime rg1.
        starRocksAssert.executeResourceGroupDdlSql(createNormalRg2ReplaceSql);
        starRocksAssert.executeResourceGroupDdlSql(createRtRg1ReplaceSql);

        starRocksAssert.executeResourceGroupDdlSql("DROP RESOURCE GROUP rg1");
        starRocksAssert.executeResourceGroupDdlSql("DROP RESOURCE GROUP rg2");
    }


    @Test
    public void testChooseShortQueryResourceGroup() throws Exception {
        createResourceGroups();

        // Prefer the short query group regardless its classifier weight is the lowest.
        String qualifiedUser = "rt_rg_user";
        long dbId = GlobalStateMgr.getCurrentState().getDb("db1").getId();
        Set<Long> dbs = ImmutableSet.of(dbId);
        starRocksAssert.getCtx().setQualifiedUser(qualifiedUser);
        starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity(qualifiedUser, "%"));
        TWorkGroup wg = GlobalStateMgr.getCurrentState().getResourceGroupMgr().chooseResourceGroup(
                starRocksAssert.getCtx(),
                ResourceGroupClassifier.QueryType.SELECT,
                dbs);
        Assert.assertEquals("rt_rg1", wg.getName());

        dropResourceGroups();
    }
}
