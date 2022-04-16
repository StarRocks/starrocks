package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.catalog.TableProperty;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzerAlterTableTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAddColumn() {
        analyzeSuccess("alter table test add column testCol int");
    }

    @Test
    public void testAddColumns() {
        analyzeSuccess("alter table test add column col1 INT KEY DEFAULT \"0\" AFTER col1 TO example_rollup_index," +
                " add column col2 int key ");
    }

    @Test
    public void testRenameTable() {
        analyzeSuccess("alter table test rename test1");
    }

    @Test
    public void testDropColumn() {
        AlterTableStmt stmt =
                (AlterTableStmt) analyzeSuccess("alter table test1 drop column col1 , drop column col2");
        Assert.assertEquals("ALTER TABLE `default_cluster:test`.`test1` DROP COLUMN `col1`, \nDROP COLUMN `col2`",
                stmt.toSql());
        Assert.assertEquals("default_cluster:test", stmt.getTbl().getDb());
        Assert.assertEquals(2, stmt.getOps().size());
    }

    @Test
    public void testModifyTableProperties() {
        analyzeSuccess("alter table test set (\"default.replication_num\" = \"2\")");
        analyzeFail("alter table test set (\"" + TableProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX +
                "_default.replication_num\" = \"2\")");
    }

    @Test
    public void testPartitionRename() {
        analyzeSuccess("alter table test rename partition p1 p2");
        analyzeFail("alter table test rename partition p1");
    }

    @Test
    public void testReorderColumns() {
        analyzeSuccess("alter table test order by (col2, col1) from index1");
        analyzeFail("alter table test order by from index1");
    }

    @Test
    public void testReplacePartition() {
        analyzeSuccess("alter table test replace partition (p1, p2, p3) with temporary partition(tp1, tp2)");
        analyzeFail("alter table test replace partition (p1, p2, p3) with temporary partition()");
    }

    @Test
    public void testRollupRename() {
        analyzeSuccess("alter table test rename rollup rollup1 rollup2");
        analyzeFail("alter table test rename rollup rollup1");
    }

    @Test
    public void testSwapTable() {
        analyzeSuccess("alter table test swap with test2");
        analyzeFail("alter table test swap with");
    }

    @Test
    public void testTableRename() {
        analyzeSuccess("alter table test rename test1 ");
        analyzeFail("alter table test rename ");
    }

    @Test
    public void TestAddRollup() {
        AlterTableStmt stmt =
                (AlterTableStmt) analyzeSuccess(
                        "alter table test1 add rollup index1 (col1, col2) from test1, index2 (col3, col4) from test1");
        Assert.assertEquals("ALTER TABLE `default_cluster:test`.`test1` ADD ROLLUP `index1` (`col1`, `col2`) FROM `test1`, " +
                        "\n `index2` (`col3`, `col4`) FROM `test1`",
                stmt.toSql());
        Assert.assertEquals("default_cluster:test", stmt.getTbl().getDb());
        Assert.assertEquals(2, stmt.getOps().size());
    }

    @Test(expected = AssertionError.class)
    public void testNoClause() {
        analyzeFail("alter table test1");
    }
}