// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ExportStmtTest {
    private String path;
    private BrokerDesc brokerDesc;
    @Mocked
    private TableRef tableRef;
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private Auth auth;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private BrokerMgr brokerMgr;
    @Mocked
    private Database db;
    @Mocked
    private Table table;

    @Before
    public void setUp() throws AnalysisException {
        path = "hdfs://127.0.0.1:9002/export/";
        brokerDesc = new BrokerDesc("broker", null);
        FsBroker fsBroker = new FsBroker("127.0.0.1", 99999);

        String dbName = "db1";
        String tableName = "tbl1";
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", ScalarType.createCharType(10), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.INT, true, null, "", ""));

        new MockUp<StatementBase>() {
            @Mock
            public void analyze(Analyzer analyzer) {
                return;
            }
        };

        new Expectations() {
            {
                tableRef.getName();
                result = new TableName(dbName, tableName);
                tableRef.getPartitionNames();
                result = null;
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getAuth();
                result = auth;
                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                result = true;
                analyzer.getCatalog();
                result = globalStateMgr;
                globalStateMgr.getDb(dbName);
                result = db;
                db.getTable(tableName);
                result = table;
                table.getBaseSchema();
                result = columns;

                globalStateMgr.getBrokerMgr();
                minTimes = 0;
                result = brokerMgr;
                brokerMgr.containsBroker(brokerDesc.getName());
                minTimes = 0;
                result = true;
                brokerMgr.getAnyBroker(brokerDesc.getName());
                minTimes = 0;
                result = fsBroker;
                ConnectContext.get();
                minTimes = 0;
                result = connectContext;
                connectContext.getSessionVariable();
                minTimes = 0;
                result = new SessionVariable();
            }
        };
    }

    @Test
    public void testExportColumns() throws UserException {
        List<String> columnNames = Lists.newArrayList("K1", "k2");
        ExportStmt stmt = new ExportStmt(tableRef, columnNames, path, Maps.newHashMap(), brokerDesc);
        stmt.analyze(analyzer);
        Assert.assertEquals(2, stmt.getColumnNames().size());
        Assert.assertEquals(columnNames, stmt.getColumnNames());
    }

    @Test(expected = AnalysisException.class)
    public void testExportDuplicatedColumn() throws UserException {
        List<String> columnNames = Lists.newArrayList("k1", "k1");
        ExportStmt stmt = new ExportStmt(tableRef, columnNames, path, Maps.newHashMap(), brokerDesc);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testExportNotExistColumn() throws UserException {
        List<String> columnNames = Lists.newArrayList("k1", "k_not_exist");
        ExportStmt stmt = new ExportStmt(tableRef, columnNames, path, Maps.newHashMap(), brokerDesc);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}