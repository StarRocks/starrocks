// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake.delete;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.proto.DeleteDataRequest;
import com.starrocks.lake.proto.DeleteDataResponse;
import com.starrocks.load.DeleteHandler;
import com.starrocks.load.DeleteJob;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.QueryStateException;
import com.starrocks.rpc.LakeServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import mockit.Expectations;
import mockit.Mocked;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DeleteTest {
    private long dbId = 1L;
    private long tableId = 2L;
    private long partitionId = 3L;
    private long indexId = 4L;
    private long tablet1Id = 10L;
    private long tablet2Id = 11L;
    private long backendId = 20L;
    private String dbName = "db1";
    private String tableName = "t1";
    private String partitionName = "p1";

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private GlobalTransactionMgr globalTransactionMgr;
    @Mocked
    private EditLog editLog;
    @Mocked
    private SystemInfoService systemInfoService;
    @Mocked
    private LakeServiceClient client;

    private Database db;
    private Auth auth;
    private Analyzer analyzer;
    private DeleteHandler deleteHandler;

    private Database createDb() {
        // Schema
        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet
        Tablet tablet1 = new LakeTablet(tablet1Id);
        Tablet tablet2 = new LakeTablet(tablet2Id);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD, true);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        Partition partition = new Partition(partitionId, partitionName, index, distributionInfo);

        // Lake table
        LakeTable table = new LakeTable(tableId, tableName, columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        Database db = new Database(dbId, dbName);
        db.createTable(table);
        return db;
    }

    @Before
    public void setUp() {
        deleteHandler = new DeleteHandler();
        auth = AccessTestUtil.fetchAdminAccess();
        analyzer = AccessTestUtil.fetchAdminAnalyzer();
        db = createDb();
        Backend backend = new Backend(backendId, "127.0.0.1", 1234);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getDb(anyString);
                result = db;

                GlobalStateMgr.getCurrentGlobalTransactionMgr();
                result = globalTransactionMgr;

                GlobalStateMgr.getCurrentSystemInfo();
                result = systemInfoService;

                systemInfoService.getBackend(anyLong);
                result = backend;
            }
        };
    }

    @Test
    public void testNormal() throws UserException, RpcException {
        TransactionState transactionState = new TransactionState();
        transactionState.setTransactionStatus(TransactionStatus.VISIBLE);

        new Expectations() {
            {
                client.deleteData((DeleteDataRequest) any);
                result = new Future<DeleteDataResponse>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return false;
                    }

                    @Override
                    public DeleteDataResponse get() throws InterruptedException, ExecutionException {
                        return null;
                    }

                    @Override
                    public DeleteDataResponse get(long timeout, @NotNull TimeUnit unit)
                            throws InterruptedException, ExecutionException, TimeoutException {
                        return null;
                    }
                };

                globalTransactionMgr.commitAndPublishTransaction(db, anyLong, (List) any, anyLong);
                result = true;

                globalTransactionMgr.getTransactionState(anyLong, anyLong);
                result = transactionState;
            }
        };

        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName(dbName, tableName),
                new PartitionNames(false, Lists.newArrayList(partitionName)), binaryPredicate);

        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }

        try {
            deleteHandler.process(deleteStmt);
        } catch (QueryStateException e) {
        }

        Map<Long, DeleteJob> idToDeleteJob = Deencapsulation.getField(deleteHandler, "idToDeleteJob");
        Collection<DeleteJob> jobs = idToDeleteJob.values();
        Assert.assertEquals(0, jobs.size());
    }

    @Test(expected = DdlException.class)
    public void testBeDeleteFail() throws RpcException, DdlException, QueryStateException {
        new Expectations() {
            {
                client.deleteData((DeleteDataRequest) any);
                result = new Future<DeleteDataResponse>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return false;
                    }

                    @Override
                    public DeleteDataResponse get() throws InterruptedException, ExecutionException {
                        DeleteDataResponse response = new DeleteDataResponse();
                        response.failedTablets = Lists.newArrayList(tablet1Id);
                        return response;
                    }

                    @Override
                    public DeleteDataResponse get(long timeout, @NotNull TimeUnit unit)
                            throws InterruptedException, ExecutionException, TimeoutException {
                        return null;
                    }
                };
            }
        };

        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, new SlotRef(null, "k1"),
                new IntLiteral(3));

        DeleteStmt deleteStmt = new DeleteStmt(new TableName(dbName, tableName),
                new PartitionNames(false, Lists.newArrayList(partitionName)), binaryPredicate);

        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }

        deleteHandler.process(deleteStmt);
    }
}
