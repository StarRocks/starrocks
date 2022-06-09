package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Writable;
import com.starrocks.journal.JournalEntity;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PartitionPersistInfoV2Test {

    private final Long dbId = 1000L;
    private final Long tableId = 1001L;
    private final Partition partition = new Partition(20000L, "p1",
            new MaterializedIndex(1, MaterializedIndex.IndexState.NORMAL),
            new RandomDistributionInfo(10));
    private final DataProperty dataProperty = new DataProperty(TStorageMedium.SSD, 1654128000000L);
    private final short replicationNum = 1;
    private final boolean isInMemory = false;
    private final boolean isTempPartition = false;

    private ListPartitionPersistInfo listPartitionPersistInfo;
    private RangePartitionPersistInfo rangePartitionPersistInfo;

    @Before
    public void setUp() throws AnalysisException, IOException {

        List<String> values = Lists.newArrayList("guangdong", "shanghai");
        List<List<String>> multiValues = new ArrayList<>();
        this.listPartitionPersistInfo = new ListPartitionPersistInfo(dbId, tableId, partition,
                dataProperty, replicationNum, isInMemory,
                isTempPartition, values, multiValues);

        PartitionKey lower = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("-1")),
                Arrays.asList(new Column("tinyint", Type.TINYINT)));
        PartitionKey upper = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("1")),
                Arrays.asList(new Column("tinyint", Type.TINYINT)));
        Range<PartitionKey> range = Range.closedOpen(lower, upper);
        this.rangePartitionPersistInfo = new RangePartitionPersistInfo(dbId, tableId, partition,
                dataProperty, replicationNum, isInMemory,
                isTempPartition, range);
    }

    @Test
    public void testListPartitionPersistInfo() {
        Assert.assertEquals(dbId, listPartitionPersistInfo.getDbId());
        Assert.assertEquals(tableId, listPartitionPersistInfo.getTableId());
        Assert.assertEquals("p1", listPartitionPersistInfo.getPartition().getName());
        Assert.assertEquals(TStorageMedium.SSD, listPartitionPersistInfo.getDataProperty().getStorageMedium());
        Assert.assertEquals(replicationNum, listPartitionPersistInfo.getReplicationNum());
        Assert.assertEquals(isInMemory, listPartitionPersistInfo.isInMemory());
        Assert.assertEquals(isTempPartition, listPartitionPersistInfo.isTempPartition());

        Assert.assertEquals(true, listPartitionPersistInfo.isListPartitionPersistInfo());
        Assert.assertEquals(ListPartitionPersistInfo.class,
                listPartitionPersistInfo.asListPartitionPersistInfo().getClass());
        Assert.assertEquals(false, listPartitionPersistInfo.isRangePartitionPersistInfo());

        Assert.assertEquals(2, listPartitionPersistInfo.getValues().size());
        Assert.assertEquals(0, listPartitionPersistInfo.getMultiValues().size());
    }

    @Test
    public void testRangePartitionPersistInfo() throws AnalysisException {
        Assert.assertEquals(dbId, rangePartitionPersistInfo.getDbId());
        Assert.assertEquals(tableId, rangePartitionPersistInfo.getTableId());
        Assert.assertEquals("p1", rangePartitionPersistInfo.getPartition().getName());
        Assert.assertEquals(TStorageMedium.SSD, rangePartitionPersistInfo.getDataProperty().getStorageMedium());
        Assert.assertEquals(replicationNum, rangePartitionPersistInfo.getReplicationNum());
        Assert.assertEquals(isInMemory, rangePartitionPersistInfo.isInMemory());
        Assert.assertEquals(isTempPartition, rangePartitionPersistInfo.isTempPartition());

        Assert.assertEquals(false, rangePartitionPersistInfo.isListPartitionPersistInfo());
        Assert.assertEquals(true, rangePartitionPersistInfo.isRangePartitionPersistInfo());
        Assert.assertEquals(RangePartitionPersistInfo.class,
                rangePartitionPersistInfo.asRangePartitionPersistInfo().getClass());
    }

    @Test
    public void testWriteOutAndReadInForList() throws IOException {
        // Write objects to file
        File file = new File("./test_serial.log");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        this.listPartitionPersistInfo.write(out);
        out.flush();
        out.close();

        // Read object from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        PartitionPersistInfoV2 partitionPersistInfoV2 = PartitionPersistInfoV2.read(in);

        Assert.assertEquals(partitionPersistInfoV2.getDbId(),
                listPartitionPersistInfo.getDbId());
        Assert.assertEquals(partitionPersistInfoV2.getTableId(),
                listPartitionPersistInfo.getTableId());
        Assert.assertEquals(partitionPersistInfoV2.getPartition().getName(),
                listPartitionPersistInfo.getPartition().getName());
        Assert.assertEquals(partitionPersistInfoV2.getDataProperty().getStorageMedium(),
                listPartitionPersistInfo.getDataProperty().getStorageMedium());
        Assert.assertEquals(partitionPersistInfoV2.getReplicationNum(),
                listPartitionPersistInfo.getReplicationNum());
        Assert.assertEquals(partitionPersistInfoV2.isInMemory(),
                listPartitionPersistInfo.isInMemory());
        Assert.assertEquals(partitionPersistInfoV2.isTempPartition(),
                listPartitionPersistInfo.isTempPartition());

        Assert.assertEquals(partitionPersistInfoV2.isListPartitionPersistInfo(),
                listPartitionPersistInfo.isListPartitionPersistInfo());
        Assert.assertEquals(partitionPersistInfoV2.asListPartitionPersistInfo().getClass(),
                listPartitionPersistInfo.asListPartitionPersistInfo().getClass());
        Assert.assertEquals(partitionPersistInfoV2.isRangePartitionPersistInfo(),
                listPartitionPersistInfo.isRangePartitionPersistInfo());

        Assert.assertEquals(partitionPersistInfoV2.asListPartitionPersistInfo().getValues().size(),
                listPartitionPersistInfo.getValues().size());
        Assert.assertEquals(partitionPersistInfoV2.asListPartitionPersistInfo().getMultiValues().size(),
                listPartitionPersistInfo.getMultiValues().size());

        file.delete();
    }

    @Test
    public void testWriteOutAndReadInForRange() throws IOException {
        // Write objects to file
        File file = new File("./test_serial.log");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        this.rangePartitionPersistInfo.write(out);
        out.flush();
        out.close();

        // Read object from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        PartitionPersistInfoV2 partitionPersistInfoV2 = PartitionPersistInfoV2.read(in);

        Assert.assertEquals(partitionPersistInfoV2.getDbId(),
                rangePartitionPersistInfo.getDbId());
        Assert.assertEquals(partitionPersistInfoV2.getTableId(),
                rangePartitionPersistInfo.getTableId());
        Assert.assertEquals(partitionPersistInfoV2.getPartition().getName(),
                rangePartitionPersistInfo.getPartition().getName());
        Assert.assertEquals(partitionPersistInfoV2.getDataProperty().getStorageMedium(),
                rangePartitionPersistInfo.getDataProperty().getStorageMedium());
        Assert.assertEquals(partitionPersistInfoV2.getReplicationNum(),
                rangePartitionPersistInfo.getReplicationNum());
        Assert.assertEquals(partitionPersistInfoV2.isInMemory(),
                rangePartitionPersistInfo.isInMemory());
        Assert.assertEquals(partitionPersistInfoV2.isTempPartition(),
                rangePartitionPersistInfo.isTempPartition());

        Assert.assertEquals(partitionPersistInfoV2.isListPartitionPersistInfo(),
                rangePartitionPersistInfo.isListPartitionPersistInfo());
        Assert.assertEquals(partitionPersistInfoV2.asRangePartitionPersistInfo().getClass(),
                rangePartitionPersistInfo.asRangePartitionPersistInfo().getClass());
        Assert.assertEquals(partitionPersistInfoV2.isRangePartitionPersistInfo(),
                rangePartitionPersistInfo.isRangePartitionPersistInfo());

        Assert.assertEquals(partitionPersistInfoV2.asRangePartitionPersistInfo().getRange().upperEndpoint(),
                rangePartitionPersistInfo.asRangePartitionPersistInfo().getRange().upperEndpoint());
        Assert.assertEquals(partitionPersistInfoV2.asRangePartitionPersistInfo().getRange().lowerEndpoint(),
                rangePartitionPersistInfo.asRangePartitionPersistInfo().getRange().lowerEndpoint());

        file.delete();
    }

    @Test
    public void testJournalEntityForPartitionPersistInfoV2() throws IOException {
        JournalEntity entity = new JournalEntity();
        entity.setOpCode(OperationType.OP_ADD_PARTITION_V2);
        entity.setData(listPartitionPersistInfo);

        // Write objects to file
        File file = new File("./test_serial.log");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        entity.write(out);
        out.flush();
        out.close();

        // Read object from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        entity.readFields(in);
        Writable writable = entity.getData();

        PartitionPersistInfoV2 partitionPersistInfoV2 = (PartitionPersistInfoV2) writable;

        Assert.assertEquals(partitionPersistInfoV2.getDbId(),
                listPartitionPersistInfo.getDbId());
        Assert.assertEquals(partitionPersistInfoV2.getTableId(),
                listPartitionPersistInfo.getTableId());
        Assert.assertEquals(partitionPersistInfoV2.getPartition().getName(),
                listPartitionPersistInfo.getPartition().getName());
        Assert.assertEquals(partitionPersistInfoV2.getDataProperty().getStorageMedium(),
                listPartitionPersistInfo.getDataProperty().getStorageMedium());
        Assert.assertEquals(partitionPersistInfoV2.getReplicationNum(),
                listPartitionPersistInfo.getReplicationNum());
        Assert.assertEquals(partitionPersistInfoV2.isInMemory(),
                listPartitionPersistInfo.isInMemory());
        Assert.assertEquals(partitionPersistInfoV2.isTempPartition(),
                listPartitionPersistInfo.isTempPartition());

        Assert.assertEquals(partitionPersistInfoV2.isListPartitionPersistInfo(),
                listPartitionPersistInfo.isListPartitionPersistInfo());
        Assert.assertEquals(partitionPersistInfoV2.asListPartitionPersistInfo().getClass(),
                listPartitionPersistInfo.asListPartitionPersistInfo().getClass());
        Assert.assertEquals(partitionPersistInfoV2.isRangePartitionPersistInfo(),
                listPartitionPersistInfo.isRangePartitionPersistInfo());

        Assert.assertEquals(partitionPersistInfoV2.asListPartitionPersistInfo().getValues().size(),
                listPartitionPersistInfo.getValues().size());
        Assert.assertEquals(partitionPersistInfoV2.asListPartitionPersistInfo().getMultiValues().size(),
                listPartitionPersistInfo.getMultiValues().size());

        file.delete();
    }

    @Test
    public void testEditLogForPartitionPersistInfoV2(@Mocked GlobalStateMgr globalStateMgr) {
        ListPartitionInfo partitionInfo = new ListPartitionInfo(PartitionType.LIST,
                Lists.newArrayList(new Column("province", Type.VARCHAR)));
        OlapTable table = new OlapTable();
        new Expectations(table) {{
            table.getPartitionInfo();
            result = partitionInfo;
            minTimes = 0;
        }};

        Database database = new Database();
        new Expectations(database) {{
            database.getTable(tableId);
            result = table;
            minTimes = 0;
        }};

        new Expectations(globalStateMgr) {{
            globalStateMgr.getDb(dbId);
            result = database;
            minTimes = 0;
        }};

        /*JournalEntity entity = new JournalEntity();
        entity.setOpCode(OperationType.OP_ADD_PARTITION_V2);
        entity.setData(listPartitionPersistInfo);
        EditLog.loadJournal(globalStateMgr, entity);

        long partitionId = partition.getId();
        Assert.assertEquals(partitionInfo.getDataProperty(partitionId).getStorageMedium(),
                listPartitionPersistInfo.getDataProperty().getStorageMedium());
        Assert.assertEquals(partitionInfo.getReplicationNum(partitionId),
                listPartitionPersistInfo.getReplicationNum());
        Assert.assertEquals(partitionInfo.getIsInMemory(partitionId),
                listPartitionPersistInfo.isInMemory());

        Assert.assertEquals(partitionInfo.getIdToValues().get(partitionId).size(),
                listPartitionPersistInfo.getValues().size());
        Assert.assertEquals(partitionInfo.getIdToMultiValues().get(partitionId).size(),
                listPartitionPersistInfo.getMultiValues().size());*/

    }

}
