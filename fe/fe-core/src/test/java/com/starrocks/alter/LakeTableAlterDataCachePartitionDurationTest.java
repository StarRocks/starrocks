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

package com.starrocks.alter;

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.cloudnative.DataCacheInfo;
import com.starrocks.cloudnative.LakeTable;
import com.starrocks.cloudnative.LakeTablet;
import com.starrocks.cloudnative.StarOSAgent;
import com.starrocks.common.FeConstants;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class LakeTableAlterDataCachePartitionDurationTest {
    private static final int NUM_BUCKETS = 4;
    private ConnectContext connectContext;
    private AlterJobV2 alterMetaJob;
    private Database db;
    private LakeTable table;
    private List<Long> shadowTabletIds = new ArrayList<>();

    public LakeTableAlterDataCachePartitionDurationTest() {
        connectContext = new ConnectContext(null);
        connectContext.setStartTime();
        connectContext.setThreadLocalInfo();
    }

    @Before
    public void before() throws Exception {
        FeConstants.runningUnitTest = true;
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(int shardCount, FilePathInfo path, FileCacheInfo cache, long groupId,
                                           List<Long> matchShardIds, Map<String, String> properties)
                    throws DdlException {
                for (int i = 0; i < shardCount; i++) {
                    shadowTabletIds.add(GlobalStateMgr.getCurrentState().getNextId());
                }
                return shadowTabletIds;
            }
        };

        new MockUp<EditLog>() {
            @Mock
            public void logAlterTableProperties(ModifyTablePropertyOperationLog info) {

            }

            @Mock
            public void logSaveNextId(long nextId) {

            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        final long dbId = GlobalStateMgr.getCurrentState().getNextId();
        final long partitionId = GlobalStateMgr.getCurrentState().getNextId();
        final long tableId = GlobalStateMgr.getCurrentState().getNextId();
        final long indexId = GlobalStateMgr.getCurrentState().getNextId();

        GlobalStateMgr.getCurrentState().setStarOSAgent(new StarOSAgent());

        KeysType keysType = KeysType.DUP_KEYS;
        db = new Database(dbId, "db0");

        Database oldDb = GlobalStateMgr.getCurrentState().getIdToDb().putIfAbsent(db.getId(), db);
        Assert.assertNull(oldDb);

        Column c0 = new Column("c0", Type.INT, true, AggregateType.NONE, false, null, null);
        DistributionInfo dist = new HashDistributionInfo(NUM_BUCKETS, Collections.singletonList(c0));
        PartitionInfo partitionInfo = new RangePartitionInfo(Collections.singletonList(c0));
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);

        table = new LakeTable(tableId, "t0", Collections.singletonList(c0), keysType, partitionInfo, dist);
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, "t0", index, dist);
        TStorageMedium storage = TStorageMedium.HDD;
        TabletMeta tabletMeta = new TabletMeta(db.getId(), table.getId(), partition.getId(), index.getId(), 0, storage, true);
        for (int i = 0; i < NUM_BUCKETS; i++) {
            Tablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId());
            index.addTablet(tablet, tabletMeta);
        }
        table.addPartition(partition);

        table.setIndexMeta(index.getId(), "t0", Collections.singletonList(c0), 0, 0, (short) 1, TStorageType.COLUMN, keysType);
        table.setBaseIndexId(index.getId());

        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();

        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://test-bucket/object-1");
        FilePathInfo pathInfo = builder.build();

        table.setStorageInfo(pathInfo, new DataCacheInfo(false, false));
        DataCacheInfo dataCacheInfo = new DataCacheInfo(false, false);
        partitionInfo.setDataCacheInfo(partitionId, dataCacheInfo);

        db.registerTableUnlocked(table);
    }

    @After
    public void after() throws Exception {
        db.dropTable(table.getName());
    }

    @Test
    public void testModifyDataCachePartitionDurationAbourtMonths() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "7 months");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        List<AlterClause> alterList = Collections.singletonList(modify);
        alterMetaJob = schemaChangeHandler.analyzeAndCreateJob(alterList, db, table);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        Assert.assertEquals("7 months", TimeUtils.toHumanReadableString(
                table.getTableProperty().getDataCachePartitionDuration()));

    }

    @Test
    public void testModifyDataCachePartitionDurationAbourtDays() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "2 days");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        List<AlterClause> alterList = Collections.singletonList(modify);
        alterMetaJob = schemaChangeHandler.analyzeAndCreateJob(alterList, db, table);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        Assert.assertEquals("2 days", TimeUtils.toHumanReadableString(
                table.getTableProperty().getDataCachePartitionDuration()));

    }

    @Test
    public void testModifyDataCachePartitionDurationAboutHours() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "4 hours");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        List<AlterClause> alterList = Collections.singletonList(modify);
        alterMetaJob = schemaChangeHandler.analyzeAndCreateJob(alterList, db, table);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        Assert.assertEquals("4 hours", TimeUtils.toHumanReadableString(
                table.getTableProperty().getDataCachePartitionDuration()));
    }
}
