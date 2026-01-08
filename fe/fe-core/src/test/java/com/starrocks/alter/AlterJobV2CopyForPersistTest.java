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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.common.SchemaVersionAndHash;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class AlterJobV2CopyForPersistTest {
    @Test
    public void testSchemaChangeJobV2CopyForPersist() throws Exception {
        SchemaChangeJobV2 job = newSchemaChangeJobV2();
        SchemaChangeJobV2 copy = (SchemaChangeJobV2) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "physicalPartitionIndexTabletMap",
                "physicalPartitionIndexMap",
                "indexIdMap",
                "indexIdToName",
                "indexSchemaMap",
                "indexSchemaVersionAndHashMap",
                "indexShortKeyMap",
                "bfColumns",
                "indexes",
                "sortKeyIdxes",
                "sortKeyUniqueIds");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newSchemaChangeJobV2,
                "physicalPartitionIndexTabletMap",
                "physicalPartitionIndexMap",
                "indexIdMap",
                "indexIdToName",
                "indexSchemaMap",
                "indexSchemaVersionAndHashMap",
                "indexShortKeyMap",
                "bfColumns",
                "indexes",
                "sortKeyIdxes",
                "sortKeyUniqueIds");
        assertTableValueMapIndependent(AlterJobV2CopyForPersistTest::newSchemaChangeJobV2,
                "physicalPartitionIndexTabletMap", 1L, 10L);
        assertMapValueListIndependent(AlterJobV2CopyForPersistTest::newSchemaChangeJobV2,
                "indexSchemaMap", 10L);
    }

    @Test
    public void testLakeTableSchemaChangeJobCopyForPersist() throws Exception {
        LakeTableSchemaChangeJob job = newLakeTableSchemaChangeJob();
        LakeTableSchemaChangeJob copy = (LakeTableSchemaChangeJob) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "physicalPartitionIndexTabletMap",
                "physicalPartitionIndexMap",
                "indexIdMap",
                "indexIdToName",
                "indexSchemaMap",
                "indexShortKeyMap",
                "bfColumns",
                "indexes",
                "commitVersionMap",
                "sortKeyIdxes",
                "sortKeyUniqueIds");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newLakeTableSchemaChangeJob,
                "physicalPartitionIndexTabletMap",
                "physicalPartitionIndexMap",
                "indexIdMap",
                "indexIdToName",
                "indexSchemaMap",
                "indexShortKeyMap",
                "bfColumns",
                "indexes",
                "commitVersionMap",
                "sortKeyIdxes",
                "sortKeyUniqueIds");
        assertTableValueMapIndependent(AlterJobV2CopyForPersistTest::newLakeTableSchemaChangeJob,
                "physicalPartitionIndexTabletMap", 1L, 10L);
        assertMapValueListIndependent(AlterJobV2CopyForPersistTest::newLakeTableSchemaChangeJob,
                "indexSchemaMap", 10L);
    }

    @Test
    public void testRollupJobV2CopyForPersist() throws Exception {
        RollupJobV2 job = newRollupJobV2();
        RollupJobV2 copy = (RollupJobV2) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "physicalPartitionIdToBaseRollupTabletIdMap",
                "physicalPartitionIdToRollupIndex",
                "rollupSchema");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newRollupJobV2,
                "physicalPartitionIdToBaseRollupTabletIdMap",
                "physicalPartitionIdToRollupIndex",
                "rollupSchema");
        assertMapValueMapIndependent(AlterJobV2CopyForPersistTest::newRollupJobV2,
                "physicalPartitionIdToBaseRollupTabletIdMap", 1L);
    }

    @Test
    public void testLakeRollupJobCopyForPersist() throws Exception {
        LakeRollupJob job = newLakeRollupJob();
        LakeRollupJob copy = (LakeRollupJob) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "commitVersionMap",
                "physicalPartitionIdToBaseRollupTabletIdMap",
                "physicalPartitionIdToRollupIndex",
                "rollupSchema");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newLakeRollupJob,
                "commitVersionMap",
                "physicalPartitionIdToBaseRollupTabletIdMap",
                "physicalPartitionIdToRollupIndex",
                "rollupSchema");
        assertMapValueMapIndependent(AlterJobV2CopyForPersistTest::newLakeRollupJob,
                "physicalPartitionIdToBaseRollupTabletIdMap", 1L);
    }

    @Test
    public void testOptimizeJobV2CopyForPersist() throws Exception {
        OptimizeJobV2 job = newOptimizeJobV2();
        OptimizeJobV2 copy = (OptimizeJobV2) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "tmpPartitionIds",
                "rewriteTasks",
                "sourcePartitionNames",
                "tmpPartitionNames");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newOptimizeJobV2,
                "tmpPartitionIds",
                "rewriteTasks",
                "sourcePartitionNames",
                "tmpPartitionNames");
    }

    @Test
    public void testOnlineOptimizeJobV2CopyForPersist() throws Exception {
        OnlineOptimizeJobV2 job = newOnlineOptimizeJobV2();
        OnlineOptimizeJobV2 copy = (OnlineOptimizeJobV2) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "tmpPartitionIds",
                "rewriteTasks",
                "sourcePartitionNames",
                "tmpPartitionNames");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newOnlineOptimizeJobV2,
                "tmpPartitionIds",
                "rewriteTasks",
                "sourcePartitionNames",
                "tmpPartitionNames");
    }

    @Test
    public void testMergePartitionJobCopyForPersist() throws Exception {
        MergePartitionJob job = newMergePartitionJob();
        MergePartitionJob copy = (MergePartitionJob) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "tempPartitionIdToSourcePartitionIds",
                "tempPartitionNameToSourcePartitionNames",
                "rewriteTasks");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newMergePartitionJob,
                "tempPartitionIdToSourcePartitionIds",
                "tempPartitionNameToSourcePartitionNames",
                "rewriteTasks");
    }

    @Test
    public void testLakeTableAsyncFastSchemaChangeJobCopyForPersist() throws Exception {
        LakeTableAsyncFastSchemaChangeJob job = newLakeTableAsyncFastSchemaChangeJob();
        LakeTableAsyncFastSchemaChangeJob copy = (LakeTableAsyncFastSchemaChangeJob) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "schemaInfos",
                "physicalPartitionIndexMap",
                "commitVersionMap");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newLakeTableAsyncFastSchemaChangeJob,
                "schemaInfos",
                "physicalPartitionIndexMap",
                "commitVersionMap");
    }

    @Test
    public void testLakeTableAlterMetaJobCopyForPersist() throws Exception {
        LakeTableAlterMetaJob job = newLakeTableAlterMetaJob();
        LakeTableAlterMetaJob copy = (LakeTableAlterMetaJob) job.copyForPersist();
        assertCollectionsEqual(job, copy,
                "physicalPartitionIndexMap",
                "commitVersionMap");
        assertCollectionsIndependent(AlterJobV2CopyForPersistTest::newLakeTableAlterMetaJob,
                "physicalPartitionIndexMap",
                "commitVersionMap");
    }

    private static SchemaChangeJobV2 newSchemaChangeJobV2() {
        SchemaChangeJobV2 job = new SchemaChangeJobV2(1L, 2L, 3L, "tbl", 1000L);
        populateSchemaChangeJobFields(job, true, false);
        return job;
    }

    private static LakeTableSchemaChangeJob newLakeTableSchemaChangeJob() {
        LakeTableSchemaChangeJob job = new LakeTableSchemaChangeJob();
        populateSchemaChangeJobFields(job, false, true);
        return job;
    }

    private static RollupJobV2 newRollupJobV2() {
        RollupJobV2 job = new RollupJobV2();
        populateRollupJobFields(job, false);
        return job;
    }

    private static LakeRollupJob newLakeRollupJob() {
        LakeRollupJob job = new LakeRollupJob();
        populateRollupJobFields(job, true);
        return job;
    }

    private static OptimizeJobV2 newOptimizeJobV2() {
        OptimizeJobV2 job = new OptimizeJobV2();
        populateOptimizeJobFields(job);
        return job;
    }

    private static OnlineOptimizeJobV2 newOnlineOptimizeJobV2() {
        OnlineOptimizeJobV2 job = new OnlineOptimizeJobV2();
        populateOptimizeJobFields(job);
        return job;
    }

    private static MergePartitionJob newMergePartitionJob() {
        MergePartitionJob job = new MergePartitionJob();
        populateMergePartitionJobFields(job);
        return job;
    }

    private static LakeTableAsyncFastSchemaChangeJob newLakeTableAsyncFastSchemaChangeJob() {
        LakeTableAsyncFastSchemaChangeJob job = new LakeTableAsyncFastSchemaChangeJob();
        populateAlterMetaBaseFields(job);
        List<IndexSchemaInfo> schemaInfos = Lists.newArrayList(
                new IndexSchemaInfo(1L, "idx1", buildSchemaInfo()));
        setField(job, "schemaInfos", schemaInfos);
        return job;
    }

    private static LakeTableAlterMetaJob newLakeTableAlterMetaJob() {
        LakeTableAlterMetaJob job = new LakeTableAlterMetaJob();
        populateAlterMetaBaseFields(job);
        return job;
    }

    private static void populateSchemaChangeJobFields(Object job, boolean includeSchemaVersionMap,
                                                      boolean includeCommitVersionMap) {
        Table<Long, Long, Map<Long, Long>> partitionIndexTabletMap = HashBasedTable.create();
        Map<Long, Long> tabletMap = Maps.newHashMap();
        tabletMap.put(100L, 200L);
        partitionIndexTabletMap.put(1L, 10L, tabletMap);
        setField(job, "physicalPartitionIndexTabletMap", partitionIndexTabletMap);

        Table<Long, Long, MaterializedIndex> partitionIndexMap = HashBasedTable.create();
        partitionIndexMap.put(1L, 10L, new MaterializedIndex(10L));
        setField(job, "physicalPartitionIndexMap", partitionIndexMap);

        Map<Long, Long> indexIdMap = Maps.newHashMap();
        indexIdMap.put(10L, 20L);
        setField(job, "indexIdMap", indexIdMap);

        Map<Long, String> indexIdToName = Maps.newHashMap();
        indexIdToName.put(10L, "idx");
        setField(job, "indexIdToName", indexIdToName);

        Map<Long, List<Column>> indexSchemaMap = Maps.newHashMap();
        indexSchemaMap.put(10L, Lists.newArrayList(new Column("c1", IntegerType.INT)));
        setField(job, "indexSchemaMap", indexSchemaMap);

        Map<Long, Short> indexShortKeyMap = Maps.newHashMap();
        indexShortKeyMap.put(10L, (short) 1);
        setField(job, "indexShortKeyMap", indexShortKeyMap);

        Set<ColumnId> bfColumns = Sets.newHashSet(ColumnId.create("c1"));
        setField(job, "bfColumns", bfColumns);

        Index index = new Index("idx", Lists.newArrayList(ColumnId.create("c1")), IndexDef.IndexType.BITMAP, "");
        setField(job, "indexes", Lists.newArrayList(index));

        setField(job, "sortKeyIdxes", Lists.newArrayList(1));
        setField(job, "sortKeyUniqueIds", Lists.newArrayList(1001));

        if (includeSchemaVersionMap) {
            Map<Long, SchemaVersionAndHash> schemaVersionMap = Maps.newHashMap();
            schemaVersionMap.put(10L, new SchemaVersionAndHash(1, 2));
            setField(job, "indexSchemaVersionAndHashMap", schemaVersionMap);
        }

        if (includeCommitVersionMap) {
            Map<Long, Long> commitVersionMap = Maps.newHashMap();
            commitVersionMap.put(1L, 100L);
            setField(job, "commitVersionMap", commitVersionMap);
        }
    }

    private static void populateRollupJobFields(Object job, boolean includeCommitVersionMap) {
        Map<Long, Map<Long, Long>> baseRollupTabletMap = Maps.newHashMap();
        Map<Long, Long> innerMap = Maps.newHashMap();
        innerMap.put(11L, 22L);
        baseRollupTabletMap.put(1L, innerMap);
        setField(job, "physicalPartitionIdToBaseRollupTabletIdMap", baseRollupTabletMap);

        Map<Long, MaterializedIndex> rollupIndexMap = Maps.newHashMap();
        rollupIndexMap.put(1L, new MaterializedIndex(100L));
        setField(job, "physicalPartitionIdToRollupIndex", rollupIndexMap);

        setField(job, "rollupSchema", Lists.newArrayList(new Column("c1", IntegerType.INT)));

        if (includeCommitVersionMap) {
            Map<Long, Long> commitVersionMap = Maps.newHashMap();
            commitVersionMap.put(1L, 100L);
            setField(job, "commitVersionMap", commitVersionMap);
        }
    }

    private static void populateOptimizeJobFields(Object job) {
        setField(job, "tmpPartitionIds", Lists.newArrayList(1L, 2L));
        OptimizeTask task = new OptimizeTask("task1");
        setField(job, "rewriteTasks", Lists.newArrayList(task));
        setField(job, "sourcePartitionNames", Lists.newArrayList("p1"));
        setField(job, "tmpPartitionNames", Lists.newArrayList("tmp_p1"));
    }

    private static void populateMergePartitionJobFields(Object job) {
        Multimap<Long, Long> idMapping = ArrayListMultimap.create();
        idMapping.put(1L, 10L);
        setField(job, "tempPartitionIdToSourcePartitionIds", idMapping);

        Multimap<String, String> nameMapping = ArrayListMultimap.create();
        nameMapping.put("tmp_p1", "p1");
        setField(job, "tempPartitionNameToSourcePartitionNames", nameMapping);

        OptimizeTask task = new OptimizeTask("task1");
        setField(job, "rewriteTasks", Lists.newArrayList(task));
    }

    private static void populateAlterMetaBaseFields(Object job) {
        Table<Long, Long, MaterializedIndex> partitionIndexMap = HashBasedTable.create();
        partitionIndexMap.put(1L, 10L, new MaterializedIndex(10L));
        setField(job, "physicalPartitionIndexMap", partitionIndexMap);

        Map<Long, Long> commitVersionMap = Maps.newHashMap();
        commitVersionMap.put(1L, 100L);
        setField(job, "commitVersionMap", commitVersionMap);
    }

    private static SchemaInfo buildSchemaInfo() {
        return SchemaInfo.newBuilder()
                .setId(1L)
                .setVersion(1)
                .setSchemaHash(10)
                .setKeysType(KeysType.DUP_KEYS)
                .setShortKeyColumnCount((short) 1)
                .setStorageType(TStorageType.COLUMN)
                .addColumn(new Column("c1", IntegerType.INT))
                .build();
    }

    private static void assertCollectionsEqual(Object original, Object copy, String... fieldNames) throws Exception {
        for (String fieldName : fieldNames) {
            Object originalValue = getField(original, fieldName);
            Object copyValue = getField(copy, fieldName);
            Assertions.assertEquals(originalValue, copyValue, fieldName);
        }
    }

    private static void assertCollectionsIndependent(Supplier<? extends AlterJobV2> supplier, String... fieldNames)
            throws Exception {
        AlterJobV2 job = supplier.get();
        AlterJobV2 copy = job.copyForPersist();
        for (String fieldName : fieldNames) {
            Object copyValue = getField(copy, fieldName);
            clearCollection(copyValue);
            Object originalValue = getField(job, fieldName);
            Assertions.assertTrue(collectionSize(originalValue) > 0, fieldName);
        }

        AlterJobV2 job2 = supplier.get();
        AlterJobV2 copy2 = job2.copyForPersist();
        for (String fieldName : fieldNames) {
            Object originalValue = getField(job2, fieldName);
            clearCollection(originalValue);
            Object copyValue = getField(copy2, fieldName);
            Assertions.assertTrue(collectionSize(copyValue) > 0, fieldName);
        }
    }

    private static void assertTableValueMapIndependent(Supplier<? extends AlterJobV2> supplier, String fieldName,
                                                       long rowKey, long columnKey) throws Exception {
        AlterJobV2 job = supplier.get();
        AlterJobV2 copy = job.copyForPersist();
        Map<Long, Long> copyMap = getTableValueMap(copy, fieldName, rowKey, columnKey);
        Map<Long, Long> originalMap = getTableValueMap(job, fieldName, rowKey, columnKey);
        copyMap.clear();
        Assertions.assertTrue(collectionSize(originalMap) > 0, fieldName);

        AlterJobV2 job2 = supplier.get();
        AlterJobV2 copy2 = job2.copyForPersist();
        Map<Long, Long> copyMap2 = getTableValueMap(copy2, fieldName, rowKey, columnKey);
        Map<Long, Long> originalMap2 = getTableValueMap(job2, fieldName, rowKey, columnKey);
        originalMap2.clear();
        Assertions.assertTrue(collectionSize(copyMap2) > 0, fieldName);
    }

    private static void assertMapValueMapIndependent(Supplier<? extends AlterJobV2> supplier, String fieldName,
                                                     long key) throws Exception {
        AlterJobV2 job = supplier.get();
        AlterJobV2 copy = job.copyForPersist();
        Map<Long, Long> copyMap = getMapValueMap(copy, fieldName, key);
        Map<Long, Long> originalMap = getMapValueMap(job, fieldName, key);
        copyMap.clear();
        Assertions.assertTrue(collectionSize(originalMap) > 0, fieldName);

        AlterJobV2 job2 = supplier.get();
        AlterJobV2 copy2 = job2.copyForPersist();
        Map<Long, Long> copyMap2 = getMapValueMap(copy2, fieldName, key);
        Map<Long, Long> originalMap2 = getMapValueMap(job2, fieldName, key);
        originalMap2.clear();
        Assertions.assertTrue(collectionSize(copyMap2) > 0, fieldName);
    }

    private static void assertMapValueListIndependent(Supplier<? extends AlterJobV2> supplier, String fieldName,
                                                      long key) throws Exception {
        AlterJobV2 job = supplier.get();
        AlterJobV2 copy = job.copyForPersist();
        List<?> copyList = getMapValueList(copy, fieldName, key);
        List<?> originalList = getMapValueList(job, fieldName, key);
        copyList.clear();
        Assertions.assertTrue(collectionSize(originalList) > 0, fieldName);

        AlterJobV2 job2 = supplier.get();
        AlterJobV2 copy2 = job2.copyForPersist();
        List<?> copyList2 = getMapValueList(copy2, fieldName, key);
        List<?> originalList2 = getMapValueList(job2, fieldName, key);
        originalList2.clear();
        Assertions.assertTrue(collectionSize(copyList2) > 0, fieldName);
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, Long> getTableValueMap(Object job, String fieldName, long rowKey, long columnKey)
            throws Exception {
        Table<Long, Long, Map<Long, Long>> table =
                (Table<Long, Long, Map<Long, Long>>) getField(job, fieldName);
        return table.get(rowKey, columnKey);
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, Long> getMapValueMap(Object job, String fieldName, long key) throws Exception {
        Map<Long, Map<Long, Long>> map = (Map<Long, Map<Long, Long>>) getField(job, fieldName);
        return map.get(key);
    }

    @SuppressWarnings("unchecked")
    private static List<?> getMapValueList(Object job, String fieldName, long key) throws Exception {
        Map<Long, List<?>> map = (Map<Long, List<?>>) getField(job, fieldName);
        return map.get(key);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) {
        try {
            Field field = findField(target.getClass(), fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new AssertionError("Failed to set field: " + fieldName, e);
        }
    }

    private static Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ex) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static void clearCollection(Object value) {
        if (value instanceof Table) {
            ((Table<?, ?, ?>) value).clear();
        } else if (value instanceof Multimap) {
            ((Multimap<?, ?>) value).clear();
        } else if (value instanceof Map) {
            ((Map<?, ?>) value).clear();
        } else if (value instanceof List) {
            ((List<?>) value).clear();
        } else if (value instanceof Set) {
            ((Set<?>) value).clear();
        } else {
            throw new IllegalArgumentException("Unsupported collection type: " + value);
        }
    }

    private static int collectionSize(Object value) {
        if (value instanceof Table) {
            return ((Table<?, ?, ?>) value).size();
        }
        if (value instanceof Multimap) {
            return ((Multimap<?, ?>) value).size();
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).size();
        }
        if (value instanceof List) {
            return ((List<?>) value).size();
        }
        if (value instanceof Set) {
            return ((Set<?>) value).size();
        }
        throw new IllegalArgumentException("Unsupported collection type: " + value);
    }
}
