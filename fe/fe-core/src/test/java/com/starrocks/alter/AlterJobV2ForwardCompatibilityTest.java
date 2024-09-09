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

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.IForwardCompatibleObject;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV1;
import com.starrocks.utframe.GsonReflectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AlterJobV2ForwardCompatibilityTest {
    private static final Logger LOG = LogManager.getLogger(AlterJobV2ForwardCompatibilityTest.class);

    // simulate a new subtype from the future
    private static class SomeSubAlterJobV2FromFuture extends AlterJobV2 {
        @SerializedName(value = "new_pros")
        private String newProperty = "hello_world";

        public SomeSubAlterJobV2FromFuture(long jobId, JobType jobType, long dbId, long tableId, String tableName,
                                           long timeoutMs) {
            super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
        }

        @Override
        protected void runPendingJob() throws AlterCancelException {
            // do nothing
        }

        @Override
        protected void runWaitingTxnJob() throws AlterCancelException {
            // do nothing
        }

        @Override
        protected void runRunningJob() throws AlterCancelException {
            // do nothing
        }

        @Override
        protected void runFinishedRewritingJob() throws AlterCancelException {
            // do nothing
        }

        @Override
        protected boolean cancelImpl(String errMsg) {
            return false;
        }

        @Override
        protected void getInfo(List<List<Comparable>> infos) {
            // do nothing
        }

        @Override
        public void replay(AlterJobV2 replayedJob) {
            // do nothing
        }

        @Override
        public Optional<Long> getTransactionId() {
            return Optional.empty();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // do nothing
        }
    }

    private Gson getGsonWithRegisteredSubType(Gson gson, Class<? extends AlterJobV2> subType, String label) {
        // runtime typeAdapter for class AlterJobV2
        RuntimeTypeAdapterFactory<AlterJobV2> alterJobV2Factory =
                RuntimeTypeAdapterFactory.of(AlterJobV2.class, "clazz")
                        .registerSubtype(RollupJobV2.class, "RollupJobV2")
                        .registerSubtype(SchemaChangeJobV2.class, "SchemaChangeJobV2")
                        .registerSubtype(OptimizeJobV2.class, "OptimizeJobV2")
                        .registerSubtype(OnlineOptimizeJobV2.class, "OnlineOptimizeJobV2");
        if (subType != null) {
            alterJobV2Factory.registerSubtype(subType, label);
        }
        return gson.newBuilder().registerTypeAdapterFactory(alterJobV2Factory).create();
    }

    // dummy test against ForwardCompatibleAlterJobV2Object for just code coverage
    // rarely used
    @Test
    public void testDummyForwardCompatibleAlterJobV2Object() {
        ForwardCompatibleAlterJobV2Object fcObject =
                new ForwardCompatibleAlterJobV2Object(1, AlterJobV2.JobType.SCHEMA_CHANGE, 1000,
                        2000, "tableName", 1000);
        Assert.assertThrows(AlterCancelException.class, fcObject::runPendingJob);
        Assert.assertThrows(AlterCancelException.class, fcObject::runWaitingTxnJob);
        Assert.assertThrows(AlterCancelException.class, fcObject::runRunningJob);
        Assert.assertThrows(AlterCancelException.class, fcObject::runFinishedRewritingJob);
        Assert.assertFalse(fcObject.cancelImpl(""));
        ExceptionChecker.expectThrowsNoException(() -> fcObject.getInfo(null));
        ExceptionChecker.expectThrowsNoException(() -> fcObject.replay(null));
        Assert.assertTrue(fcObject.getTransactionId().isEmpty());
        ExceptionChecker.expectThrowsNoException(() -> fcObject.write(null));
    }

    @Test
    public void testSerializeDeserializeFutureSubType() {
        Gson purifiedGson = GsonReflectUtils.removeRuntimeTypeAdapterFactoryForBaseType(GsonUtils.GSON.newBuilder(),
                AlterJobV2.class).create();

        // old version, Neither `SomeSubAlterJobV2FromFuture` nor `ForwardCompatibleAlterJobV2Object` registered
        Gson oldVersionNoFCFallback = getGsonWithRegisteredSubType(purifiedGson, null, null);

        // new version with the correct compatible subtype:SomeSubAlterJobV2FromFuture registered
        // but not for the `ForwardCompatibleAlterJobV2Object`
        Gson newVersionWithSubType = getGsonWithRegisteredSubType(purifiedGson, SomeSubAlterJobV2FromFuture.class,
                "SomeSubAlterJobV2FromFuture");

        // new version has only `ForwardCompatibleAlterJobV2Object` registered
        Gson newVersionFCFallback = GsonUtils.GSON;

        SomeSubAlterJobV2FromFuture futureJob =
                new SomeSubAlterJobV2FromFuture(100, AlterJobV2.JobType.SCHEMA_CHANGE,
                        1000, 2000, "table_name", 1000);
        String jsonString = newVersionWithSubType.toJson(futureJob, AlterJobV2.class);
        LOG.info("JSON str for the deserialize testing: {}", jsonString);

        // parse json with gson knowing the new type
        {
            AlterJobV2 job = newVersionWithSubType.fromJson(jsonString, AlterJobV2.class);
            Assert.assertNotNull(job);
            // can correctly recover from the json string
            Assert.assertTrue(job instanceof SomeSubAlterJobV2FromFuture);
        }

        // parse the json with oldVersionJson who doesn't know the new type
        // and also doesn't have the fallback type registered.
        {
            Assert.assertThrows(JsonParseException.class,
                    () -> oldVersionNoFCFallback.fromJson(jsonString, AlterJobV2.class));
        }

        // parse json with gson knowing the new type registered to the dummy ForwardedCompatibleObject
        {
            AlterJobV2 job =
                    newVersionFCFallback.fromJson(jsonString, AlterJobV2.class);
            Assert.assertNotNull(job);
            Assert.assertTrue(job instanceof IForwardCompatibleObject);
            Assert.assertTrue(job instanceof ForwardCompatibleAlterJobV2Object);
        }
    }

    @Test
    public void testAlterJobMgrSaveAndLoadForwardCompatibility()
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        // Remove the RuntimeTypeAdapterFactory for AlterJobV2, because the same type can't be
        // registered repeatedly.
        Gson purifiedGson = GsonReflectUtils.removeRuntimeTypeAdapterFactoryForBaseType(GsonUtils.GSON.newBuilder(),
                AlterJobV2.class).create();

        // old version, Neither `SomeSubAlterJobV2FromFuture` nor `ForwardCompatibleAlterJobV2Object` registered
        Gson oldVersionNoFCFallback = getGsonWithRegisteredSubType(purifiedGson, null, null);

        // new version with the correct compatible subtype registered, but no `ForwardCompatibleAlterJobV2Object`
        Gson newVersionWithSubType =
                getGsonWithRegisteredSubType(purifiedGson, SomeSubAlterJobV2FromFuture.class,
                        "SomeSubAlterJobV2FromFuture");

        // new version has only `ForwardCompatibleAlterJobV2Object` registered
        Gson newVersionFCFallback = GsonUtils.GSON.newBuilder().create();

        SomeSubAlterJobV2FromFuture futureJob =
                new SomeSubAlterJobV2FromFuture(100, AlterJobV2.JobType.SCHEMA_CHANGE,
                        1000, 2000, "table_name", 1000);
        SomeSubAlterJobV2FromFuture futureMVJob =
                new SomeSubAlterJobV2FromFuture(200, AlterJobV2.JobType.ROLLUP,
                        1100, 2100, "MV", 1000);

        SchemaChangeJobV2 schemaChangeJob =
                new SchemaChangeJobV2(10086L, 1001L, 2001L, "schemaChangeJob_tbl_name", 1000L);

        SchemaChangeHandler scHandler = new SchemaChangeHandler();
        MaterializedViewHandler mvHandler = new MaterializedViewHandler();
        scHandler.addAlterJobV2(futureJob);
        scHandler.addAlterJobV2(schemaChangeJob);
        mvHandler.addAlterJobV2(futureMVJob);
        AlterJobMgr mgr = new AlterJobMgr(scHandler, mvHandler, null);

        ByteArrayOutputStream baseOS = new ByteArrayOutputStream();

        // GsonUtils.GSON = newVersionWithSubType, so the new subtype can be serialized correctly.
        GsonReflectUtils.partialMockGsonExpectations(newVersionWithSubType);

        ImageWriter writer = new ImageWriter("dir", ImageFormatVersion.v1, 0);
        writer.setOutputStream(new DataOutputStream(baseOS));

        mgr.save(writer);
        byte[] rawBytes = baseOS.toByteArray();

        // save and restore with the same GSON RuntimeAdaptorFactory
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            SchemaChangeHandler replayScHandler = new SchemaChangeHandler();
            MaterializedViewHandler replayMVHander = new MaterializedViewHandler();
            AlterJobMgr replayMgr = new AlterJobMgr(replayScHandler, replayMVHander, null);
            SRMetaBlockReader reader = new SRMetaBlockReaderV1(new DataInputStream(baseIn));
            replayMgr.load(reader);
            // both partitions should be loaded correctly
            Map<Long, AlterJobV2> jobs = replayScHandler.getAlterJobsV2();

            AlterJobV2 futureJob2 = jobs.get(futureJob.getJobId());
            Assert.assertNotNull(futureJob2);
            Assert.assertTrue(futureJob2 instanceof SomeSubAlterJobV2FromFuture);

            AlterJobV2 schemaJob2 = jobs.get(schemaChangeJob.getJobId());
            Assert.assertNotNull(schemaJob2);
            Assert.assertTrue(schemaJob2 instanceof SchemaChangeJobV2);

            Map<Long, AlterJobV2> mvJobs = replayMVHander.getAlterJobsV2();
            AlterJobV2 futureMvJob2 = mvJobs.get(futureMVJob.getJobId());
            Assert.assertNotNull(futureMvJob2);
            Assert.assertTrue(futureMvJob2 instanceof SomeSubAlterJobV2FromFuture);
        }

        // test `oldVersionJson`, the unknown new subtype will cause the load excepted
        GsonReflectUtils.partialMockGsonExpectations(oldVersionNoFCFallback);
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            SchemaChangeHandler replayScHandler = new SchemaChangeHandler();
            MaterializedViewHandler replayMVHander = new MaterializedViewHandler();
            AlterJobMgr replayMgr = new AlterJobMgr(replayScHandler, replayMVHander, null);
            SRMetaBlockReader reader = new SRMetaBlockReaderV1(new DataInputStream(baseIn));
            Assert.assertThrows(JsonParseException.class, () -> replayMgr.load(reader));
        }

        // test `oldJsonWithForwardCompatibility`
        // the new subtype is registered as the ForwardCompatibleRecyclePartitionInfoV2
        GsonReflectUtils.partialMockGsonExpectations(newVersionFCFallback);
        {
            ByteArrayInputStream baseIn = new ByteArrayInputStream(rawBytes);
            SchemaChangeHandler replayScHandler = new SchemaChangeHandler();
            MaterializedViewHandler replayMVHander = new MaterializedViewHandler();
            AlterJobMgr replayMgr = new AlterJobMgr(replayScHandler, replayMVHander, null);
            SRMetaBlockReader reader = new SRMetaBlockReaderV1(new DataInputStream(baseIn));
            replayMgr.load(reader);
            // futureJob is ignored
            Assert.assertFalse(replayScHandler.getAlterJobsV2().containsKey(futureJob.getJobId()));
            // schemaChangeJob is preserved
            Assert.assertTrue(replayScHandler.getAlterJobsV2().containsKey(schemaChangeJob.getJobId()));

            // futureMVJob is ignored
            Map<Long, AlterJobV2> mvJobs = replayMVHander.getAlterJobsV2();
            Assert.assertTrue(mvJobs.isEmpty());
        }
    }
}
