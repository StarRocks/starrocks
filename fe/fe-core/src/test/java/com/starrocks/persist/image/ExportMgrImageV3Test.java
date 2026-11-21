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

package com.starrocks.persist.image;

import com.starrocks.common.Config;
import com.starrocks.load.ExportFailMsg;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The EXPORT_MGR section: export jobs saved as a v3 image and loaded back are the source jobs, as they are through
 * the v2 image, and both loads drop the jobs the manager no longer keeps.
 */
public class ExportMgrImageV3Test {
    @TempDir
    Path tempDir;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    /** Jobs in the v2 form, so that the source objects are what a v2 load delivers, not live ones. */
    private static List<ExportJob> sourceJobs(long now) {
        List<String> documents = new ArrayList<>();
        // pending: the optional members absent, the constructor defaults stand
        documents.add("{\"id\":1001,\"qd\":\"c9b4d5e6-1111-4a2b-9c3d-000000000001\",\"dd\":10001,\"td\":20001,"
                + "\"ep\":\"hdfs://nn:9000/export/pending/\",\"cs\":\"\\t\",\"rd\":\"\\n\",\"withHdr\":false,"
                + "\"tn\":{\"tbl\":\"t1\",\"fullDb\":\"default_cluster:db1\"},\"se\":\"PENDING\","
                + "\"ct\":" + now + ",\"st\":-1,\"ft\":-1,\"pg\":0}");
        // exporting: every member set
        documents.add("{\"id\":1002,\"qd\":\"c9b4d5e6-1111-4a2b-9c3d-000000000002\",\"dd\":10001,\"td\":20001,"
                + "\"bd\":{\"n\":\"broker0\",\"p\":{\"username\":\"u\",\"password\":\"p\"}},"
                + "\"ep\":\"hdfs://nn:9000/export/exporting/\",\"cs\":\",\",\"rd\":\"\\n\",\"withHdr\":true,"
                + "\"pt\":{\"timeout\":\"3600\",\"load_mem_limit\":\"1024\"},\"ps\":[\"p1\",\"p2\"],"
                + "\"tn\":{\"tbl\":\"t1\",\"fullDb\":\"default_cluster:db1\"},\"se\":\"EXPORTING\","
                + "\"ct\":" + now + ",\"st\":" + (now + 1) + ",\"ft\":-1,\"pg\":45,"
                + "\"fm\":{\"cancelType\":\"UNKNOWN\",\"msg\":\"\"},\"warehouseId\":7}");
        // finished just now: kept
        documents.add("{\"id\":1003,\"qd\":\"c9b4d5e6-1111-4a2b-9c3d-000000000003\",\"dd\":10001,\"td\":20002,"
                + "\"bd\":{\"n\":\"\",\"p\":{}},\"ep\":\"s3://bucket/export/\",\"cs\":\"|\",\"rd\":\"\\r\\n\","
                + "\"withHdr\":false,\"pt\":{},\"ps\":[],\"tn\":{\"tbl\":\"t2\",\"fullDb\":\"default_cluster:db1\"},"
                + "\"se\":\"FINISHED\",\"ct\":" + now + ",\"st\":" + (now + 1) + ",\"ft\":" + (now + 2) + ","
                + "\"pg\":100,\"fm\":{\"cancelType\":\"UNKNOWN\",\"msg\":\"\"},\"warehouseId\":0}");
        // cancelled two hours ago: older than history_job_keep_max_second, dropped by the load
        long twoHoursAgo = now - 2 * 3600 * 1000L;
        documents.add("{\"id\":1004,\"qd\":\"c9b4d5e6-1111-4a2b-9c3d-000000000004\",\"dd\":10002,\"td\":20003,"
                + "\"ep\":\"hdfs://nn:9000/export/cancelled/\",\"cs\":\"\\t\",\"rd\":\"\\n\",\"withHdr\":false,"
                + "\"tn\":{\"tbl\":\"t3\",\"fullDb\":\"default_cluster:db2\"},\"se\":\"CANCELLED\","
                + "\"ct\":" + twoHoursAgo + ",\"st\":" + (twoHoursAgo + 1) + ",\"ft\":" + (twoHoursAgo + 2) + ","
                + "\"pg\":10,\"fm\":{\"cancelType\":\"USER_CANCEL\",\"msg\":\"cancelled by user\"},\"warehouseId\":0}");
        List<ExportJob> jobs = new ArrayList<>();
        for (String document : documents) {
            jobs.add(GsonUtils.GSON.fromJson(document, ExportJob.class));
        }
        return jobs;
    }

    @Test
    public void testV3ImageMatchesV2() throws Exception {
        int keepMaxSecond = Config.history_job_keep_max_second;
        Config.history_job_keep_max_second = 3600;
        try {
            long now = System.currentTimeMillis();
            List<ExportJob> sourceJobs = sourceJobs(now);
            ExportMgr live = new ExportMgr();
            for (ExportJob job : sourceJobs) {
                live.unprotectAddJob(job);
            }
            Assertions.assertEquals(4, live.getIdToJob().size());

            // reference: the v2 image round trip
            UtFrameUtils.PseudoImage v2Image = new UtFrameUtils.PseudoImage();
            live.saveExportJobV2(v2Image.getImageWriter());
            ExportMgr v2Loaded = new ExportMgr();
            v2Loaded.loadExportJobV2(v2Image.getMetaBlockReader());

            // the v3 image round trip
            Path file = tempDir.resolve("export_mgr.image");
            try (OutputStream os = Files.newOutputStream(file);
                    ImageWriterV3 writer = new ImageWriterV3(os, 1L)) {
                live.save(writer);
                writer.finish();
            }
            ExportMgr v3Loaded = new ExportMgr();
            try (ImageReaderV3 reader = new ImageReaderV3(file)) {
                List<IndexEntryPB> sections = reader.sections();
                Assertions.assertEquals(1, sections.size());
                Assertions.assertEquals(SectionId.EXPORT_MGR.getValue(), sections.get(0).getType());
                // every job is written; the expiry is applied by the load
                Assertions.assertEquals(4, sections.get(0).getNumEntries());
                reader.load(v3Loaded.getImageV3Loaders());
            }

            // Both loads drop the cancelled job of two hours ago and deliver the source jobs otherwise.
            Set<Long> kept = Set.of(1001L, 1002L, 1003L);
            Assertions.assertEquals(kept, v2Loaded.getIdToJob().keySet());
            Assertions.assertEquals(kept, v3Loaded.getIdToJob().keySet());
            for (ExportJob source : sourceJobs) {
                if (!kept.contains(source.getId())) {
                    continue;
                }
                for (ExportMgr loaded : List.of(v2Loaded, v3Loaded)) {
                    ExportJob job = loaded.getIdToJob().get(source.getId());
                    GsonEquivalence.assertEquivalent(source, job);
                    Assertions.assertEquals(source.getQueryId(), job.getQueryId());
                    Assertions.assertEquals(source.getState(), job.getState());
                    Assertions.assertTrue(job.isReplayed());
                }
            }

            // the optional members made the trip, and the defaults stood where the source had none
            ExportJob exporting = v3Loaded.getIdToJob().get(1002L);
            Assertions.assertEquals("db1", exporting.getTableName().getDb());
            Assertions.assertEquals("t1", exporting.getTableName().getTbl());
            Assertions.assertEquals(List.of("p1", "p2"), exporting.getPartitions());
            Assertions.assertEquals("broker0", exporting.getBrokerDesc().getName());
            Assertions.assertEquals(7, exporting.getWarehouseId());
            ExportJob pending = v3Loaded.getIdToJob().get(1001L);
            Assertions.assertNull(pending.getPartitions());
            Assertions.assertEquals(ExportFailMsg.CancelType.UNKNOWN, pending.getFailMsg().getCancelType());
            Assertions.assertEquals("hdfs://nn:9000/export/pending/", pending.getExportPath());
        } finally {
            Config.history_job_keep_max_second = keepMaxSecond;
        }
    }
}
