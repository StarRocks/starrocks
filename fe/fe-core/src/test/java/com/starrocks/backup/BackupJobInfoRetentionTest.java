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

package com.starrocks.backup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The retention fields a backup writes into its job info file, and how a reader that does not find
 * them behaves. Both directions matter for upgrades: an older FE has to keep reading a file this
 * version wrote, and this version has to keep reading a file an older FE wrote.
 */
public class BackupJobInfoRetentionTest {

    private static final String LEGACY_JSON = "{\n"
            + "    \"backup_time\": 1522231864000,\n"
            + "    \"name\": \"snapshot1\",\n"
            + "    \"database\": \"db1\",\n"
            + "    \"id\": 10000,\n"
            + "    \"backup_result\": \"succeed\",\n"
            + "    \"backup_objects\": {}\n"
            + "}";

    private static final String JSON_WITH_RETENTION = "{\n"
            + "    \"backup_time\": 1522231864000,\n"
            + "    \"name\": \"snapshot1\",\n"
            + "    \"database\": \"db1\",\n"
            + "    \"id\": 10000,\n"
            + "    \"cluster_id\": 1276893842,\n"
            + "    \"finish_time\": 1522231900000,\n"
            + "    \"ttl\": \"7 DAY\",\n"
            + "    \"expire_time\": 1522836664000,\n"
            + "    \"backup_result\": \"succeed\",\n"
            + "    \"backup_objects\": {}\n"
            + "}";

    private File jobInfoFile;

    @AfterEach
    public void deleteFile() {
        if (jobInfoFile != null && jobInfoFile.exists()) {
            jobInfoFile.delete();
        }
    }

    private BackupJobInfo parse(String json) throws IOException {
        deleteFile();
        jobInfoFile = File.createTempFile("job_info_retention", ".json");
        try (PrintWriter out = new PrintWriter(jobInfoFile)) {
            out.print(json);
        }
        return BackupJobInfo.fromFile(jobInfoFile.getAbsolutePath());
    }

    @Test
    public void testReadRetention() throws IOException {
        BackupJobInfo info = parse(JSON_WITH_RETENTION);
        Assertions.assertEquals(Integer.valueOf(1276893842), info.clusterId);
        Assertions.assertEquals(Long.valueOf(1522231900000L), info.finishTime);
        Assertions.assertEquals("7 DAY", info.ttl);
        Assertions.assertEquals(Long.valueOf(1522836664000L), info.expireTime);
        // The job started before it wrapped up, and expireTime was resolved from the latter.
        Assertions.assertTrue(info.backupTime < info.finishTime);
    }

    @Test
    public void testReadFileWrittenBeforeRetentionExisted() throws IOException {
        BackupJobInfo info = parse(LEGACY_JSON);
        // No cluster is recorded, which is what leaves the snapshot out of automatic cleanup.
        Assertions.assertNull(info.clusterId);
        Assertions.assertNull(info.finishTime);
        Assertions.assertNull(info.ttl);
        Assertions.assertNull(info.expireTime);
        // The rest of the file still reads as before.
        Assertions.assertEquals("snapshot1", info.name);
        Assertions.assertEquals("db1", info.dbName);
        Assertions.assertEquals(1522231864000L, info.backupTime);
    }

    @Test
    public void testWriteThenReadRetention() throws IOException {
        BackupJobInfo written = parse(JSON_WITH_RETENTION);
        BackupJobInfo reread = parse(written.toJson(true).toString());

        Assertions.assertEquals(written.clusterId, reread.clusterId);
        Assertions.assertEquals(written.finishTime, reread.finishTime);
        Assertions.assertEquals(written.ttl, reread.ttl);
        Assertions.assertEquals(written.expireTime, reread.expireTime);
    }

    @Test
    public void testKeepForeverWritesNoRetentionKeys() throws IOException {
        BackupJobInfo info = parse(LEGACY_JSON);
        String json = info.toJson(true).toString();

        // An older FE reads this file by key, so an unset retention has to be absent rather than
        // present and empty.
        Assertions.assertFalse(json.contains("cluster_id"));
        Assertions.assertFalse(json.contains("finish_time"));
        Assertions.assertFalse(json.contains("ttl"));
        Assertions.assertFalse(json.contains("expire_time"));

        BackupJobInfo reread = parse(json);
        Assertions.assertNull(reread.clusterId);
        Assertions.assertNull(reread.finishTime);
        Assertions.assertNull(reread.ttl);
        Assertions.assertNull(reread.expireTime);
    }
}
