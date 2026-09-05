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

import com.google.common.collect.Maps;
import com.starrocks.backup.SnapshotRetentionCache.SnapshotRetention;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SnapshotRetentionCacheTest {

    private static final String MD5 = "0123456789abcdef0123456789abcdef";
    private static final String TS1 = "2026-01-01-10-00-00-000";
    private static final String TS2 = "2026-02-02-10-00-00-000";

    private Repository repo;
    private SnapshotRetentionCache cache;

    /** What the repository currently holds under the label, and how often it was asked for it. */
    private String remoteTimestamp;
    private Integer remoteClusterId;
    private Long remoteFinishTime;
    private String remoteTtl;
    private Long remoteExpireTime;
    private int listCount;
    private int readCount;

    private static RemoteFile jobInfoFileOf(String backupTimestamp) {
        return new RemoteFile(Repository.PREFIX_JOB_INFO + backupTimestamp + "." + MD5, true, 100);
    }

    @BeforeEach
    public void setUp() {
        repo = new Repository(10000, "repo", false, "bos://backup", new BlobStorage("broker", Maps.newHashMap()));
        cache = new SnapshotRetentionCache();
        remoteTimestamp = TS1;
        remoteClusterId = 1001;
        remoteFinishTime = 1522231900000L;
        remoteTtl = "7 DAY";
        remoteExpireTime = 1522836664000L;
        listCount = 0;
        readCount = 0;

        new MockUp<Repository>() {
            @Mock
            public RemoteFile findJobInfoFile(String label) {
                listCount++;
                return remoteTimestamp == null ? null : jobInfoFileOf(remoteTimestamp);
            }

            @Mock
            public Status readJobInfoFile(String label, RemoteFile jobInfoFile, List<BackupJobInfo> infos) {
                readCount++;
                BackupJobInfo info = new BackupJobInfo();
                info.clusterId = remoteClusterId;
                info.finishTime = remoteFinishTime;
                info.ttl = remoteTtl;
                info.expireTime = remoteExpireTime;
                infos.add(info);
                return Status.OK;
            }
        };
    }

    @Test
    public void testHitSkipsTheDownloadButNotTheListing() {
        SnapshotRetention first = cache.get(repo, "label1");
        Assertions.assertNotNull(first);
        Assertions.assertEquals(TS1, first.getBackupTimestamp());
        Assertions.assertEquals(remoteFinishTime, first.getFinishTime());
        Assertions.assertEquals(1, listCount);
        Assertions.assertEquals(1, readCount);

        SnapshotRetention second = cache.get(repo, "label1");
        Assertions.assertSame(first, second);
        // The listing is what says which backup sits under the label right now, so it always runs.
        Assertions.assertEquals(2, listCount);
        Assertions.assertEquals(1, readCount);
    }

    @Test
    public void testLabelBackedUpAgainReadsThrough() {
        SnapshotRetention first = cache.get(repo, "label1");
        Assertions.assertEquals("7 DAY", first.getTtl());

        // The label was dropped and backed up again: same name, different backup, no ttl.
        remoteTimestamp = TS2;
        remoteTtl = null;
        remoteExpireTime = null;

        SnapshotRetention second = cache.get(repo, "label1");
        Assertions.assertEquals(TS2, second.getBackupTimestamp());
        Assertions.assertNull(second.getTtl());
        Assertions.assertNull(second.getExpireTime());
        Assertions.assertEquals(2, readCount);

        // And the entry was replaced rather than added alongside the old one.
        SnapshotRetention third = cache.get(repo, "label1");
        Assertions.assertSame(second, third);
        Assertions.assertEquals(2, readCount);
    }

    @Test
    public void testInvalidateForgetsTheEntry() {
        cache.get(repo, "label1");
        Assertions.assertEquals(1, readCount);

        cache.invalidate(repo.getId(), "label1");
        cache.get(repo, "label1");
        Assertions.assertEquals(2, readCount);
    }

    @Test
    public void testNoJobInfoFile() {
        remoteTimestamp = null;
        Assertions.assertNull(cache.get(repo, "label1"));
        Assertions.assertEquals(0, readCount);
    }

    @Test
    public void testOwnershipAndExpiry() {
        SnapshotRetention retention = cache.get(repo, "label1");
        Assertions.assertTrue(retention.isOwnedBy(1001));
        Assertions.assertFalse(retention.isOwnedBy(1002));
        Assertions.assertTrue(retention.isExpired(remoteExpireTime));
        Assertions.assertTrue(retention.isExpired(remoteExpireTime + 1));
        Assertions.assertFalse(retention.isExpired(remoteExpireTime - 1));
    }

    @Test
    public void testSnapshotWithoutRetentionBelongsToNobody() {
        remoteClusterId = null;
        remoteFinishTime = null;
        remoteTtl = null;
        remoteExpireTime = null;

        SnapshotRetention retention = cache.get(repo, "label1");
        Assertions.assertNotNull(retention);
        Assertions.assertNull(retention.getFinishTime());
        Assertions.assertFalse(retention.isOwnedBy(1001));
        Assertions.assertFalse(retention.isExpired(Long.MAX_VALUE));
    }
}
