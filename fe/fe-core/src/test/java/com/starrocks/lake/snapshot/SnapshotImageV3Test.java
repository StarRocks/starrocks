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

package com.starrocks.lake.snapshot;

import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV2;
import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV3;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnapshotImageV3Test {
    @TempDir
    Path temporary;
    private String oldMeta;
    private RestoreClusterSnapshotMgr restore;

    @BeforeEach
    public void setUp() throws Exception {
        oldMeta = Config.meta_dir;
        Config.meta_dir = temporary.toString();
        restore = Mockito.mock(RestoreClusterSnapshotMgr.class, Mockito.CALLS_REAL_METHODS);
        Files.createDirectories(root().resolve("starmgr"));
        Files.write(root().resolve("starmgr/image.20"), new byte[] {2});
    }

    @AfterEach
    public void tearDown() {
        Config.meta_dir = oldMeta;
    }

    private Path root() {
        return temporary.resolve("image");
    }

    private RestoredSnapshotInfo restoreInfo() {
        return Deencapsulation.invoke(restore, "buildRestoredSnapshotInfo", "fallback-name");
    }

    private Path manifest(long journalId) throws Exception {
        Path manifest = root().resolve(ClusterSnapshotUtils.SNAPSHOT_META_FILE_NAME);
        ClusterSnapshot snapshot = new ClusterSnapshot(1, "snapshot-name", ClusterSnapshot.ClusterSnapshotType.AUTOMATED,
                "storage", 1, 2, journalId, 20);
        Files.writeString(manifest, GsonUtils.GSON.toJson(snapshot));
        return manifest;
    }

    @Test
    public void restoresV3AndKeepsManifestJournalIdentity() throws Exception {
        writeV2(root(), 9, new byte[] {1});
        writeV3(root(), 10);
        Path metadata = manifest(10);
        RestoredSnapshotInfo info = restoreInfo();
        assertEquals("snapshot-name", info.getSnapshotName());
        assertEquals(10, info.getFeJournalId());
        assertEquals(20, info.getStarMgrJournalId());
        assertFalse(Files.exists(metadata));
    }

    @Test
    public void legacySnapshotWithoutManifestUsesNewestImage() throws Exception {
        writeV2(root(), 10, new byte[] {1});
        assertEquals(10, restoreInfo().getFeJournalId());
        writeV3(root(), 11);
        assertEquals(11, restoreInfo().getFeJournalId());
        assertEquals("fallback-name", restoreInfo().getSnapshotName());
    }

    @Test
    public void rejectsManifestThatDoesNotMatchActualImages() throws Exception {
        writeV3(root(), 11);
        Path metadata = manifest(10);
        assertThrows(StarRocksException.class, this::restoreInfo);
        assertTrue(Files.exists(metadata));
    }

    @Test
    public void rejectsCorruptPreferredImageAndMissingStarMgrImage() throws Exception {
        writeV2(root(), 10, new byte[] {1});
        Path v3 = writeV3(root(), 10);
        Files.write(v3, new byte[] {0});
        assertThrows(StarRocksException.class, this::restoreInfo);
        writeV3(root(), 10);
        Files.delete(root().resolve("starmgr/image.20"));
        assertThrows(StarRocksException.class, this::restoreInfo);
    }
}
