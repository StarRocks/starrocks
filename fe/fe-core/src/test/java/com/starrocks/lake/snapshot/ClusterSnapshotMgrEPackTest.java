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

import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ClusterSnapshotMgrEPackTest {
    // Regression test for POST-1636: the automated cluster snapshot interval set via
    // ALTER AUTOMATED CLUSTER SNAPSHOT SET INTERVAL must survive an FE restart (image reload).
    // ClusterSnapshotMgrEPack.load() previously dropped automatedSnapshotIntervalSeconds, so the
    // interval reverted to the 600s (10 min) default after restart.
    @Test
    public void testAutomatedSnapshotIntervalSurvivesImageReload()
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        long intervalSeconds = 86400L; // 1440 min, deliberately different from the 600s (10 min) default

        ClusterSnapshotMgrEPack mgr = new ClusterSnapshotMgrEPack();
        mgr.setAutomatedSnapshotInterval(intervalSeconds);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        mgr.save(image.getImageWriter());

        ClusterSnapshotMgrEPack reloaded = new ClusterSnapshotMgrEPack();
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        reloaded.load(reader);

        Assertions.assertEquals(intervalSeconds, reloaded.getAutomatedSnapshotIntervalSeconds());
        Assertions.assertEquals(intervalSeconds, reloaded.getEffectiveAutomatedSnapshotIntervalSeconds());
    }
}
