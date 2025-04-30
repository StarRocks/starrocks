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
package com.starrocks.journal;

import com.starrocks.common.Config;
import com.starrocks.ha.BDBHA;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckpointWorkerTest {
    private final List<File> tmpDirs = new ArrayList<>();

    public void createImageFile() throws Exception {
        File metaDir = Files.createTempDirectory(Paths.get("."), "CheckpointWorkerTest").toFile();
        tmpDirs.add(metaDir);
        Config.meta_dir = metaDir.getAbsolutePath();
        new File(Config.meta_dir + "/image/v2/").mkdirs();

        Files.createFile(Path.of(Config.meta_dir, "image", "v2", "image.1000"));
    }

    @After
    public void cleanup() throws Exception {
        for (File tmpDir : tmpDirs) {
            FileUtils.deleteDirectory(tmpDir);
        }
    }

    @Test
    public void testPreCheckParamValid() throws Exception {
        BDBHA bdbha = mock(BDBHA.class);
        when(bdbha.getLatestEpoch()).thenReturn(10L);
        GlobalStateMgr.getServingState().setHaProtocol(bdbha);

        createImageFile();

        Assert.assertTrue(new File(Config.meta_dir + "/image/v2/image.1000").exists());

        CheckpointWorker worker = new GlobalStateCheckpointWorker(null);
        worker.init();
        Assert.assertFalse(worker.preCheckParamValid(9, 999));
        Assert.assertFalse(worker.preCheckParamValid(9, 1000));
        Assert.assertFalse(worker.preCheckParamValid(9, 1001));
    }
}
