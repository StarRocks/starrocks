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

package com.starrocks.sql.automv.qe;

import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.EditLogDeserializer;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class RecommendationsTaskTest {
    @Test
    public void testRecommendationsTaskStatusSerde() throws IOException {
        RecommendationsTaskStatus taskStatus =
                new RecommendationsTaskStatus("abc",
                        "default_catalog.tunespace_db.__tunespace__",
                        "default_catalog.tunespace_db.__recommendations_result__");

        taskStatus.setStatus(RecommendationsTaskStatus.Status.PENDING);
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(byteOutput);
        taskStatus.write(dataOutput);

        ByteArrayInputStream byteInput = new ByteArrayInputStream(byteOutput.toByteArray());
        DataInput dataInput = new DataInputStream(byteInput);
        RecommendationsTaskStatus taskStatus2 = RecommendationsTaskStatus.read(dataInput);
        Assert.assertEquals(taskStatus2, taskStatus);
    }

    @Test
    public void testRecommendationsTaskStatusSerde2() throws IOException {
        RecommendationsTaskStatus taskStatus =
                new RecommendationsTaskStatus("foobar", "a.b.c", "b.c.d");

        DataOutputBuffer buffer = new DataOutputBuffer(1024);
        JournalEntity entity = new JournalEntity(OperationTypeEPack.OP_RECOMMENDATIONS_TASK_STATUS_CHANGE,
                new Text(GsonUtils.GSON.toJson(taskStatus)));
        buffer.writeShort(entity.opCode());
        entity.data().write(buffer);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        short opCode = in.readShort();
        JournalEntity replayEntry = new JournalEntity(opCode, EditLogDeserializer.deserialize(opCode, in));
        Assert.assertEquals(OperationTypeEPack.OP_RECOMMENDATIONS_TASK_STATUS_CHANGE, replayEntry.opCode());
    }

    @Test
    public void testRecommendationsTaskManagerSaveAndLoad()
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        RecommendationsTaskManager mgr = new RecommendationsTaskManager();
        for (int i = 0; i < 10; ++i) {
            RecommendationsTaskStatus taskStatus =
                    new RecommendationsTaskStatus("foobar" + i, "a.b.c", "b.c.d");
            mgr.applyLogEntry(taskStatus);
        }
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        mgr.save(image.getImageWriter());
        SRMetaBlockReader reader = image.getMetaBlockReader();
        RecommendationsTaskManager mgr2 = new RecommendationsTaskManager();
        mgr2.load(reader);

        for (int i = 0; i < 10; ++i) {
            RecommendationsTaskStatus taskStatus = mgr2.getTaskStatus("foobar" + i);
            Assert.assertNotNull(taskStatus);
            Assert.assertEquals(taskStatus, mgr.getTaskStatus("foobar" + i));
        }
    }
}
