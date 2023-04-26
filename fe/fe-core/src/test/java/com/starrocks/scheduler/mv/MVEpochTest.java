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


package com.starrocks.scheduler.mv;

import com.starrocks.catalog.MvId;
import com.starrocks.common.io.DataOutputBuffer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MVEpochTest {

    @Test
    void write() throws IOException {
        long txnId = 9137;
        BinlogConsumeStateVO binlog = new BinlogConsumeStateVO();
        binlog.getBinlogMap().put(
                new BinlogConsumeStateVO.BinlogIdVO(1),
                new BinlogConsumeStateVO.BinlogLSNVO(2, 1));
        MVEpoch epoch = new MVEpoch(new MvId(0, 1024));
        epoch.onReady();
        epoch.onSchedule();
        epoch.onCommitting();
        epoch.onCommitted(binlog);
        epoch.setStartTimeMilli(1024);
        epoch.setCommitTimeMilli(1024);

        assertEquals(MVEpoch.EpochState.COMMITTED, epoch.getState());
        DataOutputBuffer buffer = new DataOutputBuffer(1024);
        epoch.write(buffer);
        byte[] bytes = buffer.getData();

        DataInput input = new DataInputStream(new ByteArrayInputStream(buffer.getData()));
        MVEpoch deserialized = MVEpoch.read(input);
        assertEquals(epoch, deserialized);
    }
}