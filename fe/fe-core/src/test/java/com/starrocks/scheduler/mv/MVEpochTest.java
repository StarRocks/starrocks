// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

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
        MVEpoch epoch = new MVEpoch(1024);
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