// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.common.DdlException;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationScheduleTest {
    @Test
    public void testParseSchedule() throws DdlException {
        ReplicationSchedule schedule1 = new ReplicationSchedule("1s");
        ReplicationSchedule schedule2 = new ReplicationSchedule("1 s");
        ReplicationSchedule schedule3 = new ReplicationSchedule("0.1s");
        ReplicationSchedule schedule4 = new ReplicationSchedule("1seconds");
        ReplicationSchedule schedule5 = new ReplicationSchedule("1M");
        ReplicationSchedule schedule6 = new ReplicationSchedule("1minutes");
        ReplicationSchedule schedule7 = new ReplicationSchedule("1h");
        ReplicationSchedule schedule8 = new ReplicationSchedule("1Hours");
        ReplicationSchedule schedule9 = new ReplicationSchedule("1d");
        ReplicationSchedule schedule10 = new ReplicationSchedule("1days");

        try {
            ReplicationSchedule schedule11 = new ReplicationSchedule("1a");
        } catch (Exception e) {
            Assert.assertEquals("Invalid parameter 1a", e.getMessage());
        }
    }

    @Test
    public void testSchedule() throws DdlException {
        ReplicationSchedule schedule = new ReplicationSchedule();

        Assert.assertFalse(schedule.isScheduled());
        Assert.assertFalse(schedule.isFinished());
        Assert.assertFalse(schedule.isPending());

        Assert.assertFalse(schedule.isRoundScheduled());
        Assert.assertFalse(schedule.isRoundFinished());
        Assert.assertFalse(schedule.isRoundPending());
        Assert.assertFalse(schedule.needRetry());

        Assert.assertTrue(schedule.needSchedule());

        schedule.startSchedule(); // start

        Assert.assertTrue(schedule.isScheduled());
        Assert.assertFalse(schedule.isFinished());
        Assert.assertTrue(schedule.isPending());

        Assert.assertTrue(schedule.isRoundScheduled());
        Assert.assertFalse(schedule.isRoundFinished());
        Assert.assertTrue(schedule.isRoundPending());
        Assert.assertFalse(schedule.needRetry());

        Assert.assertFalse(schedule.needSchedule());

        schedule.finishSchedule(false); // finish

        Assert.assertTrue(schedule.isScheduled());
        Assert.assertTrue(schedule.isFinished());
        Assert.assertFalse(schedule.isPending());

        Assert.assertTrue(schedule.isRoundScheduled());
        Assert.assertTrue(schedule.isRoundFinished());
        Assert.assertFalse(schedule.isRoundPending());
        Assert.assertFalse(schedule.needRetry());

        Assert.assertTrue(schedule.needSchedule());

        schedule.startSchedule(); // start

        Assert.assertTrue(schedule.isScheduled());
        Assert.assertFalse(schedule.isFinished());
        Assert.assertTrue(schedule.isPending());

        Assert.assertTrue(schedule.isRoundScheduled());
        Assert.assertFalse(schedule.isRoundFinished());
        Assert.assertTrue(schedule.isRoundPending());
        Assert.assertFalse(schedule.needRetry());

        Assert.assertFalse(schedule.needSchedule());

        schedule.finishSchedule(true); // finish with retry

        Assert.assertTrue(schedule.isScheduled());
        Assert.assertFalse(schedule.isFinished());
        Assert.assertTrue(schedule.isPending());

        Assert.assertTrue(schedule.isRoundScheduled());
        Assert.assertTrue(schedule.isRoundFinished());
        Assert.assertFalse(schedule.isRoundPending());
        Assert.assertTrue(schedule.needRetry());

        Assert.assertTrue(schedule.needSchedule());

        schedule.startSchedule(); // start retry

        Assert.assertTrue(schedule.isScheduled());
        Assert.assertFalse(schedule.isFinished());
        Assert.assertTrue(schedule.isPending());

        Assert.assertTrue(schedule.isRoundScheduled());
        Assert.assertFalse(schedule.isRoundFinished());
        Assert.assertTrue(schedule.isRoundPending());
        Assert.assertFalse(schedule.needRetry());

        Assert.assertFalse(schedule.needSchedule());

        schedule.finishSchedule(false); // finish

        Assert.assertTrue(schedule.isScheduled());
        Assert.assertTrue(schedule.isFinished());
        Assert.assertFalse(schedule.isPending());

        Assert.assertTrue(schedule.isRoundScheduled());
        Assert.assertTrue(schedule.isRoundFinished());
        Assert.assertFalse(schedule.isRoundPending());
        Assert.assertFalse(schedule.needRetry());

        Assert.assertTrue(schedule.needSchedule());
    }
}
