// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.common.DdlException;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationScheduleTest {
    @Test
    public void testParseSchedule() throws DdlException {
        new ReplicationSchedule("1s");
        new ReplicationSchedule("1 s");
        new ReplicationSchedule("0.1s");
        new ReplicationSchedule("1seconds");
        new ReplicationSchedule("1M");
        new ReplicationSchedule("1minutes");
        new ReplicationSchedule("1h");
        new ReplicationSchedule("1Hours");
        new ReplicationSchedule("1d");
        new ReplicationSchedule("1days");

        try {
            new ReplicationSchedule("1a");
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
