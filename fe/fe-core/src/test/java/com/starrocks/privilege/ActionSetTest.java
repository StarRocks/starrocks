// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ActionSetTest {
    private static final Action SELECT = new Action((short) 1, "SELECT");
    private static final Action INSERT = new Action((short) 2, "INSERT");
    private static final Action DELETE = new Action((short) 3, "DELETE");
    @Test
    public void testBasic() {
        List<Action> l = new ArrayList<>();

        // only have select
        l.add(SELECT);
        ActionSet s = new ActionSet(l);
        Assert.assertEquals(2, s.bitSet);
        Assert.assertTrue(s.contains(SELECT));
        Assert.assertTrue(s.contains(new ActionSet(Arrays.asList(SELECT))));
        Assert.assertFalse(s.contains(INSERT));
        Assert.assertFalse(s.contains(DELETE));
        Assert.assertFalse(s.contains(new ActionSet(Arrays.asList(INSERT, DELETE))));
        Assert.assertFalse(s.contains(new ActionSet(Arrays.asList(SELECT, DELETE))));
        Assert.assertFalse(s.isEmpty());

        // add select + insert
        l.clear();
        l.add(SELECT);
        l.add(INSERT);
        s.add(new ActionSet(l));
        Assert.assertEquals(2 + 4, s.bitSet);
        Assert.assertTrue(s.contains(SELECT));
        Assert.assertTrue(s.contains(INSERT));
        Assert.assertTrue(s.contains(new ActionSet(Arrays.asList(SELECT, INSERT))));
        Assert.assertFalse(s.contains(DELETE));
        Assert.assertFalse(s.contains(new ActionSet(Arrays.asList(DELETE))));
        Assert.assertFalse(s.isEmpty());

        // remove delete
        l.clear();
        l.add(DELETE);
        s.remove(new ActionSet(l));
        Assert.assertEquals(2 + 4, s.bitSet);
        Assert.assertTrue(s.contains(SELECT));
        Assert.assertTrue(s.contains(INSERT));
        Assert.assertTrue(s.contains(new ActionSet(Arrays.asList(SELECT, INSERT))));
        Assert.assertFalse(s.contains(DELETE));
        Assert.assertFalse(s.contains(new ActionSet(Arrays.asList(SELECT, DELETE))));
        Assert.assertFalse(s.isEmpty());

        // remove select + insert
        l.clear();
        l.add(SELECT);
        l.add(INSERT);
        s.remove(new ActionSet(l));
        Assert.assertEquals(0, s.bitSet);
        Assert.assertFalse(s.contains(SELECT));
        Assert.assertFalse(s.contains(INSERT));
        Assert.assertFalse(s.contains(DELETE));
        Assert.assertFalse(s.contains(new ActionSet(Arrays.asList(SELECT, DELETE, INSERT))));
        Assert.assertTrue(s.isEmpty());

    }

    @Test
    public void testDifferent() {
        ActionSet res = new ActionSet(Arrays.asList(INSERT, SELECT)).difference(
                new ActionSet(Arrays.asList(INSERT, DELETE)));
        System.out.println(res.bitSet);
        Assert.assertTrue(res.contains(DELETE));
        Assert.assertEquals(8L, res.bitSet);

        res = new ActionSet(Arrays.asList(INSERT)).difference(new ActionSet(Arrays.asList(INSERT, DELETE)));
        Assert.assertTrue(res.contains(DELETE));
        Assert.assertEquals(8L, res.bitSet);

        res = new ActionSet(Arrays.asList(INSERT, DELETE)).difference(new ActionSet(Arrays.asList(INSERT)));
        Assert.assertTrue(res.isEmpty());
        Assert.assertEquals(0L, res.bitSet);
    }
}
