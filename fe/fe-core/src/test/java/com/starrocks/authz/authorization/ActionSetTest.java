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


package com.starrocks.authz.authorization;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ActionSetTest {
    private static final PrivilegeType SELECT = PrivilegeType.SELECT;
    private static final PrivilegeType INSERT = PrivilegeType.INSERT;
    private static final PrivilegeType DELETE = PrivilegeType.DELETE;

    @Test
    public void testBasic() {
        List<PrivilegeType> l = new ArrayList<>();

        // only have select
        l.add(SELECT);
        ActionSet s = new ActionSet(l);
        Assert.assertEquals(128, s.bitSet);
        Assert.assertTrue(s.contains(SELECT));
        Assert.assertTrue(s.contains(new ActionSet(Arrays.asList(SELECT))));
        Assert.assertFalse(s.contains(INSERT));
        Assert.assertFalse(s.contains(DELETE));
        Assert.assertFalse(s.contains(new ActionSet(Arrays.asList(INSERT, DELETE))));
        Assert.assertFalse(s.contains(new ActionSet(Arrays.asList(SELECT, DELETE))));
        Assert.assertFalse(s.isEmpty());

        // add select + insert
        l.clear();
        l.add(PrivilegeType.SELECT);
        l.add(PrivilegeType.INSERT);
        s.add(new ActionSet(l));
        Assert.assertEquals(192, s.bitSet);
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
        Assert.assertEquals(192, s.bitSet);
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
        Assert.assertEquals(16, res.bitSet);

        res = new ActionSet(Arrays.asList(INSERT)).difference(new ActionSet(Arrays.asList(INSERT, DELETE)));
        Assert.assertTrue(res.contains(DELETE));
        Assert.assertEquals(16, res.bitSet);

        res = new ActionSet(Arrays.asList(INSERT, DELETE)).difference(new ActionSet(Arrays.asList(INSERT)));
        Assert.assertTrue(res.isEmpty());
        Assert.assertEquals(0L, res.bitSet);
    }

    @Test
    public void testCopyConstructor() {
        ActionSet set1 = new ActionSet(Arrays.asList(SELECT, INSERT));
        ActionSet set2 = new ActionSet(set1);
        set2.add(new ActionSet(Arrays.asList(DELETE)));
        Assert.assertEquals(192, set1.bitSet);
        Assert.assertEquals(208, set2.bitSet);
        set1.remove(new ActionSet(Arrays.asList(INSERT)));
        Assert.assertEquals(128, set1.bitSet);
        Assert.assertEquals(208, set2.bitSet);
    }
}
