// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.starrocks.common.lock;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.sql.StatementPlanner;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestLockInterface {

    @Test
    public void testReentrantReadWriteTryLock() {
        List<Database> dbs = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            dbs.add(new Database(i, "db" + i));
        }
        {
            Assert.assertTrue(StatementPlanner.tryLockDatabases(dbs, 10, TimeUnit.MILLISECONDS));
            Assert.assertTrue(StatementPlanner.tryLockDatabases(dbs, 10, TimeUnit.MILLISECONDS));
            StatementPlanner.unlockDatabases(dbs);
            StatementPlanner.unlockDatabases(dbs);
        }

        {
            new MockUp<Database>() {
                @Mock
                public boolean tryReadLock(long timeout, TimeUnit unit) {
                    return false;
                }
            };
            Assert.assertFalse(StatementPlanner.tryLockDatabases(dbs, 10, TimeUnit.MILLISECONDS));
        }
    }
}
