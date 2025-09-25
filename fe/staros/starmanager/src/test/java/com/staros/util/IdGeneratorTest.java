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


package com.staros.util;

import org.junit.Assert;
import org.junit.Test;

public class IdGeneratorTest {
    @Test
    public void testIdGenerator() {
        IdGenerator idGenerator = new IdGenerator(null);

        // test basic get next id
        long start = idGenerator.getNextId();
        for (int i = 1; i <= IdGenerator.BATCH_ID_INTERVAL; ++i) {
            idGenerator.getNextId();
        }
        long end = idGenerator.getNextId();
        Assert.assertEquals(end, start + IdGenerator.BATCH_ID_INTERVAL + 1);
        Assert.assertEquals(idGenerator.getNextPersistentId(), start + IdGenerator.BATCH_ID_INTERVAL * 2);

        // test set next id
        idGenerator.setNextId(start);
        Assert.assertEquals(end + 1, idGenerator.getNextId());
        long nextId = idGenerator.getNextPersistentId() + IdGenerator.BATCH_ID_INTERVAL;
        idGenerator.setNextId(nextId);
        Assert.assertEquals(nextId, idGenerator.getNextId());
        Assert.assertEquals(nextId + IdGenerator.BATCH_ID_INTERVAL, idGenerator.getNextPersistentId());
    }
}
