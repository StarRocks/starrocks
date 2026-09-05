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

package com.starrocks.qe;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.starrocks.utframe.StarRocksTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

public class JournalObservableTest extends StarRocksTestBase {
    @Test
    public void testUpperBound() {
        Multiset<JournalObserver> elements = TreeMultiset.create();
        JournalObserver ovserver2 = new JournalObserver(2L);
        JournalObserver ovserver4 = new JournalObserver(4L);
        JournalObserver ovserver41 = new JournalObserver(4L);
        JournalObserver ovserver42 = new JournalObserver(4L);
        JournalObserver ovserver6 = new JournalObserver(6L);

        // empty
        {
            Assertions.assertEquals(0, JournalObservable.upperBound(elements.toArray(), 0, 1L));
        }

        // one element
        {
            elements.add(ovserver2);
            int size = elements.size();
            Assertions.assertEquals(0, JournalObservable.upperBound(elements.toArray(), size, 1L));
            Assertions.assertEquals(1, JournalObservable.upperBound(elements.toArray(), size, 2L));
            Assertions.assertEquals(1, JournalObservable.upperBound(elements.toArray(), size, 3L));
        }

        // same element
        {
            elements.clear();
            elements.add(ovserver2);
            elements.add(ovserver6);
            elements.add(ovserver4);
            elements.add(ovserver41);
            elements.add(ovserver42);

            for (JournalObserver journalObserver : elements) {
                logSysInfo(journalObserver);
            }

            int size = elements.size();
            Assertions.assertEquals(0, JournalObservable.upperBound(elements.toArray(), size, 1L));
            Assertions.assertEquals(1, JournalObservable.upperBound(elements.toArray(), size, 2L));
            Assertions.assertEquals(1, JournalObservable.upperBound(elements.toArray(), size, 3L));
            Assertions.assertEquals(4, JournalObservable.upperBound(elements.toArray(), size, 4L));
            elements.remove(ovserver41);
            Assertions.assertEquals(3, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
            elements.remove(ovserver4);
            Assertions.assertEquals(2, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
            elements.remove(ovserver42);
            Assertions.assertEquals(1, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
        }

        // same element 2
        {
            elements.clear();
            elements.add(ovserver4);
            elements.add(ovserver41);

            int size = elements.size();
            Assertions.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 4L));
            elements.remove(ovserver41);
            Assertions.assertEquals(1, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
            elements.remove(ovserver4);
            Assertions.assertEquals(0, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
        }

        // odd elements
        {
            elements.clear();
            elements.add(ovserver2);
            elements.add(ovserver2);
            elements.add(ovserver4);
            elements.add(ovserver4);
            elements.add(ovserver6);
            elements.add(ovserver6);
            int size = elements.size();
            //            logSysInfo("size=" + size);
            //            for(int i = 0; i < size; i ++) {
            //                logSysInfo("array " + i + " = " + ((MasterOpExecutor)elements.get(i)).getTargetJournalId());
            //            }
            Assertions.assertEquals(0, JournalObservable.upperBound(elements.toArray(), size, 1L));
            Assertions.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 2L));
            Assertions.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 3L));
            Assertions.assertEquals(4, JournalObservable.upperBound(elements.toArray(), size, 4L));
            Assertions.assertEquals(4, JournalObservable.upperBound(elements.toArray(), size, 5L));
            Assertions.assertEquals(6, JournalObservable.upperBound(elements.toArray(), size, 6L));
            Assertions.assertEquals(6, JournalObservable.upperBound(elements.toArray(), size, 7L));
        }
        // even elements
        {
            elements.clear();
            elements.add(ovserver2);
            elements.add(ovserver2);
            elements.add(ovserver4);
            elements.add(ovserver4);
            elements.add(ovserver4);
            elements.add(ovserver6);
            elements.add(ovserver6);
            int size = elements.size();
            Assertions.assertEquals(0, JournalObservable.upperBound(elements.toArray(), size, 1L));
            Assertions.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 2L));
            Assertions.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 3L));
            Assertions.assertEquals(5, JournalObservable.upperBound(elements.toArray(), size, 4L));
            Assertions.assertEquals(5, JournalObservable.upperBound(elements.toArray(), size, 5L));
            Assertions.assertEquals(7, JournalObservable.upperBound(elements.toArray(), size, 6L));
            Assertions.assertEquals(7, JournalObservable.upperBound(elements.toArray(), size, 7L));
        }
        {
            CountDownLatch latch = new CountDownLatch(1);
            logSysInfo(latch.getCount());

            latch.countDown();
            logSysInfo(latch.getCount());

            latch.countDown();
            logSysInfo(latch.getCount());

            latch.countDown();
            logSysInfo(latch.getCount());
        }
        logSysInfo("success");
    }
}

