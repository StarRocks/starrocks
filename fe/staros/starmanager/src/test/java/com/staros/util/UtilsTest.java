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

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.staros.util.Utils.executeNoExceptionOrDie;

public class UtilsTest {
    @Test
    public void testExecutionNoException() {
        AtomicInteger a = new AtomicInteger();
        Runnable runnable = a::incrementAndGet;
        executeNoExceptionOrDie(runnable);
        Assert.assertEquals(1, a.get());
    }

    @Test
    public void testExecutionExceptionAndDie() {
        final String msg = "Throw exception message";
        AtomicInteger a = new AtomicInteger();
        AtomicInteger exitCount = new AtomicInteger();
        Runnable runnable = () -> {
            a.incrementAndGet();
            throw new RuntimeException(msg);
        };
        Assert.assertThrows(Throwable.class, runnable::run);
        Assert.assertEquals(1L, a.get());
        new MockUp<System>() {
            @Mock
            public void exit(int status) {
                exitCount.incrementAndGet();
                throw new RuntimeException(String.valueOf(status));
            }
        };
        RuntimeException exception =
                Assert.assertThrows(RuntimeException.class, () -> executeNoExceptionOrDie(runnable));
        Assert.assertEquals("-1", exception.getMessage());
        Assert.assertEquals(2L, a.get());
        Assert.assertEquals(1L, exitCount.get());
    }
}
