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

public class AbstractServerTest {

    private static class TestServer extends AbstractServer {
        public int startCount = 0;
        public int stopCount = 0;

        @Override
        public void doStart() {
            ++startCount;
        }

        @Override
        public void doStop() {
            ++stopCount;
        }
    }
    @Test
    public void testStartStopIdempotent() {
        TestServer server = new TestServer();
        Assert.assertEquals(0, server.startCount);
        Assert.assertEquals(0, server.stopCount);
        Assert.assertFalse(server.isRunning());

        // start, startCount + 1
        server.start();
        Assert.assertEquals(1, server.startCount);
        Assert.assertEquals(0, server.stopCount);
        Assert.assertTrue(server.isRunning());

        // start again, takes no effect
        server.start();
        Assert.assertEquals(1, server.startCount);
        Assert.assertEquals(0, server.stopCount);
        Assert.assertTrue(server.isRunning());

        // stop, stopCount + 1
        server.stop();
        Assert.assertEquals(1, server.startCount);
        Assert.assertEquals(1, server.stopCount);
        Assert.assertFalse(server.isRunning());

        // stop again, takes no effect
        server.stop();
        Assert.assertEquals(1, server.startCount);
        Assert.assertEquals(1, server.stopCount);
        Assert.assertFalse(server.isRunning());

        // start again
        server.start();
        Assert.assertEquals(2, server.startCount);
        Assert.assertEquals(1, server.stopCount);
        Assert.assertTrue(server.isRunning());

        // stop again
        server.stop();
        Assert.assertEquals(2, server.startCount);
        Assert.assertEquals(2, server.stopCount);
        Assert.assertFalse(server.isRunning());
    }
}
