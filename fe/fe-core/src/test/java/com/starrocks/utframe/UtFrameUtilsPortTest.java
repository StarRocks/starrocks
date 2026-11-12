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

package com.starrocks.utframe;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class UtFrameUtilsPortTest {

    @Test
    public void testParseEphemeralPortLow() {
        Assertions.assertEquals(32768, UtFrameUtils.parseEphemeralPortLow("32768\t60999"));
        Assertions.assertEquals(1024, UtFrameUtils.parseEphemeralPortLow(" 1024 65535\n"));
        // what Files.readAllBytes() returned for "32768\t60999" in procfs
        Assertions.assertEquals(-1, UtFrameUtils.parseEphemeralPortLow("3"));
        Assertions.assertEquals(-1, UtFrameUtils.parseEphemeralPortLow(null));
        Assertions.assertEquals(-1, UtFrameUtils.parseEphemeralPortLow(""));
        Assertions.assertEquals(-1, UtFrameUtils.parseEphemeralPortLow("low high"));
        Assertions.assertEquals(-1, UtFrameUtils.parseEphemeralPortLow("60999 32768"));
        Assertions.assertEquals(-1, UtFrameUtils.parseEphemeralPortLow("0 60999"));
        Assertions.assertEquals(-1, UtFrameUtils.parseEphemeralPortLow("32768 65536"));
    }

    @Test
    public void testReadEphemeralPortRangeFromProcfs() throws Exception {
        Path path = Paths.get(UtFrameUtils.EPHEMERAL_PORT_RANGE_FILE);
        Assumptions.assumeTrue(Files.exists(path), "not Linux");
        List<String> lines = Files.readAllLines(path);
        int low = UtFrameUtils.parseEphemeralPortLow(lines.get(0));
        Assertions.assertTrue(low > 0, "unexpected content: " + lines);

        Assertions.assertEquals(lines.get(0), UtFrameUtils.readEphemeralPortRange(path));
        Assertions.assertEquals(low, UtFrameUtils.getEphemeralPortLow());
    }

    @Test
    public void testFindValidPortAvoidsEphemeralRange() {
        int low = UtFrameUtils.getEphemeralPortLow();
        Assumptions.assumeTrue(UtFrameUtils.MIN_UT_PORT < low, "no port below the ephemeral range starting at " + low);
        int port = UtFrameUtils.findValidPort();
        Assertions.assertTrue(port >= UtFrameUtils.MIN_UT_PORT && port < low,
                "port " + port + " is not in [" + UtFrameUtils.MIN_UT_PORT + ", " + low + ")");
    }
}
