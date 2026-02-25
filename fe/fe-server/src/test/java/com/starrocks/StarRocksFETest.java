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

package com.starrocks;

import com.starrocks.common.CommandLineOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StarRocksFETest {

    @Test
    public void testParseArgs() {
        CommandLineOptions options = StarRocksFE.parseArgs(new String[] {"-ht", "IP"});
        Assertions.assertEquals("IP", options.getHostType());
        options = StarRocksFE.parseArgs(new String[] {"--host_type", "FQDN"});
        Assertions.assertEquals("FQDN", options.getHostType());
        options = StarRocksFE.parseArgs(new String[] {"--cluster_snapshot"});
        Assertions.assertTrue(options.isStartFromSnapshot());
        options = StarRocksFE.parseArgs(new String[] {"-rs"});
        Assertions.assertTrue(options.isStartFromSnapshot());
        options = StarRocksFE.parseArgs(new String[] {"--version"});
        Assertions.assertTrue(options.isVersion());
        options = StarRocksFE.parseArgs(new String[] {"-v"});
        Assertions.assertTrue(options.isVersion());
        options = StarRocksFE.parseArgs(new String[] {"--helper", "192.168.3.1:9010"});
        Assertions.assertEquals("192.168.3.1:9010", options.getHelpers());
        options = StarRocksFE.parseArgs(new String[] {"-h", "192.168.3.1:9010"});
        Assertions.assertEquals("192.168.3.1:9010", options.getHelpers());
        options = StarRocksFE.parseArgs(new String[] {"-b", "-l"});
        Assertions.assertNotNull(options.getBdbToolOpts());
        options = StarRocksFE.parseArgs(new String[] {"-bdb", "-l"});
        Assertions.assertNotNull(options.getBdbToolOpts());
    }
}
