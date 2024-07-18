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

package com.starrocks.common;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Log4jConfigTest {
    private String logFormat;

    @Before
    public void setUp() throws IOException {
        logFormat = Config.sys_log_format;
        Log4jConfig.initLogging();
    }

    @After
    public void tearDown() {
        Config.sys_log_format = logFormat;
    }

    @Test
    public void testJsonLoggingFormatConfig() throws IOException {
        // ${([^}]*)}
        String regStr = "\\$\\{([^\\}]*)\\}";
        String name = "";
        // default plaintext configuration
        Config.sys_log_format = "plaintext";
        {
            String xmlConfig = Log4jConfig.generateActiveLog4jXmlConfig();
            Assert.assertFalse(xmlConfig.contains("<JSONLayout"));
            Assert.assertTrue(xmlConfig.contains("<PatternLayout"));
            // no unresolved variable
            Matcher matcher = Pattern.compile(regStr, Pattern.MULTILINE).matcher(xmlConfig);
            if (matcher.find()) {
                name = matcher.group(1);
                Assert.fail(String.format("Unexpected of unresolved variables:'%s' in the final xmlConfig", name));
            }
        }
        // check the json configuration
        Config.sys_log_format = "json";
        {
            String xmlConfig = Log4jConfig.generateActiveLog4jXmlConfig();
            Assert.assertTrue(xmlConfig.contains("<JSONLayout"));
            Assert.assertFalse(xmlConfig.contains("<PatternLayout"));
            // no unresolved variable
            Matcher matcher = Pattern.compile(regStr, Pattern.MULTILINE).matcher(xmlConfig);
            if (matcher.find()) {
                name = matcher.group(1);
                Assert.fail(String.format("Unexpected of unresolved variables:'%s' in the final xmlConfig", name));
            }
        }
    }
}
