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

import com.google.gson.JsonObject;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Log4jConfigTest {
    private static final Logger LOG = LogManager.getLogger(Log4jConfigTest.class);

    private String logFormat;

    @BeforeEach
    public void setUp() throws IOException {
        FeConstants.runningUnitTest = true;
        logFormat = Config.sys_log_format;
        Log4jConfig.initLogging();
    }

    @AfterEach
    public void tearDown() {
        Config.sys_log_format = logFormat;
        FeConstants.runningUnitTest = false;
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
            Assertions.assertFalse(xmlConfig.contains("<JsonTemplateLayout"));
            Assertions.assertTrue(xmlConfig.contains("<PatternLayout"));
            // no unresolved variable
            Matcher matcher = Pattern.compile(regStr, Pattern.MULTILINE).matcher(xmlConfig);
            if (matcher.find()) {
                name = matcher.group(1);
                Assertions.fail(String.format("Unexpected of unresolved variables:'%s' in the final xmlConfig", name));
            }
        }
        // check the json configuration
        Config.sys_log_format = "json";
        {
            String xmlConfig = Log4jConfig.generateActiveLog4jXmlConfig();
            Assertions.assertTrue(xmlConfig.contains("<JsonTemplateLayout"));
            Assertions.assertFalse(xmlConfig.contains("<PatternLayout"));
            // no unresolved variable
            Matcher matcher = Pattern.compile(regStr, Pattern.MULTILINE).matcher(xmlConfig);
            if (matcher.find()) {
                name = matcher.group(1);
                Assertions.fail(String.format("Unexpected of unresolved variables:'%s' in the final xmlConfig", name));
            }
        }
    }

    @Test
    public void testJsonLogOutputFormat() throws IOException {
        // enable logging with json format to console
        // with `sys_log_to_console = true`, the log will be written to System.err
        Config.sys_log_format = "json";
        Config.sys_log_to_console = true;
        Config.sys_log_level = "INFO";
        Log4jConfig.initLogging();

        String logMessage = "Test log message from unit test";
        // Hook System.err
        PrintStream oldErr = System.err;
        ByteArrayOutputStream byteOs = new ByteArrayOutputStream();
        System.setErr(new PrintStream(byteOs, true));
        // log a message
        LOG.warn(logMessage);
        // reset System.out
        System.setErr(oldErr);
        // reset logging
        Config.sys_log_format = "plaintext";
        Config.sys_log_to_console = false;
        Log4jConfig.initLogging();

        // validate log message
        String logMsg = byteOs.toString(Charset.defaultCharset()).trim();
        System.out.println("logMsg: " + logMsg);

        Reader reader = new InputStreamReader(new ByteArrayInputStream(byteOs.toByteArray()));
        JsonObject jsonObject = Streams.parse(new JsonReader(reader)).getAsJsonObject();

        Assertions.assertEquals("testJsonLogOutputFormat", jsonObject.get("method").getAsString());
        Assertions.assertEquals("Log4jConfigTest.java", jsonObject.get("file").getAsString());
        Assertions.assertEquals("WARN", jsonObject.get("level").getAsString());
        Assertions.assertEquals("main", jsonObject.get("thread.name").getAsString());
        Assertions.assertEquals(logMessage, jsonObject.get("message").getAsString());
    }

    @Test
    public void testRolloverFileIndexAndDeleteCount() throws IOException {
        Config.sys_log_format = "plaintext";
        String oldFileIndex = Config.profile_log_roll_file_index;
        int oldDeleteCount = Config.profile_log_delete_count;
        try {
            // default: preserve historical behavior (min file index, no accumulated-count cap)
            Config.profile_log_roll_file_index = "min";
            Config.profile_log_delete_count = -1;
            String xmlConfig = Log4jConfig.generateActiveLog4jXmlConfig();
            Assertions.assertTrue(xmlConfig.contains("<DefaultRolloverStrategy max=\"5\" fileIndex=\"min\">"));
            Assertions.assertTrue(xmlConfig.contains("<IfFileName glob=\"fe.profile.log.*\" />"));
            Assertions.assertFalse(xmlConfig.contains("<IfAccumulatedFileCount"),
                    "no accumulated-count cap should be emitted when delete_count <= 0");

            // opt-in: nomax file index with a hard retention cap
            Config.profile_log_roll_file_index = "nomax";
            Config.profile_log_delete_count = 3;
            xmlConfig = Log4jConfig.generateActiveLog4jXmlConfig();
            Assertions.assertTrue(xmlConfig.contains("fileIndex=\"nomax\""));
            Assertions.assertTrue(xmlConfig.contains("<IfAccumulatedFileCount exceeds=\"3\" />"));
            Assertions.assertTrue(xmlConfig.contains("<IfAny>"));

            // invalid file index is rejected
            Config.profile_log_roll_file_index = "bogus";
            Config.profile_log_delete_count = -1;
            try {
                Log4jConfig.generateActiveLog4jXmlConfig();
                Assertions.fail("Expected IOException for invalid fileIndex");
            } catch (IOException e) {
                Assertions.assertTrue(e.getMessage().contains("profile_log_roll_file_index config error"));
            }
        } finally {
            Config.profile_log_roll_file_index = oldFileIndex;
            Config.profile_log_delete_count = oldDeleteCount;
        }
    }
}
