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
import org.slf4j.bridge.SLF4JBridgeHandler;

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
        Assertions.assertEquals(logMessage, jsonObject.get("message").getAsString());
    }

    @Test
    public void testJulToSlf4jBridge() throws IOException {
        Config.sys_log_format = "plaintext";
        Config.sys_log_to_console = true;
        Config.sys_log_level = "INFO";

        Log4jConfig.initLogging();

        PrintStream oldErr = System.err;
        ByteArrayOutputStream byteOs = new ByteArrayOutputStream();
        System.setErr(new PrintStream(byteOs, true));

        try {
            String julMessage = "This is a message from java.util.logging";
            java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger("com.starrocks.test.jul");
            julLogger.info(julMessage);

            String capturedOutput = byteOs.toString(Charset.defaultCharset());

            Assertions.assertTrue(capturedOutput.contains(julMessage),
                    "The JUL log message should be captured by Log4j2. Captured: " + capturedOutput);
            Assertions.assertTrue(capturedOutput.contains("INFO"), "Log output should contain level info");

        } finally {
            System.setErr(oldErr);
            Config.sys_log_to_console = false;
            SLF4JBridgeHandler.uninstall();
        }
    }

    @Test
    public void testJulLevelMapping() throws IOException {

        java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");

        try {
            Log4jConfig.updateLogging("DEBUG", null, null);
            Assertions.assertEquals(java.util.logging.Level.FINE, rootLogger.getLevel());

            Log4jConfig.updateLogging("WARN", null, null);
            Assertions.assertEquals(java.util.logging.Level.WARNING, rootLogger.getLevel());

            Log4jConfig.updateLogging("ERROR", null, null);
            Assertions.assertEquals(java.util.logging.Level.SEVERE, rootLogger.getLevel());

            Log4jConfig.updateLogging("FATAL", null, null);
            Assertions.assertEquals(java.util.logging.Level.SEVERE, rootLogger.getLevel());

            Log4jConfig.updateLogging("INFO", null, null);
            Assertions.assertEquals(java.util.logging.Level.INFO, rootLogger.getLevel());

            try {
                Log4jConfig.updateLogging("TRACE", null, null);
                Assertions.fail("Expected IOException was not thrown for unsupported TRACE level");
            } catch (IOException e) {
                Assertions.assertTrue(e.getMessage().contains("sys_log_level config error"));
            }
            Assertions.assertEquals(java.util.logging.Level.INFO, rootLogger.getLevel());

        } finally {
            Log4jConfig.updateLogging("INFO", null, null);
        }
    }
}
