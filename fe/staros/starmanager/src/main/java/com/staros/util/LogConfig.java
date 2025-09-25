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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.lookup.Interpolator;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

public class LogConfig extends XmlConfiguration {
    private static String xmlConfTemplate = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
            "\n" +
            "<Configuration status=\"info\" packages=\"com.staros.util\">\n" +
            "  <Appenders>\n" +
            "    <RollingFile name=\"starmgr\" fileName=\"${log_dir}/starmgr.log\"" +
            " filePattern=\"${log_dir}/starmgr.log.${log_file_pattern}-%i\">\n" +
            "      <PatternLayout charset=\"UTF-8\">\n" +
            "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid) [%C{1}.%M():%L] %m%n</Pattern>\n" +
            "      </PatternLayout>\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"100MB\"/>\n" +
            "      </Policies>\n" +
            "    </RollingFile>\n" +
            "  </Appenders>\n" +
            "  <Loggers>\n" +
            "    <Root level=\"${log_level}\">\n" +
            "      <AppenderRef ref=\"starmgr\"/>\n" +
            "    </Root>\n" +
            "  </Loggers>\n" +
            "</Configuration>";
    private static StrSubstitutor strSub;

    public static void initLogging() {
        String newXmlConfTemplate = xmlConfTemplate;
        String logDir = Config.LOG_DIR;
        String logLevel = Config.LOG_LEVEL;
        String logFilePattern = "%d{yyyyMMdd}";

        if (!(logLevel.equalsIgnoreCase("DEBUG") ||
                logLevel.equalsIgnoreCase("INFO") ||
                logLevel.equalsIgnoreCase("WARN") ||
                logLevel.equalsIgnoreCase("ERROR"))) {
            System.out.println("log_level config error, should be DEBUG or INFO or WARN or ERROR.");
            System.exit(-1);
        }

        Map<String, String> properties = new HashMap<>();
        properties.put("log_dir", logDir);
        properties.put("log_level", logLevel);
        properties.put("log_file_pattern", logFilePattern);

        strSub = new StrSubstitutor(new Interpolator(properties));
        newXmlConfTemplate = strSub.replace(newXmlConfTemplate);

        // new SimpleLog4jConfiguration with newXmlConfTemplate
        ByteArrayInputStream bais = null;
        ConfigurationSource source = null;
        try {
            bais = new ByteArrayInputStream(newXmlConfTemplate.getBytes("UTF-8"));
            source = new ConfigurationSource(bais);
        } catch (Exception e) {
            System.out.println(String.format("caught exception when config logging, %s.", e));
            System.exit(-1);
        }

        LogConfig config = new LogConfig(source);

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.start(config);
    }

    public LogConfig(ConfigurationSource configSource) {
        super(LoggerContext.getContext(), configSource);
    }

    @Override
    public StrSubstitutor getStrSubstitutor() {
        return strSub;
    }
}
