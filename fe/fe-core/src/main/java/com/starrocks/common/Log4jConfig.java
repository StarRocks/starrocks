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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Log4jConfig.java

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

package com.starrocks.common;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import groovy.lang.Tuple3;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.lookup.Interpolator;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

// 
// don't use trace. use INFO, WARN, ERROR, FATAL
public class Log4jConfig extends XmlConfiguration {
    private static final Set<String> DEBUG_LEVELS = ImmutableSet.of("FATAL", "ERROR", "WARN", "INFO", "DEBUG");

    private static final String APPENDER_TEMPLATE = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
            "\n" +
            "<Configuration status=\"info\" packages=\"com.starrocks.common\">\n" +
            "  <Appenders>\n" +
            "    <Console name=\"ConsoleErr\" target=\"SYSTEM_ERR\" follow=\"true\">\n" +
            "      <PatternLayout pattern=\"%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid) [%C{1}.%M():%L] %m%n\"/>\n" +
            "    </Console>\n" +
            "    <RollingFile name=\"Sys\" fileName=\"${sys_log_dir}/fe.log\" filePattern=\"${sys_log_dir}/fe.log.${sys_file_pattern}-%i\">\n" +
            "      <PatternLayout charset=\"UTF-8\">\n" +
            "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid) [%C{1}.%M():%L] %m%n</Pattern>\n" +
            "      </PatternLayout>\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${sys_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${sys_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.log.*\" />\n" +
            "          <IfLastModified age=\"${sys_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +
            "    <RollingFile name=\"SysWF\" fileName=\"${sys_log_dir}/fe.warn.log\" filePattern=\"${sys_log_dir}/fe.warn.log.${sys_file_pattern}-%i\">\n" +
            "      <PatternLayout charset=\"UTF-8\">\n" +
            "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid) [%C{1}.%M():%L] %m%n</Pattern>\n" +
            "      </PatternLayout>\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${sys_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${sys_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.warn.log.*\" />\n" +
            "          <IfLastModified age=\"${sys_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +
            "    <RollingFile name=\"Auditfile\" fileName=\"${audit_log_dir}/fe.audit.log\" filePattern=\"${audit_log_dir}/fe.audit.log.${audit_file_pattern}-%i\">\n" +
            "      <PatternLayout charset=\"UTF-8\">\n" +
            "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} [%c{1}] %m%n</Pattern>\n" +
            "      </PatternLayout>\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${audit_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${audit_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.audit.log.*\" />\n" +
            "          <IfLastModified age=\"${audit_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +
            "    <RollingFile name=\"dumpFile\" fileName=\"${dump_log_dir}/fe.dump.log\" filePattern=\"${dump_log_dir}/fe.dump.log.${dump_file_pattern}-%i\">\n" +
            "      <PatternLayout charset=\"UTF-8\">\n" +
            "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} [%c{1}] %m%n</Pattern>\n" +
            "      </PatternLayout>\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${dump_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${dump_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${dump_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.dump.log.*\" />\n" +
            "          <IfLastModified age=\"${dump_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +
            "    <RollingFile name=\"BigQueryFile\" fileName=\"${big_query_log_dir}/fe.big_query.log\" filePattern=\"${big_query_log_dir}/fe.big_query.log.${big_query_file_pattern}-%i\">\n" +
            "      <PatternLayout charset=\"UTF-8\">\n" +
            "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} [%c{1}] %m%n</Pattern>\n" +
            "      </PatternLayout>\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${big_query_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${big_query_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.big_query.log.*\" />\n" +
            "          <IfLastModified age=\"${big_query_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +
            "    <RollingFile name=\"InternalFile\" fileName=\"${internal_log_dir}/fe.internal.log\" filePattern=\"${internal_log_dir}/fe.internal.log.${internal_file_pattern}-%i\">\n" +
            "      <PatternLayout charset=\"UTF-8\">\n" +
            "        <Pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %p (%t|%tid) [%C{1}.%M():%L] %m%n</Pattern>\n" +
            "      </PatternLayout>\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${internal_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${sys_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${internal_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.internal.log.*\" />\n" +
            "          <IfLastModified age=\"${internal_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +
            "  </Appenders>\n";

    // Predefined loggers to write log to file
    private static final String FILE_LOGGER_TEMPLATE = "  <Loggers>\n" +
            "    <Root level=\"${sys_log_level}\">\n" +
            "      <AppenderRef ref=\"Sys\"/>\n" +
            "      <AppenderRef ref=\"SysWF\" level=\"WARN\"/>\n" +
            "    </Root>\n" +
            "    <Logger name=\"audit\" level=\"ERROR\" additivity=\"false\">\n" +
            "      <AppenderRef ref=\"Auditfile\"/>\n" +
            "    </Logger>\n" +
            "    <Logger name=\"dump\" level=\"ERROR\" additivity=\"false\">\n" +
            "      <AppenderRef ref=\"dumpFile\"/>\n" +
            "    </Logger>\n" +
            "    <Logger name=\"big_query\" level=\"ERROR\" additivity=\"false\">\n" +
            "      <AppenderRef ref=\"BigQueryFile\"/>\n" +
            "    </Logger>\n" +
            "    <Logger name=\"org.apache.kafka\" level=\"WARN\"> \n" +
            "      <AppenderRef ref=\"SysWF\"/>\n" +
            "    </Logger>\n" +
            "    <!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->\n" +
            "  </Loggers>\n" +
            "</Configuration>";

    // Predefined console logger, all logs will be written to console
    private static final String CONSOLE_LOGGER_TEMPLATE = "  <Loggers>\n" +
            "    <Root level=\"${sys_log_level}\">\n" +
            "      <AppenderRef ref=\"ConsoleErr\"/>\n" +
            "    </Root>\n" +
            "    <!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->\n" +
            "  </Loggers>\n" +
            "</Configuration>";
    private static StrSubstitutor strSub;
    private static String sysLogLevel;
    private static String[] verboseModules;
    private static String[] auditModules;
    private static String[] dumpModules;
    private static String[] bigQueryModules;
    private static String[] internalModules;

    private static void reconfig() throws IOException {
        Map<String, String> properties = Maps.newHashMap();

        // sys log config
        if (!DEBUG_LEVELS.contains(StringUtils.upperCase(sysLogLevel))) {
            throw new IOException("sys_log_level config error");
        }
        properties.put("sys_log_dir", Config.sys_log_dir);
        properties.put("sys_roll_maxsize", String.valueOf(Config.log_roll_size_mb));
        properties.put("sys_roll_num", String.valueOf(Config.sys_log_roll_num));
        properties.put("sys_log_delete_age", String.valueOf(Config.sys_log_delete_age));
        properties.put("sys_log_level", sysLogLevel);
        properties.put("sys_file_pattern", getIntervalPattern("sys_log_roll_interval", Config.sys_log_roll_interval));

        // audit log config
        properties.put("audit_log_dir", Config.audit_log_dir);
        properties.put("audit_roll_maxsize", String.valueOf(Config.log_roll_size_mb));
        properties.put("audit_roll_num", String.valueOf(Config.audit_log_roll_num));
        properties.put("audit_log_delete_age", String.valueOf(Config.audit_log_delete_age));
        properties.put("audit_file_pattern",
                getIntervalPattern("audit_log_roll_interval", Config.audit_log_roll_interval));

        // dump log config
        properties.put("dump_log_dir", Config.dump_log_dir);
        properties.put("dump_roll_maxsize", String.valueOf(Config.log_roll_size_mb));
        properties.put("dump_roll_num", String.valueOf(Config.dump_log_roll_num));
        properties.put("dump_log_delete_age", String.valueOf(Config.dump_log_delete_age));
        properties.put("dump_file_pattern",
                getIntervalPattern("dump_log_roll_interval", Config.dump_log_roll_interval));

        // big query log config
        properties.put("big_query_log_dir", Config.big_query_log_dir);
        properties.put("big_query_roll_maxsize", String.valueOf(Config.log_roll_size_mb));
        properties.put("big_query_roll_num", String.valueOf(Config.big_query_log_roll_num));
        properties.put("big_query_log_delete_age", String.valueOf(Config.big_query_log_delete_age));
        properties.put("big_query_file_pattern",
                getIntervalPattern("big_query_log_roll_interval", Config.big_query_log_roll_interval));

        // internal log config
        properties.put("internal_log_dir", Config.internal_log_dir);
        properties.put("internal_roll_maxsize", String.valueOf(Config.log_roll_size_mb));
        properties.put("internal_roll_num", String.valueOf(Config.internal_log_roll_num));
        properties.put("internal_log_delete_age", String.valueOf(Config.internal_log_delete_age));
        properties.put("internal_file_pattern",
                getIntervalPattern("big_query_log_roll_interval", Config.internal_log_roll_interval));

        String xmlConfTemplate = generateXmlConfTemplate();
        strSub = new StrSubstitutor(new Interpolator(properties));
        xmlConfTemplate = strSub.replace(xmlConfTemplate);

        if (!FeConstants.runningUnitTest && !FeConstants.isReplayFromQueryDump) {
            System.out.println("=====");
            System.out.println(xmlConfTemplate);
            System.out.println("=====");
        }

        // new SimpleLog4jConfiguration with xmlConfTemplate
        ByteArrayInputStream bis = new ByteArrayInputStream(xmlConfTemplate.getBytes(StandardCharsets.UTF_8));
        ConfigurationSource source = new ConfigurationSource(bis);
        Log4jConfig config = new Log4jConfig(source);

        // LoggerContext.start(new Configuration)
        LoggerContext context = (LoggerContext) LogManager.getContext(LogManager.class.getClassLoader(), false);
        context.start(config);
    }

    private static String generateXmlConfTemplate() {
        // verbose modules and audit log modules
        StringBuilder sb = new StringBuilder();

        for (String s : internalModules) {
            sb.append("<Logger name='internal.").append(s).append("' level=\"INFO\"> \n");
            sb.append("   <AppenderRef ref=\"InternalFile\"/>\n");
            sb.append("</Logger>\n");
        }

        for (String s : verboseModules) {
            sb.append("<Logger name='").append(s).append("' level='DEBUG'/>");
        }
        for (String s : auditModules) {
            sb.append("<Logger name='audit.").append(s).append("' level='INFO'/>");
        }
        for (String s : dumpModules) {
            sb.append("<Logger name='dump.").append(s).append("' level='INFO'/>");
        }
        for (String s : bigQueryModules) {
            sb.append("<Logger name='big_query.").append(s).append("' level='INFO'/>");
        }

        String newXmlConfTemplate = APPENDER_TEMPLATE;
        newXmlConfTemplate += Config.sys_log_to_console ? CONSOLE_LOGGER_TEMPLATE : FILE_LOGGER_TEMPLATE;
        newXmlConfTemplate = newXmlConfTemplate.replaceAll("<!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->",
                sb.toString());
        return newXmlConfTemplate;
    }

    private static String getIntervalPattern(String name, String config) throws IOException {
        if (config.equalsIgnoreCase("HOUR")) {
            return "%d{yyyyMMddHH}";
        } else if (config.equalsIgnoreCase("DAY")) {
            return "%d{yyyyMMdd}";
        } else {
            throw new IOException(name + " config error: " + config);
        }
    }

    @Override
    public StrSubstitutor getStrSubstitutor() {
        return strSub;
    }

    public Log4jConfig(final ConfigurationSource configSource) {
        super(LoggerContext.getContext(), configSource);
    }

    public static synchronized void initLogging() throws IOException {
        sysLogLevel = Config.sys_log_level;
        verboseModules = Config.sys_log_verbose_modules;
        auditModules = Config.audit_log_modules;
        dumpModules = Config.dump_log_modules;
        bigQueryModules = Config.big_query_log_modules;
        internalModules = Config.internal_log_modules;
        reconfig();
    }

    public static synchronized Tuple3<String, String[], String[]> updateLogging(
            String level, String[] verboseNames, String[] auditNames) throws IOException {
        boolean toReconfig = false;
        if (level != null) {
            sysLogLevel = level;
            toReconfig = true;
        }
        if (verboseNames != null) {
            verboseModules = verboseNames;
            toReconfig = true;
        }
        if (auditNames != null) {
            auditModules = auditNames;
            toReconfig = true;
        }
        if (toReconfig) {
            reconfig();
        }
        return new Tuple3<>(sysLogLevel, verboseModules, auditModules);
    }
}
