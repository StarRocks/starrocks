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

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.lookup.Interpolator;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

// 
// don't use trace. use INFO, WARN, ERROR, FATAL
//
public class Log4jConfig extends XmlConfiguration {
    private static final long serialVersionUID = 1L;

    private static String xmlConfTemplateAppenders = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
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
            "  </Appenders>\n";

    // Predefined loggers to write log to file
    private static String xmlConfTemplateFileLoggers =
            "  <Loggers>\n" +
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
            "    <Logger name=\"org.apache.thrift\" level=\"DEBUG\"> \n" +
            "      <AppenderRef ref=\"Sys\"/>\n" +
            "    </Logger>\n" +
            "    <Logger name=\"org.apache.thrift.transport\" level=\"DEBUG\"> \n" +
            "      <AppenderRef ref=\"Sys\"/>\n" +
            "    </Logger>\n" +
            "    <Logger name=\"com.starrocks.thrift\" level=\"DEBUG\"> \n" +
            "      <AppenderRef ref=\"Sys\"/>\n" +
            "    </Logger>\n" +
            "    <Logger name=\"org.apache.kafka\" level=\"WARN\"> \n" +
            "      <AppenderRef ref=\"SysWF\"/>\n" +
            "    </Logger>\n" +
            "    <!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->\n" +
            "  </Loggers>\n" +
            "</Configuration>";

    // Predefined console logger, all logs will be written to console
    private static String xmlConfTemplateConsoleLoggers =
            "  <Loggers>\n" +
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

    private static void reconfig() throws IOException {
        String newXmlConfTemplate = xmlConfTemplateAppenders;
        newXmlConfTemplate += Config.sys_log_to_console ? xmlConfTemplateConsoleLoggers : xmlConfTemplateFileLoggers;

        // sys log config
        String sysLogDir = Config.sys_log_dir;
        String sysRollNum = String.valueOf(Config.sys_log_roll_num);
        String sysDeleteAge = String.valueOf(Config.sys_log_delete_age);

        if (!(sysLogLevel.equalsIgnoreCase("DEBUG") ||
                sysLogLevel.equalsIgnoreCase("INFO") ||
                sysLogLevel.equalsIgnoreCase("WARN") ||
                sysLogLevel.equalsIgnoreCase("ERROR") ||
                sysLogLevel.equalsIgnoreCase("FATAL"))) {
            throw new IOException("sys_log_level config error");
        }

        String sysLogRollPattern = "%d{yyyyMMdd}";
        String sysRollMaxSize = String.valueOf(Config.log_roll_size_mb);
        if (Config.sys_log_roll_interval.equals("HOUR")) {
            sysLogRollPattern = "%d{yyyyMMddHH}";
        } else if (Config.sys_log_roll_interval.equals("DAY")) {
            sysLogRollPattern = "%d{yyyyMMdd}";
        } else {
            throw new IOException("sys_log_roll_interval config error: " + Config.sys_log_roll_interval);
        }

        // audit log config
        String auditLogDir = Config.audit_log_dir;
        String auditLogRollPattern = "%d{yyyyMMdd}";
        String auditRollNum = String.valueOf(Config.audit_log_roll_num);
        String auditRollMaxSize = String.valueOf(Config.log_roll_size_mb);
        String auditDeleteAge = String.valueOf(Config.audit_log_delete_age);
        if (Config.audit_log_roll_interval.equals("HOUR")) {
            auditLogRollPattern = "%d{yyyyMMddHH}";
        } else if (Config.audit_log_roll_interval.equals("DAY")) {
            auditLogRollPattern = "%d{yyyyMMdd}";
        } else {
            throw new IOException("audit_log_roll_interval config error: " + Config.audit_log_roll_interval);
        }

        // dump log config
        String dumpLogDir = Config.dump_log_dir;
        String dumpLogRollPattern = "%d{yyyyMMdd}";
        String dumpRollNum = String.valueOf(Config.dump_log_roll_num);
        String dumpRollMaxSize = String.valueOf(Config.log_roll_size_mb);
        String dumpDeleteAge = String.valueOf(Config.dump_log_delete_age);
        if (Config.dump_log_roll_interval.equals("HOUR")) {
            dumpLogRollPattern = "%d{yyyyMMddHH}";
        } else if (Config.dump_log_roll_interval.equals("DAY")) {
            dumpLogRollPattern = "%d{yyyyMMdd}";
        } else {
            throw new IOException("dump_log_roll_interval config error: " + Config.dump_log_roll_interval);
        }

        // big query log config
        String bigQueryLogDir = Config.big_query_log_dir;
        String bigQueryLogRollPattern = "%d{yyyyMMdd}";
        String bigQueryLogRollNum = String.valueOf(Config.big_query_log_roll_num);
        String bigQueryLogRollMaxSize = String.valueOf(Config.log_roll_size_mb);
        String bigQueryLogDeleteAge = String.valueOf(Config.big_query_log_delete_age);
        if (Config.big_query_log_roll_interval.equals("HOUR")) {
            bigQueryLogRollPattern = "%d{yyyyMMddHH}";
        } else if (Config.big_query_log_roll_interval.equals("DAY")) {
            bigQueryLogRollPattern = "%d{yyyyMMdd}";
        } else {
            throw new IOException("big_query_log_roll_interval config error: " + Config.big_query_log_roll_interval);
        }

        // verbose modules and audit log modules
        StringBuilder sb = new StringBuilder();
        for (String s : verboseModules) {
            sb.append("<Logger name='" + s + "' level='DEBUG'/>");
        }
        for (String s : auditModules) {
            sb.append("<Logger name='audit." + s + "' level='INFO'/>");
        }
        for (String s : dumpModules) {
            sb.append("<Logger name='dump." + s + "' level='INFO'/>");
        }
        for (String s : bigQueryModules) {
            sb.append("<Logger name='big_query." + s + "' level='INFO'/>");
        }

        newXmlConfTemplate = newXmlConfTemplate.replaceAll("<!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->",
                sb.toString());

        Map<String, String> properties = Maps.newHashMap();
        properties.put("sys_log_dir", sysLogDir);
        properties.put("sys_file_pattern", sysLogRollPattern);
        properties.put("sys_roll_maxsize", sysRollMaxSize);
        properties.put("sys_roll_num", sysRollNum);
        properties.put("sys_log_delete_age", sysDeleteAge);
        properties.put("sys_log_level", sysLogLevel);

        properties.put("audit_log_dir", auditLogDir);
        properties.put("audit_file_pattern", auditLogRollPattern);
        properties.put("audit_roll_maxsize", auditRollMaxSize);
        properties.put("audit_roll_num", auditRollNum);
        properties.put("audit_log_delete_age", auditDeleteAge);

        properties.put("dump_log_dir", dumpLogDir);
        properties.put("dump_file_pattern", dumpLogRollPattern);
        properties.put("dump_roll_maxsize", dumpRollMaxSize);
        properties.put("dump_roll_num", dumpRollNum);
        properties.put("dump_log_delete_age", dumpDeleteAge);

        properties.put("big_query_log_dir", bigQueryLogDir);
        properties.put("big_query_file_pattern", bigQueryLogRollPattern);
        properties.put("big_query_roll_maxsize", bigQueryLogRollMaxSize);
        properties.put("big_query_roll_num", bigQueryLogRollNum);
        properties.put("big_query_log_delete_age", bigQueryLogDeleteAge);

        strSub = new StrSubstitutor(new Interpolator(properties));
        newXmlConfTemplate = strSub.replace(newXmlConfTemplate);

        System.out.println("=====");
        System.out.println(newXmlConfTemplate);
        System.out.println("=====");

        // new SimpleLog4jConfiguration with xmlConfTemplate
        ByteArrayInputStream bis = new ByteArrayInputStream(newXmlConfTemplate.getBytes("UTF-8"));
        ConfigurationSource source = new ConfigurationSource(bis);
        Log4jConfig config = new Log4jConfig(source);

        // LoggerContext.start(new Configuration)
        LoggerContext context = (LoggerContext) LogManager.getContext(LogManager.class.getClassLoader(), false);
        context.start(config);
    }

    public static class Tuple<X, Y, Z> {
        public final X x;
        public final Y y;
        public final Z z;

        public Tuple(X x, Y y, Z z) {
            this.x = x;
            this.y = y;
            this.z = z;
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
        reconfig();
    }

    public static synchronized Tuple<String, String[], String[]> updateLogging(
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
        return new Tuple<String, String[], String[]>(sysLogLevel, verboseModules, auditModules);
    }
}
