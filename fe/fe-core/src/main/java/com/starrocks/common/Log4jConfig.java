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

import com.google.common.annotations.VisibleForTesting;
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
            "      ${syslog_default_layout}\n" +
            "    </Console>\n" +
            "    <RollingFile name=\"Sys\" fileName=\"${sys_log_dir}/fe.log\" filePattern=\"${sys_log_dir}/fe.log.${sys_file_pattern}-%i${sys_file_postfix}\">\n" +
            "      ${syslog_default_layout}\n" +
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
            "    <RollingFile name=\"SysWF\" fileName=\"${sys_log_dir}/fe.warn.log\" filePattern=\"${sys_log_dir}/fe.warn.log.${sys_file_pattern}-%i${sys_file_postfix}\">\n" +
            "      ${syslog_warning_layout}\n" +
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
            "    <RollingFile name=\"Auditfile\" fileName=\"${audit_log_dir}/fe.audit.log\" filePattern=\"${audit_log_dir}/fe.audit.log.${audit_file_pattern}-%i${audit_file_postfix}\">\n" +
            "      ${syslog_audit_layout}\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${audit_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${audit_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${audit_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.audit.log.*\" />\n" +
            "          <IfLastModified age=\"${audit_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +
            "    <RollingFile name=\"dumpFile\" fileName=\"${dump_log_dir}/fe.dump.log\" filePattern=\"${dump_log_dir}/fe.dump.log.${dump_file_pattern}-%i\">\n" +
            "      ${syslog_dump_layout}\n" +
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
            "      ${syslog_bigquery_layout}\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${big_query_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${big_query_log_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${big_query_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.big_query.log.*\" />\n" +
            "          <IfLastModified age=\"${big_query_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +

            "    <RollingFile name=\"ProfileFile\" fileName=\"${profile_log_dir}/fe.profile.log\" filePattern=\"${profile_log_dir}/fe.profile.log.${profile_file_pattern}-%i\">\n" +
            "      ${syslog_profile_layout}\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${profile_log_roll_size_mb}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${profile_log_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${profile_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.profile.log.*\" />\n" +
            "          <IfLastModified age=\"${profile_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +

            "    <RollingFile name=\"FeaturesFile\" fileName=\"${profile_log_dir}/fe.features.log\" " +
            "       filePattern=\"${profile_log_dir}/fe.features.log.${feature_file_pattern}-%i\">\n" +
            "      ${syslog_profile_layout}\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${feature_log_roll_size_mb}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${feature_log_roll_num}\" fileIndex=\"min\">\n" +
            "        <Delete basePath=\"${feature_log_dir}/\" maxDepth=\"1\" followLinks=\"true\">\n" +
            "          <IfFileName glob=\"fe.features.log.*\" />\n" +
            "          <IfLastModified age=\"${feature_log_delete_age}\" />\n" +
            "        </Delete>\n" +
            "      </DefaultRolloverStrategy>\n" +
            "    </RollingFile>\n" +

            "    <RollingFile name=\"InternalFile\" fileName=\"${internal_log_dir}/fe.internal.log\" filePattern=\"${internal_log_dir}/fe.internal.log.${internal_file_pattern}-%i\">\n" +
            "      ${syslog_default_layout}\n" +
            "      <Policies>\n" +
            "        <TimeBasedTriggeringPolicy/>\n" +
            "        <SizeBasedTriggeringPolicy size=\"${internal_roll_maxsize}MB\"/>\n" +
            "      </Policies>\n" +
            "      <DefaultRolloverStrategy max=\"${internal_log_roll_num}\" fileIndex=\"min\">\n" +
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
            "    <Logger name=\"profile\" level=\"INFO\" additivity=\"false\">\n" +
            "      <AppenderRef ref=\"ProfileFile\"/>\n" +
            "    </Logger>\n" +
            "    <Logger name=\"features\" level=\"INFO\" additivity=\"false\">\n" +
            "      <AppenderRef ref=\"FeaturesFile\"/>\n" +
            "    </Logger>\n" +
            "<!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->" +
            "  </Loggers>\n" +
            "</Configuration>";

    // Predefined console logger, all logs will be written to console
    private static final String CONSOLE_LOGGER_TEMPLATE = "  <Loggers>\n" +
            "    <Root level=\"${sys_log_level}\">\n" +
            "      <AppenderRef ref=\"ConsoleErr\"/>\n" +
            "    </Root>\n" +
            "<!--REPLACED BY AUDIT AND VERBOSE MODULE NAMES-->" +
            "  </Loggers>\n" +
            "</Configuration>";
    private static StrSubstitutor strSub;
    private static String sysLogLevel;
    private static String[] verboseModules;
    private static String[] auditModules;
    private static String[] dumpModules;
    private static String[] bigQueryModules;
    private static String[] internalModules;
    private static boolean compressSysLog;
    private static boolean compressAuditLog;
    private static String[] warnModules;
    private static String[] builtinWarnModules = {
            "org.apache.kafka",
            "org.apache.hudi",
            "org.apache.hadoop.io.compress",
    };

    @VisibleForTesting
    static String generateActiveLog4jXmlConfig() throws IOException {
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
        properties.put("sys_file_postfix", compressSysLog ? ".gz" : "");
        properties.put("audit_file_postfix", compressAuditLog ? ".gz" : "");

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
        properties.put("big_query_log_roll_num", String.valueOf(Config.big_query_log_roll_num));
        properties.put("big_query_log_delete_age", String.valueOf(Config.big_query_log_delete_age));
        properties.put("big_query_file_pattern",
                getIntervalPattern("big_query_log_roll_interval", Config.big_query_log_roll_interval));

        // profile log config
        properties.put("profile_log_dir", Config.profile_log_dir);
        properties.put("profile_log_roll_size_mb", String.valueOf(Config.profile_log_roll_size_mb));
        properties.put("profile_log_roll_num", String.valueOf(Config.profile_log_roll_num));
        properties.put("profile_log_delete_age", String.valueOf(Config.profile_log_delete_age));
        properties.put("profile_file_pattern",
                getIntervalPattern("profile_log_roll_interval", Config.profile_log_roll_interval));

        // feature log config
        properties.put("feature_log_dir", Config.feature_log_dir);
        properties.put("feature_log_roll_size_mb", String.valueOf(Config.feature_log_roll_size_mb));
        properties.put("feature_log_roll_num", String.valueOf(Config.feature_log_roll_num));
        properties.put("feature_log_delete_age", String.valueOf(Config.feature_log_delete_age));
        properties.put("feature_file_pattern",
                getIntervalPattern("feature_log_roll_interval", Config.feature_log_roll_interval));

        // internal log config
        properties.put("internal_log_dir", Config.internal_log_dir);
        properties.put("internal_roll_maxsize", String.valueOf(Config.log_roll_size_mb));
        properties.put("internal_log_roll_num", String.valueOf(Config.internal_log_roll_num));
        properties.put("internal_log_delete_age", String.valueOf(Config.internal_log_delete_age));
        properties.put("internal_file_pattern",
                getIntervalPattern("big_query_log_roll_interval", Config.internal_log_roll_interval));

        // appender layout
        final String jsonLoggingConfValue = "json";
        if (jsonLoggingConfValue.equalsIgnoreCase(Config.sys_log_format)) {
            // json logging, use `'` and replace them to `"` in batch to avoid too many escapes
            String jsonConfig =
                    "{'@timestamp':{'$resolver':'timestamp','pattern':{'format':'yyyy-MM-dd HH:mm:ss.SSSXXX','timeZone':'UTC'}}," +
                            "'level':{'$resolver':'level','field':'name'}," +
                            "'thread.name':{'$resolver':'thread','field':'name'}," +
                            "'thread.id':{'$resolver':'thread','field':'id'}," +
                            "'line':{'$resolver':'source','field':'lineNumber'}," +
                            "'file':{'$resolver':'source','field':'fileName'}," +
                            "'method':{'$resolver':'source','field':'methodName'}," +
                            "'message':{'$resolver':'message','stringfield':'true'}," +
                            "'exception':{'$resolver':'exception','field':'stackTrace','stackTrace':{'stringified':true,'full':true}}}";
            jsonConfig = jsonConfig.replace("'", "\"");
            String jsonLayoutFormatter = "<JsonTemplateLayout maxStringLength=\"%d\" locationInfoEnabled=\"true\">\n" +
                    "<EventTemplate><![CDATA[" + jsonConfig + "]]></EventTemplate>\n" + "</JsonTemplateLayout>";
            String jsonLayoutDefault = String.format(jsonLayoutFormatter, Config.sys_log_json_max_string_length);
            String jsonLayoutProfile = String.format(jsonLayoutFormatter, Config.sys_log_json_profile_max_string_length);
            properties.put("syslog_default_layout", jsonLayoutDefault);
            properties.put("syslog_warning_layout", jsonLayoutDefault);
            properties.put("syslog_audit_layout", jsonLayoutDefault);
            properties.put("syslog_dump_layout", jsonLayoutDefault);
            properties.put("syslog_bigquery_layout", jsonLayoutDefault);
            properties.put("syslog_profile_layout", jsonLayoutProfile);
        } else {
            // fallback to plaintext logging
            properties.put("syslog_default_layout",
                    "<PatternLayout charset=\"UTF-8\" pattern=\"%d{yyyy-MM-dd HH:mm:ss.SSSXXX} %p (%t|%tid) [%C{1}.%M():%L] %m%n\"/>");
            properties.put("syslog_warning_layout",
                    "<PatternLayout charset=\"UTF-8\" pattern=\"%d{yyyy-MM-dd HH:mm:ss.SSSXXX} %p (%t|%tid) [%C{1}.%M():%L] %m%n %ex\"/>");
            properties.put("syslog_audit_layout",
                    "<PatternLayout charset=\"UTF-8\" pattern=\"%d{yyyy-MM-dd HH:mm:ss.SSSXXX} [%c{1}] %m%n\"/>");
            properties.put("syslog_dump_layout",
                    "<PatternLayout charset=\"UTF-8\" pattern=\"%d{yyyy-MM-dd HH:mm:ss.SSSXXX} [%c{1}] %m%n\"/>");
            properties.put("syslog_bigquery_layout",
                    "<PatternLayout charset=\"UTF-8\" pattern=\"%d{yyyy-MM-dd HH:mm:ss.SSSXXX} [%c{1}] %m%n\"/>");
            properties.put("syslog_profile_layout",
                    "<PatternLayout charset=\"UTF-8\" pattern=\"%d{yyyy-MM-dd HH:mm:ss.SSSXXX} [%c{1}] %m%n\"/>");
        }

        String xmlConfTemplate = generateXmlConfTemplate();
        strSub = new StrSubstitutor(new Interpolator(properties));
        return strSub.replace(xmlConfTemplate);
    }

    private static void reconfig() throws IOException {
        String xmlConfTemplate = generateActiveLog4jXmlConfig();
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
        boolean log2Console = Config.sys_log_to_console;
        // verbose modules and audit log modules
        StringBuilder sb = new StringBuilder();

        for (String s : internalModules) {
            sb.append("    <Logger name='internal.").append(s).append("' level=\"INFO\" additivity=\"false\"> \n");
            if (log2Console) {
                sb.append("      <AppenderRef ref=\"ConsoleErr\"/>\n");
            } else {
                sb.append("      <AppenderRef ref=\"InternalFile\"/>\n");
            }
            sb.append("    </Logger>\n");
        }

        for (String s : verboseModules) {
            sb.append("    <Logger name='").append(s).append("' level='DEBUG'/>\n");
        }
        for (String s : auditModules) {
            sb.append("    <Logger name='audit.").append(s).append("' level='INFO'/>\n");
        }
        for (String s : dumpModules) {
            sb.append("    <Logger name='dump.").append(s).append("' level='INFO'/>\n");
        }
        for (String s : bigQueryModules) {
            sb.append("    <Logger name='big_query.").append(s).append("' level='INFO'/>\n");
        }
        for (String s : builtinWarnModules) {
            sb.append("    <Logger name='").append(s).append("' level='WARN'><AppenderRef ref='SysWF'/></Logger>\n");
        }
        for (String s : warnModules) {
            sb.append("    <Logger name='").append(s).append("' level='WARN'><AppenderRef ref='SysWF'/></Logger>\n");
        }

        String newXmlConfTemplate = APPENDER_TEMPLATE;
        newXmlConfTemplate += log2Console ? CONSOLE_LOGGER_TEMPLATE : FILE_LOGGER_TEMPLATE;
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
        compressSysLog = Config.sys_log_enable_compress;
        compressAuditLog = Config.audit_log_enable_compress;
        warnModules = Config.sys_log_warn_modules;
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
