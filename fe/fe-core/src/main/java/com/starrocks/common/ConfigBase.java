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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/ConfigBase.java

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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.Util;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigBase {
    private static final Logger LOG = LogManager.getLogger(ConfigBase.class);

    public static final String AUTHENTICATION_CHAIN_MECHANISM_NATIVE = "native";

    @Retention(RetentionPolicy.RUNTIME)
    public @interface ConfField {
        boolean mutable() default false;

        String comment() default "";

        /**
         * alias for a configuration defined in Config, used for compatibility reason.
         * when changing a configuration name, you can put the old name in alias annotation.
         * <p>
         * usage: @ConfField(alias = {"old_name1", "old_name2"})
         *
         * @return an array of alias names
         */
        String[] aliases() default {};
    }

    protected Properties props;
    private static boolean isPersisted = false;
    private static String configPath;
    protected static Field[] configFields;
    protected static Map<String, Field> allMutableConfigs = new HashMap<>();

    public void init(String propFile) throws Exception {
        configPath = propFile;
        configFields = this.getClass().getFields();
        initAllMutableConfigs();
        props = new Properties();
        try (FileReader reader = new FileReader(propFile)) {
            props.load(reader);
        }
        if (Files.isWritable(Path.of(propFile)) && !Util.isRunningInContainer()) {
            isPersisted = true;
        }

        replacedByEnv();
        setFields();
    }

    public static boolean isIsPersisted() {
        return isPersisted;
    }

    public static void initAllMutableConfigs() {
        for (Field field : configFields) {
            ConfField confField = field.getAnnotation(ConfField.class);
            if (confField == null || !confField.mutable()) {
                continue;
            }
            allMutableConfigs.put(field.getName(), field);
            for (String aliasName : confField.aliases()) {
                allMutableConfigs.put(aliasName, field);
            }
        }
    }

    public static Map<String, Field> getAllMutableConfigs() {
        return allMutableConfigs;
    }

    public static HashMap<String, String> dump() throws Exception {
        HashMap<String, String> map = new HashMap<String, String>();
        Field[] fields = configFields;
        for (Field f : fields) {
            if (f.getAnnotation(ConfField.class) == null) {
                continue;
            }
            if (f.getType().isArray()) {
                switch (f.getType().getSimpleName()) {
                    case "short[]":
                        map.put(f.getName(), Arrays.toString((short[]) f.get(null)));
                        break;
                    case "int[]":
                        map.put(f.getName(), Arrays.toString((int[]) f.get(null)));
                        break;
                    case "long[]":
                        map.put(f.getName(), Arrays.toString((long[]) f.get(null)));
                        break;
                    case "double[]":
                        map.put(f.getName(), Arrays.toString((double[]) f.get(null)));
                        break;
                    case "boolean[]":
                        map.put(f.getName(), Arrays.toString((boolean[]) f.get(null)));
                        break;
                    case "String[]":
                        map.put(f.getName(), Arrays.toString((String[]) f.get(null)));
                        break;
                    default:
                        throw new InvalidConfException("unknown type: " + f.getType().getSimpleName());
                }
            } else {
                map.put(f.getName(), f.get(null).toString());
            }
        }
        return map;
    }

    private void replacedByEnv() throws InvalidConfException {
        Pattern pattern = Pattern.compile("\\$\\{([^\\}]*)\\}");
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            Matcher m = pattern.matcher(value);
            while (m.find()) {
                String envValue = System.getProperty(m.group(1));
                envValue = (envValue != null) ? envValue : System.getenv(m.group(1));
                if (envValue != null) {
                    value = value.replace("${" + m.group(1) + "}", envValue);
                } else {
                    throw new InvalidConfException("no such env variable: " + m.group(1));
                }
            }
            props.setProperty(key, value);
        }
    }

    public String getConfigValue(String confKey, String[] aliases) {
        String confVal = props.getProperty(confKey);
        if (Strings.isNullOrEmpty(confVal)) {
            for (String aliasName : aliases) {
                confVal = props.getProperty(aliasName);
                if (!Strings.isNullOrEmpty(confVal)) {
                    break;
                }
            }
        }

        return confVal;
    }

    private void setFields() throws Exception {
        Field[] fields = configFields;
        for (Field f : fields) {
            // ensure that field has "@ConfField" annotation
            ConfField anno = f.getAnnotation(ConfField.class);
            if (anno == null) {
                continue;
            }

            // ensure that field has property string
            String confVal = getConfigValue(f.getName(), anno.aliases());
            if (Strings.isNullOrEmpty(confVal)) {
                continue;
            }

            setConfigField(f, confVal);
        }
    }

    private static void validateConfValue(Field f, String[] arrayArgs, String confVal)
            throws InvalidConfException {
        switch (f.getName()) {
            case "authentication_chain":
                Set<String> argsSet = new HashSet<>(Arrays.asList(arrayArgs));
                if (!f.getType().equals(String[].class)
                        || argsSet.size() != arrayArgs.length
                        || !argsSet.contains(AUTHENTICATION_CHAIN_MECHANISM_NATIVE)) {
                    throw new InvalidConfException("'authentication_chain' configuration invalid, " +
                            "'native' must be in the list, and cannot have duplicates, current value: "
                            + confVal);
                }
                break;
            default:
                break;
        }
    }

    public static void setConfigField(Field f, String confVal) throws Exception {
        confVal = confVal.trim();
        boolean isEmpty = confVal.isEmpty();

        String[] sa = confVal.split(",");
        for (int i = 0; i < sa.length; i++) {
            sa[i] = sa[i].trim();
        }

        validateConfValue(f, sa, confVal);

        // set config field
        switch (f.getType().getSimpleName()) {
            case "short":
                f.setShort(null, Short.parseShort(confVal));
                break;
            case "int":
                f.setInt(null, Integer.parseInt(confVal));
                break;
            case "long":
                f.setLong(null, Long.parseLong(confVal));
                break;
            case "double":
                f.setDouble(null, Double.parseDouble(confVal));
                break;
            case "boolean":
                f.setBoolean(null, Boolean.parseBoolean(confVal));
                break;
            case "String":
                f.set(null, confVal);
                break;
            case "short[]":
                short[] sha = isEmpty ? new short[0] : new short[sa.length];
                for (int i = 0; i < sha.length; i++) {
                    sha[i] = Short.parseShort(sa[i]);
                }
                f.set(null, sha);
                break;
            case "int[]":
                int[] ia = isEmpty ? new int[0] : new int[sa.length];
                for (int i = 0; i < ia.length; i++) {
                    ia[i] = Integer.parseInt(sa[i]);
                }
                f.set(null, ia);
                break;
            case "long[]":
                long[] la = isEmpty ? new long[0] : new long[sa.length];
                for (int i = 0; i < la.length; i++) {
                    la[i] = Long.parseLong(sa[i]);
                }
                f.set(null, la);
                break;
            case "double[]":
                double[] da = isEmpty ? new double[0] : new double[sa.length];
                for (int i = 0; i < da.length; i++) {
                    da[i] = Double.parseDouble(sa[i]);
                }
                f.set(null, da);
                break;
            case "boolean[]":
                boolean[] ba = isEmpty ? new boolean[0] : new boolean[sa.length];
                for (int i = 0; i < ba.length; i++) {
                    ba[i] = Boolean.parseBoolean(sa[i]);
                }
                f.set(null, ba);
                break;
            case "String[]":
                f.set(null, isEmpty ? new String[0] : sa);
                break;
            default:
                throw new InvalidConfException("unknown type: " + f.getType().getSimpleName());
        }
    }

    public static synchronized void setMutableConfig(String key, String value,
                                                     boolean isPersisted, String userIdentity) throws InvalidConfException {
        if (isPersisted) {
            if (!ConfigBase.isIsPersisted()) {
                String errMsg = "set persisted config failed, because current running mode is not persisted";
                LOG.warn(errMsg);
                throw new InvalidConfException(errMsg);
            }

            try {
                appendPersistedProperties(key, value, userIdentity);
            } catch (IOException e) {
                throw new InvalidConfException("Failed to set config '" + key + "'. err: " + e.getMessage());
            }
        }

        Field field = allMutableConfigs.get(key);
        if (field == null) {
            throw new InvalidConfException(ErrorCode.ERROR_CONFIG_NOT_EXIST, key);
        }

        try {
            ConfigBase.setConfigField(field, value);
        } catch (Exception e) {
            throw new InvalidConfException("Failed to set config '" + key + "'. err: " + e.getMessage());
        }

        LOG.info("set config {} to {}", key, value);
    }

    private static void appendPersistedProperties(String key, String value, String userIdentity) throws IOException {
        Properties props = new Properties();
        Path path = Paths.get(configPath);
        List<String> lines = Files.readAllLines(path);
        try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
            props.load(reader);
        }

        String oldValue = props.getProperty(key);
        String comment;
        if (StringUtils.isEmpty(oldValue)) {
            comment = String.format("# The user %s added %s=%s at %s", userIdentity, key, value,
                    LocalDateTime.now().format(DateUtils.DATE_TIME_FORMATTER_UNIX));
        } else {
            comment = String.format("# The user %s changed %s to %s at %s", userIdentity, oldValue, value,
                    LocalDateTime.now().format(DateUtils.DATE_TIME_FORMATTER_UNIX));
        }

        boolean keyExists = false;
        // Keep the original configuration file format
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(configPath))) {
            for (String s : lines) {
                String line = s.trim();

                // Compatible with key=value & key = value
                if (line.matches("^" + key + "\\s*=\\s*.*$")) {
                    keyExists = true;
                    writer.write(comment);
                    writer.newLine();
                    writer.write(key + " = " + value);
                    writer.newLine();
                    continue;
                }

                writer.write(s);
                writer.newLine();
            }

            if (!keyExists) {
                writer.newLine();
                writer.write(comment);
                writer.newLine();
                writer.write(key + " = " + value);
                writer.newLine();
            }
        }
    }

    private static boolean isAliasesMatch(PatternMatcher matcher, String[] aliases) {
        if (matcher == null) {
            return true;
        }

        for (String aliasName : aliases) {
            if (matcher.match(aliasName)) {
                return true;
            }
        }

        return false;
    }

    public static synchronized List<List<String>> getConfigInfo(PatternMatcher matcher) throws InvalidConfException {
        List<List<String>> configs = Lists.newArrayList();
        Field[] fields = configFields;
        for (Field f : fields) {
            List<String> config = Lists.newArrayList();
            ConfField anno = f.getAnnotation(ConfField.class);
            if (anno == null) {
                continue;
            }

            String confKey = f.getName();
            // If the alias match here, we also show the config
            if (matcher != null && !matcher.match(confKey) && !isAliasesMatch(matcher, anno.aliases())) {
                continue;
            }
            String confVal;
            try {
                if (f.getType().isArray()) {
                    switch (f.getType().getSimpleName()) {
                        case "short[]":
                            confVal = Arrays.toString((short[]) f.get(null));
                            break;
                        case "int[]":
                            confVal = Arrays.toString((int[]) f.get(null));
                            break;
                        case "long[]":
                            confVal = Arrays.toString((long[]) f.get(null));
                            break;
                        case "double[]":
                            confVal = Arrays.toString((double[]) f.get(null));
                            break;
                        case "boolean[]":
                            confVal = Arrays.toString((boolean[]) f.get(null));
                            break;
                        case "String[]":
                            confVal = Arrays.toString((String[]) f.get(null));
                            break;
                        default:
                            throw new InvalidConfException("Unknown type: " + f.getType().getSimpleName());
                    }
                } else {
                    confVal = String.valueOf(f.get(null));
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new InvalidConfException("Failed to get config '" + confKey + "'. err: " + e.getMessage());
            }

            config.add(confKey);
            config.add(Arrays.toString(anno.aliases()));
            config.add(Strings.nullToEmpty(confVal));
            config.add(f.getType().getSimpleName());
            config.add(String.valueOf(anno.mutable()));
            config.add(anno.comment());
            configs.add(config);
        }

        return configs;
    }

    public static synchronized void setConfig(AdminSetConfigStmt stmt) throws DdlException {
        String user = ConnectContext.get().getCurrentUserIdentity().getUser();
        setFrontendConfig(stmt.getConfig().getMap(), stmt.isPersistent(), user);
        List<Frontend> allFrontends = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null);
        int timeout = ConnectContext.get().getExecTimeout() * 1000 + Config.thrift_rpc_timeout_ms;
        StringBuilder errMsg = new StringBuilder();
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode().first)) {
                continue;
            }
            errMsg.append(callFrontNodeSetConfig(stmt, fe, timeout, errMsg));
        }
        if (!errMsg.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_SET_CONFIG_FAILED, errMsg.toString());
        }
    }

    private static synchronized StringBuilder callFrontNodeSetConfig(AdminSetConfigStmt stmt, Frontend fe, int timeout,
                                                                     StringBuilder errMsg) {
        TSetConfigRequest request = new TSetConfigRequest();
        request.setKeys(Lists.newArrayList(stmt.getConfig().getKey()));
        request.setValues(Lists.newArrayList(stmt.getConfig().getValue()));
        request.setIs_persistent(stmt.isPersistent());
        request.setUser_identity(ConnectContext.get().getCurrentUserIdentity().getUser());
        try {
            TSetConfigResponse response = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.frontendPool,
                    new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                    timeout,
                    client -> client.setConfig(request));
            TStatus status = response.getStatus();
            if (status.getStatus_code() != TStatusCode.OK) {
                errMsg.append("set config for fe[").append(fe.getHost()).append("] failed: ");
                if (status.getError_msgs() != null && status.getError_msgs().size() > 0) {
                    errMsg.append(String.join(",", status.getError_msgs()));
                }
                errMsg.append(";");
            }
        } catch (Exception e) {
            LOG.warn("set remote fe: {} config failed", fe.getHost(), e);
            errMsg.append("set config for fe[").append(fe.getHost()).append("] failed: ").append(e.getMessage());
        }
        return errMsg;
    }

    public static synchronized void setFrontendConfig(Map<String, String> configs, boolean isPersisted, String userIdentity)
            throws InvalidConfException {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            ConfigBase.setMutableConfig(entry.getKey(), entry.getValue(), isPersisted, userIdentity);
        }
    }
}
