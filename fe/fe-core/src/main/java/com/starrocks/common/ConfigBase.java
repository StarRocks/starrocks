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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigBase {
    private static final Logger LOG = LogManager.getLogger(ConfigBase.class);

    @Retention(RetentionPolicy.RUNTIME)
    public static @interface ConfField {
        boolean mutable() default false;

        String comment() default "";

        /**
         * alias for a configuration defined in Config, use for compatibility reason.
         * when changing a configuration name, you can put the old name in alias annotation.<p>
         * usage: @ConfField(alias = {"old_name1", "old_name2"})
         *
         * @return an array of alias names
         */
        String[] aliases() default {};
    }

    protected Properties props;
    protected static Field[] configFields;
    protected static Map<String, Field> allMutableConfigs = new HashMap<>();

    public void init(String propFile) throws Exception {
        configFields = this.getClass().getFields();
        initAllMutableConfigs();
        props = new Properties();
        FileReader reader = null;
        try {
            reader = new FileReader(propFile);
            props.load(reader);
        } catch (Exception e) {
            throw e;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        replacedByEnv();
        setFields();
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
                        throw new Exception("unknown type: " + f.getType().getSimpleName());
                }
            } else {
                map.put(f.getName(), f.get(null).toString());
            }
        }
        return map;
    }

    private void replacedByEnv() throws Exception {
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
                    throw new Exception("no such env variable: " + m.group(1));
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

    public static void setConfigField(Field f, String confVal) throws IllegalAccessException, Exception {
        confVal = confVal.trim();

        String[] sa = confVal.split(",");
        for (int i = 0; i < sa.length; i++) {
            sa[i] = sa[i].trim();
        }

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
                short[] sha = new short[sa.length];
                for (int i = 0; i < sha.length; i++) {
                    sha[i] = Short.parseShort(sa[i]);
                }
                f.set(null, sha);
                break;
            case "int[]":
                int[] ia = new int[sa.length];
                for (int i = 0; i < ia.length; i++) {
                    ia[i] = Integer.parseInt(sa[i]);
                }
                f.set(null, ia);
                break;
            case "long[]":
                long[] la = new long[sa.length];
                for (int i = 0; i < la.length; i++) {
                    la[i] = Long.parseLong(sa[i]);
                }
                f.set(null, la);
                break;
            case "double[]":
                double[] da = new double[sa.length];
                for (int i = 0; i < da.length; i++) {
                    da[i] = Double.parseDouble(sa[i]);
                }
                f.set(null, da);
                break;
            case "boolean[]":
                boolean[] ba = new boolean[sa.length];
                for (int i = 0; i < ba.length; i++) {
                    ba[i] = Boolean.parseBoolean(sa[i]);
                }
                f.set(null, ba);
                break;
            case "String[]":
                f.set(null, sa);
                break;
            default:
                throw new Exception("unknown type: " + f.getType().getSimpleName());
        }
    }

    public static synchronized void setMutableConfig(String key, String value) throws DdlException {
        Field field = allMutableConfigs.get(key);
        if (field == null) {
            throw new DdlException("Config '" + key + "' does not exist or is not mutable");
        }

        try {
            ConfigBase.setConfigField(field, value);
        } catch (Exception e) {
            throw new DdlException("Failed to set config '" + key + "'. err: " + e.getMessage());
        }

        LOG.info("set config {} to {}", key, value);
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

    public static synchronized List<List<String>> getConfigInfo(PatternMatcher matcher) throws DdlException {
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
                            throw new DdlException("Unknown type: " + f.getType().getSimpleName());
                    }
                } else {
                    confVal = String.valueOf(f.get(null));
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new DdlException("Failed to get config '" + confKey + "'. err: " + e.getMessage());
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
}
