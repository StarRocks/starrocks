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

package com.starrocks.connector.config;

import com.starrocks.common.exception.AnalysisException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

public abstract class ConnectorConfig {
    private static final Logger LOG = LogManager.getLogger(ConnectorConfig.class);

    protected Map<String, String> properties;

    public void loadConfig(Map<String, String> properties) {
        this.properties = properties;

        Field[] fields = this.getClass().getDeclaredFields();

        Arrays.stream(fields).forEach(field -> {
            if (field.isAnnotationPresent(Config.class)) {
                Config config = field.getAnnotation(Config.class);
                String key = config.key();
                String value = properties.get(key);
                String defaultValue = config.defaultValue();
                boolean nullable = config.nullable();
                String trimValue = getTrimOrDefault(value, defaultValue);
                try {
                    if (!nullable && StringUtils.isEmpty(trimValue)) {
                        throw new AnalysisException("key: " + key + " in properties can't null when create connector");
                    }
                    // reflection only handle public attr
                    field.setAccessible(true);
                    // extract real value from properties
                    if (Integer.class.equals(field.getType()) || int.class.equals(field.getType())) {
                        field.setInt(this, Integer.parseInt(trimValue));
                    } else if (Long.class.equals(field.getType())) {
                        field.setLong(this, Long.parseLong(trimValue));
                    } else if (String.class.equals(field.getType())) {
                        field.set(this, trimValue);
                    } else if (Boolean.class.equals(field.getType()) || boolean.class.equals(field.getType())) {
                        field.set(this, Boolean.valueOf(trimValue));
                    } else if (String[].class.equals(field.getType())) {
                        field.set(this, trimValue.split(","));
                    } else {
                        throw new AnalysisException("Unexpected value: " + field.getType());
                    }
                    // use default value overwrite properties
                    properties.put(key, trimValue);
                } catch (AnalysisException | IllegalAccessException e) {
                    LOG.error("load Config error, Unexpected value:" + field.getType());
                }
            }
        });
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    // use trim value, if null return defaultValue
    private String getTrimOrDefault(String value, String defaultValue) {
        return (StringUtils.isEmpty(value) || StringUtils.isEmpty(value.trim())) ? defaultValue : value.trim();
    }

}
