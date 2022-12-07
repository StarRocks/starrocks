// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.config;

import com.google.common.base.Strings;
import com.starrocks.sql.analyzer.SemanticException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * ConfigProperty describes a configuration property. It contains the configuration
 * key, deprecated older versions of the key, and an optional default value for the configuration
 * @param <T> The type of the default value.
 */
public class ConfigProperty<T> implements Serializable {

    private final String key;

    private final T defaultValue;

    private final String doc;

    private final Optional<String> sinceVersion;

    private final Optional<String> deprecatedVersion;

    private final Set<T> validValues;

    public static final String EMPTY_STRING = "";

    ConfigProperty(String key, T defaultValue, String doc, Optional<String> sinceVersion,
                   Optional<String> deprecatedVersion, Set<T> validValues) {
        this.key = Objects.requireNonNull(key);
        this.defaultValue = defaultValue;
        this.doc = doc;
        this.sinceVersion = sinceVersion;
        this.deprecatedVersion = deprecatedVersion;
        this.validValues = validValues;
    }

    public String key() {
        return key;
    }

    public T defaultValue() {
        if (defaultValue == null) {
            throw new SemanticException("There's no default value for this config");
        }
        return defaultValue;
    }

    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    public String doc() {
        return Strings.isNullOrEmpty(doc) ? EMPTY_STRING : doc;
    }


    public T checkValues(T value) {
        if (validValues != null && !validValues.isEmpty() && !validValues.contains(value)) {
            throw new SemanticException(
                    "The value of " + key + " should be one of "
                            + String.join(",", validValues.stream()
                            .map(String::valueOf).collect((Collectors.toList()))) + ", but was: " + value);
        }
        return value;
    }

    public ConfigProperty<T> withValidValues(T... validValues) {
        Objects.requireNonNull(validValues);
        return new ConfigProperty<>(key, defaultValue, doc, sinceVersion,
                deprecatedVersion, new HashSet<>(Arrays.asList(validValues)));
    }

    public ConfigProperty<T> withDocumentation(String doc) {
        Objects.requireNonNull(doc);
        return new ConfigProperty<>(key, defaultValue, doc, sinceVersion, deprecatedVersion, validValues);
    }

    public ConfigProperty<T> sinceVersion(String sinceVersion) {
        Objects.requireNonNull(sinceVersion);
        return new ConfigProperty<>(key, defaultValue, doc, Optional.of(sinceVersion), deprecatedVersion, validValues);
    }

    /**
     * Create a OptionBuilder with key.
     *
     * @param key The key of the option
     * @return Return a OptionBuilder.
     */
    public static PropertyBuilder key(String key) {
        Objects.requireNonNull(key);
        return new PropertyBuilder(key);
    }

    /**
     * The PropertyBuilder is used to build the ConfigProperty.
     */
    public static final class PropertyBuilder {

        private final String key;

        PropertyBuilder(String key) {
            this.key = key;
        }

        public <T> ConfigProperty<T> defaultValue(T value) {
            Objects.requireNonNull(value);
            ConfigProperty<T> configProperty = new ConfigProperty<>(key, value, "",
                    Optional.empty(), Optional.empty(), Collections.emptySet());
            return configProperty;
        }

        public ConfigProperty<String> noDefaultValue() {
            ConfigProperty<String> configProperty = new ConfigProperty<>(key, null, "",
                    Optional.empty(), Optional.empty(), Collections.emptySet());
            return configProperty;
        }
    }
}