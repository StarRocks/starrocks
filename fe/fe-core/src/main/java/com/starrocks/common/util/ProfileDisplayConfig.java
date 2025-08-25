// Copyright 2025-present StarRocks, Inc. All rights reserved.
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

package com.starrocks.common.util;

/**
 * Configuration class for query profile display options.
 * Provides backward compatibility and allows customization of readability features.
 * Implements requirements from issue #60952.
 */
public class ProfileDisplayConfig {
    
    // Default settings for backward compatibility
    private static final boolean DEFAULT_USE_TIERED_DISPLAY = false; // Backward compatibility
    private static final boolean DEFAULT_SHOW_ZERO_VALUES = true;   // Backward compatibility  
    private static final boolean DEFAULT_SHOW_RULE_OF_THUMB = true;
    private static final boolean DEFAULT_GROUP_METRICS = true;
    private static final boolean DEFAULT_USE_FRIENDLY_NAMES = true;
    private static final boolean DEFAULT_SHOW_DISTRIBUTIONS = true;
    private static final TieredProfileFormatter.MetricTier DEFAULT_MAX_TIER = TieredProfileFormatter.MetricTier.ADVANCED;
    
    private final boolean useTieredDisplay;
    private final boolean showZeroValues;
    private final boolean showRuleOfThumb;
    private final boolean groupMetrics;
    private final boolean useFriendlyNames;
    private final boolean showDistributions;
    private final TieredProfileFormatter.MetricTier maxTier;
    
    public ProfileDisplayConfig() {
        this(DEFAULT_USE_TIERED_DISPLAY, DEFAULT_SHOW_ZERO_VALUES, DEFAULT_SHOW_RULE_OF_THUMB,
             DEFAULT_GROUP_METRICS, DEFAULT_USE_FRIENDLY_NAMES, DEFAULT_SHOW_DISTRIBUTIONS, DEFAULT_MAX_TIER);
    }
    
    public ProfileDisplayConfig(boolean useTieredDisplay, boolean showZeroValues, boolean showRuleOfThumb,
                               boolean groupMetrics, boolean useFriendlyNames, boolean showDistributions,
                               TieredProfileFormatter.MetricTier maxTier) {
        this.useTieredDisplay = useTieredDisplay;
        this.showZeroValues = showZeroValues;
        this.showRuleOfThumb = showRuleOfThumb;
        this.groupMetrics = groupMetrics;
        this.useFriendlyNames = useFriendlyNames;
        this.showDistributions = showDistributions;
        this.maxTier = maxTier;
    }
    
    // Builder pattern for easy configuration
    public static class Builder {
        private boolean useTieredDisplay = DEFAULT_USE_TIERED_DISPLAY;
        private boolean showZeroValues = DEFAULT_SHOW_ZERO_VALUES;
        private boolean showRuleOfThumb = DEFAULT_SHOW_RULE_OF_THUMB;
        private boolean groupMetrics = DEFAULT_GROUP_METRICS;
        private boolean useFriendlyNames = DEFAULT_USE_FRIENDLY_NAMES;
        private boolean showDistributions = DEFAULT_SHOW_DISTRIBUTIONS;
        private TieredProfileFormatter.MetricTier maxTier = DEFAULT_MAX_TIER;
        
        public Builder useTieredDisplay(boolean useTieredDisplay) {
            this.useTieredDisplay = useTieredDisplay;
            return this;
        }
        
        public Builder showZeroValues(boolean showZeroValues) {
            this.showZeroValues = showZeroValues;
            return this;
        }
        
        public Builder showRuleOfThumb(boolean showRuleOfThumb) {
            this.showRuleOfThumb = showRuleOfThumb;
            return this;
        }
        
        public Builder groupMetrics(boolean groupMetrics) {
            this.groupMetrics = groupMetrics;
            return this;
        }
        
        public Builder useFriendlyNames(boolean useFriendlyNames) {
            this.useFriendlyNames = useFriendlyNames;
            return this;
        }
        
        public Builder showDistributions(boolean showDistributions) {
            this.showDistributions = showDistributions;
            return this;
        }
        
        public Builder maxTier(TieredProfileFormatter.MetricTier maxTier) {
            this.maxTier = maxTier;
            return this;
        }
        
        public ProfileDisplayConfig build() {
            return new ProfileDisplayConfig(useTieredDisplay, showZeroValues, showRuleOfThumb,
                                          groupMetrics, useFriendlyNames, showDistributions, maxTier);
        }
    }
    
    // Predefined configurations for common use cases
    public static ProfileDisplayConfig createBasicConfig() {
        return new Builder()
                .useTieredDisplay(true)
                .maxTier(TieredProfileFormatter.MetricTier.BASIC)
                .showZeroValues(false)
                .showRuleOfThumb(true)
                .groupMetrics(true)
                .useFriendlyNames(true)
                .showDistributions(false)
                .build();
    }
    
    public static ProfileDisplayConfig createAdvancedConfig() {
        return new Builder()
                .useTieredDisplay(true)
                .maxTier(TieredProfileFormatter.MetricTier.ADVANCED)
                .showZeroValues(false)
                .showRuleOfThumb(true)
                .groupMetrics(true)
                .useFriendlyNames(true)
                .showDistributions(true)
                .build();
    }
    
    public static ProfileDisplayConfig createTraceConfig() {
        return new Builder()
                .useTieredDisplay(true)
                .maxTier(TieredProfileFormatter.MetricTier.TRACE)
                .showZeroValues(true)
                .showRuleOfThumb(false)
                .groupMetrics(true)
                .useFriendlyNames(true)
                .showDistributions(true)
                .build();
    }
    
    public static ProfileDisplayConfig createLegacyConfig() {
        return new Builder()
                .useTieredDisplay(false)
                .showZeroValues(true)
                .showRuleOfThumb(false)
                .groupMetrics(false)
                .useFriendlyNames(false)
                .showDistributions(false)
                .build();
    }
    
    // Getters
    public boolean isUseTieredDisplay() { return useTieredDisplay; }
    public boolean isShowZeroValues() { return showZeroValues; }
    public boolean isShowRuleOfThumb() { return showRuleOfThumb; }
    public boolean isGroupMetrics() { return groupMetrics; }
    public boolean isUseFriendlyNames() { return useFriendlyNames; }
    public boolean isShowDistributions() { return showDistributions; }
    public TieredProfileFormatter.MetricTier getMaxTier() { return maxTier; }
    
    /**
     * Parse tier string to enum
     */
    public static TieredProfileFormatter.MetricTier parseTier(String tierStr) {
        if (tierStr == null) return DEFAULT_MAX_TIER;
        
        switch (tierStr.toLowerCase()) {
            case "basic": return TieredProfileFormatter.MetricTier.BASIC;
            case "advanced": return TieredProfileFormatter.MetricTier.ADVANCED;
            case "trace": return TieredProfileFormatter.MetricTier.TRACE;
            default: return DEFAULT_MAX_TIER;
        }
    }
    
    /**
     * Create config from HTTP request parameters for backward compatibility
     */
    public static ProfileDisplayConfig fromRequestParams(String tier, String format) {
        // Maintain backward compatibility - only use new features if explicitly requested
        boolean useNewFormat = "tiered".equals(format) || tier != null;
        
        if (!useNewFormat) {
            return createLegacyConfig();
        }
        
        TieredProfileFormatter.MetricTier tierEnum = parseTier(tier);
        
        switch (tierEnum) {
            case BASIC:
                return createBasicConfig();
            case TRACE:
                return createTraceConfig();
            case ADVANCED:
            default:
                return createAdvancedConfig();
        }
    }
}