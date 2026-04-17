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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.thrift.TFlatJsonConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlatJsonConfig implements Writable {
    @SerializedName("flatJsonEnable")
    private boolean flatJsonEnable;

    @SerializedName("flatJsonNullFactor")
    private double flatJsonNullFactor;

    @SerializedName("flatJsonSparsityFactor")
    private double flatJsonSparsityFactor;

    @SerializedName("flatJsonColumnMax")
    private int flatJsonColumnMax;

    // Paths that must always be flattened, regardless of sparsity.
    // Stored in the internal dot-separated format (no leading "$.").
    @SerializedName("flatJsonColumnPaths")
    private List<String> flatJsonColumnPaths;

    // Upper bound on the number of force-path columns (independent of flatJsonColumnMax).
    // 0 means "use system default" (Config.flat_json_column_paths_max).
    @SerializedName("flatJsonColumnPathsMax")
    private int flatJsonColumnPathsMax;

    public FlatJsonConfig(boolean enabled, double nullFactor, double sparsityFactor, int columnMax) {
        this.flatJsonEnable = enabled;
        this.flatJsonNullFactor = nullFactor;
        this.flatJsonSparsityFactor = sparsityFactor;
        this.flatJsonColumnMax = columnMax;
        this.flatJsonColumnPaths = Collections.emptyList();
        this.flatJsonColumnPathsMax = 0;
    }

    public FlatJsonConfig(FlatJsonConfig config) {
        this.flatJsonEnable = config.getFlatJsonEnable();
        this.flatJsonNullFactor = config.getFlatJsonNullFactor();
        this.flatJsonSparsityFactor = config.getFlatJsonSparsityFactor();
        this.flatJsonColumnMax = config.getFlatJsonColumnMax();
        this.flatJsonColumnPaths = new ArrayList<>(config.getFlatJsonColumnPaths());
        this.flatJsonColumnPathsMax = config.getFlatJsonColumnPathsMax();
    }

    public FlatJsonConfig() {
        this(false, Config.flat_json_null_factor, Config.flat_json_sparsity_factory,
                Config.flat_json_column_max);
    }

    public void buildFromProperties(Map<String, String> properties) {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE)) {
            flatJsonEnable = Boolean.parseBoolean(properties.get(
                    PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR)) {
            flatJsonNullFactor = Double.parseDouble(properties.get(
                    PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR)) {
            flatJsonSparsityFactor = Double.parseDouble(properties.get(
                    PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX)) {
            flatJsonColumnMax = Integer.parseInt(properties.get(
                    PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_PATHS)) {
            flatJsonColumnPaths = PropertyAnalyzer.analyzeFlatJsonColumnPaths(properties);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_PATHS_MAX)) {
            int max = PropertyAnalyzer.analyzeFlatJsonColumnPathsMax(properties);
            if (max >= 0) {
                flatJsonColumnPathsMax = max;
            }
        }
    }

    public boolean getFlatJsonEnable() {
        return flatJsonEnable;
    }

    public void setFlatJsonEnable(boolean flatJsonEnable) {
        this.flatJsonEnable = flatJsonEnable;
    }

    public double getFlatJsonNullFactor() {
        return flatJsonNullFactor;
    }

    public void setFlatJsonNullFactor(double flatJsonNullFactor) {
        this.flatJsonNullFactor = flatJsonNullFactor;
    }

    public double getFlatJsonSparsityFactor() {
        return flatJsonSparsityFactor;
    }

    public void setFlatJsonSparsityFactor(double flatJsonSparsityFactor) {
        this.flatJsonSparsityFactor = flatJsonSparsityFactor;
    }

    public int getFlatJsonColumnMax() {
        return flatJsonColumnMax;
    }

    public void setFlatJsonColumnMax(int flatJsonColumnMax) {
        this.flatJsonColumnMax = flatJsonColumnMax;
    }

    public List<String> getFlatJsonColumnPaths() {
        return flatJsonColumnPaths == null ? Collections.emptyList() : flatJsonColumnPaths;
    }

    public void setFlatJsonColumnPaths(List<String> paths) {
        this.flatJsonColumnPaths = paths == null ? Collections.emptyList() : paths;
    }

    public int getFlatJsonColumnPathsMax() {
        return flatJsonColumnPathsMax;
    }

    public void setFlatJsonColumnPathsMax(int max) {
        this.flatJsonColumnPathsMax = max;
    }

    public Map<String, String> toProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, String.valueOf(flatJsonEnable));

        // Only include other flat JSON properties if flat JSON is enabled
        if (flatJsonEnable) {
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, String.valueOf(flatJsonNullFactor));
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR,
                    String.valueOf(flatJsonSparsityFactor));
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, String.valueOf(flatJsonColumnMax));
            List<String> fps = getFlatJsonColumnPaths();
            if (!fps.isEmpty()) {
                properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_PATHS, String.join(",", fps));
            }
            if (flatJsonColumnPathsMax > 0) {
                properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_PATHS_MAX,
                        String.valueOf(flatJsonColumnPathsMax));
            }
        }
        return properties;
    }

    public TFlatJsonConfig toTFlatJsonConfig() {
        TFlatJsonConfig tFlatJsonConfig = new TFlatJsonConfig();
        tFlatJsonConfig.setFlat_json_enable(flatJsonEnable);
        tFlatJsonConfig.setFlat_json_null_factor(flatJsonNullFactor);
        tFlatJsonConfig.setFlat_json_sparsity_factor(flatJsonSparsityFactor);
        tFlatJsonConfig.setFlat_json_column_max(flatJsonColumnMax);
        List<String> fps = getFlatJsonColumnPaths();
        if (!fps.isEmpty()) {
            tFlatJsonConfig.setFlat_json_column_paths(fps);
        }
        if (flatJsonColumnPathsMax > 0) {
            tFlatJsonConfig.setFlat_json_column_paths_max(flatJsonColumnPathsMax);
        }
        return tFlatJsonConfig;
    }

    @Override
    public String toString() {
        return String.format("{ flat_json_enable : %b,\n " +
                "flat_json_null_factor : %f,\n " +
                "flat_json_sparsity_factor : %f,\n" +
                "flat_json_column_max : %d,\n" +
                "flat_json_column_paths : %s,\n" +
                "flat_json_column_paths_max : %d }",
                flatJsonEnable, flatJsonNullFactor, flatJsonSparsityFactor, flatJsonColumnMax,
                getFlatJsonColumnPaths(), flatJsonColumnPathsMax);
    }
}
