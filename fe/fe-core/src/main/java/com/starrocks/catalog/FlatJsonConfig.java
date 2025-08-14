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

import java.util.HashMap;
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

    public FlatJsonConfig(boolean enabled, double nullFactor, double sparsityFactor, int columnMax) {
        this.flatJsonEnable = enabled;
        this.flatJsonNullFactor = nullFactor;
        this.flatJsonSparsityFactor = sparsityFactor;
        this.flatJsonColumnMax = columnMax;
    }

    public FlatJsonConfig(FlatJsonConfig config) {
        this.flatJsonEnable = config.getFlatJsonEnable();
        this.flatJsonNullFactor = config.getFlatJsonNullFactor();
        this.flatJsonSparsityFactor = config.getFlatJsonSparsityFactor();
        this.flatJsonColumnMax = config.getFlatJsonColumnMax();
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

    public Map<String, String> toProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, String.valueOf(flatJsonEnable));

        // Only include other flat JSON properties if flat JSON is enabled
        if (flatJsonEnable) {
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, String.valueOf(flatJsonNullFactor));
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR,
                    String.valueOf(flatJsonSparsityFactor));
            properties.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, String.valueOf(flatJsonColumnMax));
        }
        return properties;
    }

    public TFlatJsonConfig toTFlatJsonConfig() {
        TFlatJsonConfig tFlatJsonConfig = new TFlatJsonConfig();
        tFlatJsonConfig.setFlat_json_enable(flatJsonEnable);
        tFlatJsonConfig.setFlat_json_null_factor(flatJsonNullFactor);
        tFlatJsonConfig.setFlat_json_sparsity_factor(flatJsonSparsityFactor);
        tFlatJsonConfig.setFlat_json_column_max(flatJsonColumnMax);
        return tFlatJsonConfig;
    }




    @Override
    public String toString() {
        return String.format("{ flat_json_enable : %b,\n " +
                "flat_json_null_factor : %f,\n " +
                "flat_json_sparsity_factor : %f,\n" +
                "flat_json_column_max : %d }", flatJsonEnable, flatJsonNullFactor, flatJsonSparsityFactor,
                flatJsonColumnMax);
    }
}
