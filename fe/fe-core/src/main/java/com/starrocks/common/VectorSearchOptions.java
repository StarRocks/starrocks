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

package com.starrocks.common;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.starrocks.persist.gson.GsonUtils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VectorSearchOptions {

    public VectorSearchOptions() {}

    @SerializedName(value = "enableUseANN")
    private boolean enableUseANN = false;

    @SerializedName(value = "useIVFPQ")
    private boolean useIVFPQ = false;

    @SerializedName(value = "vectorDistanceColumnName")
    private String vectorDistanceColumnName = "vector_distance";

    @SerializedName(value = "vectorLimitK")
    private long vectorLimitK;

    @SerializedName(value = "queryVector")
    private List<String> queryVector = new ArrayList<>();

    @SerializedName(value = "vectorRange")
    private double vectorRange = -1;

    @SerializedName(value = "resultOrder")
    private int resultOrder = 0;

    public boolean isEnableUseANN() {
        return enableUseANN;
    }

    public void setEnableUseANN(boolean enableUseANN) {
        this.enableUseANN = enableUseANN;
    }

    public boolean isUseIVFPQ() {
        return useIVFPQ;
    }

    public void setUseIVFPQ(boolean useIVFPQ) {
        this.useIVFPQ = useIVFPQ;
    }

    public String getVectorDistanceColumnName() {
        return vectorDistanceColumnName;
    }

    public void setVectorDistanceColumnName(String vectorDistanceColumnName) {
        this.vectorDistanceColumnName = vectorDistanceColumnName;
    }

    public long getVectorLimitK() {
        return vectorLimitK;
    }

    public void setVectorLimitK(long vectorLimitK) {
        this.vectorLimitK = vectorLimitK;
    }

    public List<String> getQueryVector() {
        return queryVector;
    }

    public void setQueryVector(List<String> queryVector) {
        this.queryVector = queryVector;
    }

    public double getVectorRange() {
        return vectorRange;
    }

    public void setVectorRange(double vectorRange) {
        this.vectorRange = vectorRange;
    }

    public int getResultOrder() {
        return resultOrder;
    }

    public void setResultOrder(int resultOrder) {
        this.resultOrder = resultOrder;
    }

    public static VectorSearchOptions read(String json) {
        return GsonUtils.GSON.fromJson(json, VectorSearchOptions.class);
    }

    public static Map<String, String> readAnnParams(String json) {
        Type type = new TypeToken<Map<String, String>>() {}.getType();
        return GsonUtils.GSON.fromJson(json, type);
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}