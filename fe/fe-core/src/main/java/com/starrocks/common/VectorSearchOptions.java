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

import com.starrocks.thrift.TVectorSearchOptions;

import java.util.ArrayList;
import java.util.List;

public class VectorSearchOptions {
    private static final int RESULT_ORDER_ASC = 0;
    private static final int RESULT_ORDER_DESC = 1;

    private boolean enableUseANN = false;
    private boolean useIVFPQ = false;

    private String distanceColumnName = "";
    private int distanceSlotId = 0;

    private long limitK = 0;
    private int resultOrder = 0;

    private double predicateRange = -1;
    private List<String> queryVector = new ArrayList<>();

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

    public String getDistanceColumnName() {
        return distanceColumnName;
    }

    public void setDistanceColumnName(String distanceColumnName) {
        this.distanceColumnName = distanceColumnName;
    }

    public void setDistanceSlotId(int distanceSlotId) {
        this.distanceSlotId = distanceSlotId;
    }

    public void setLimitK(long limitK) {
        this.limitK = limitK;
    }

    public void setQueryVector(List<String> queryVector) {
        this.queryVector = queryVector;
    }

    public void setPredicateRange(double predicateRange) {
        this.predicateRange = predicateRange;
    }

    public void setResultOrder(boolean isAsc) {
        this.resultOrder = isAsc ? RESULT_ORDER_ASC : RESULT_ORDER_DESC;
    }

    public TVectorSearchOptions toThrift() {
        TVectorSearchOptions opts = new TVectorSearchOptions();
        opts.setEnable_use_ann(true);
        opts.setVector_limit_k(limitK);
        opts.setVector_distance_column_name(distanceColumnName);
        opts.setVector_slot_id(distanceSlotId);
        opts.setQuery_vector(queryVector);
        opts.setVector_range(predicateRange);
        opts.setResult_order(resultOrder);
        opts.setUse_ivfpq(useIVFPQ);
        return opts;
    }

    public String getExplainString(String prefix) {
        return prefix + "VECTORINDEX: ON" + "\n" +
                prefix + prefix +
                "IVFPQ: " + (useIVFPQ ? "ON" : "OFF") + ", " +
                "Distance Column: <" + distanceSlotId + ":" + distanceColumnName + ">, " +
                "LimitK: " + limitK + ", " +
                "Order: " + (resultOrder == RESULT_ORDER_ASC ? "ASC" : "DESC") + ", " +
                "Query Vector: " + queryVector + ", " +
                "Predicate Range: " + predicateRange +
                "\n";
    }
}