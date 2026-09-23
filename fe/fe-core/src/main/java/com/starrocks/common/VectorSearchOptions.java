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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VectorSearchOptions {
    private static final int RESULT_ORDER_ASC = 0;
    private static final int RESULT_ORDER_DESC = 1;

    private boolean enableUseANN = false;
    // When true, re-rank the ANN result by recomputing the exact distance on the full-precision
    // vectors (used for a quantized index whose index distance is lossy).
    private boolean refineDistance = false;
    // Whether a split of this scan must land on a segment boundary. Folded from the
    // enable_vector_index_split_at_segment_boundary session variable; see TVectorSearchOptions.
    private boolean splitAtSegmentBoundary = true;

    private String distanceColumnName = "";
    private int distanceSlotId = 0;

    private long limitK = 0;
    private int resultOrder = 0;

    private double predicateRange = -1;
    private boolean hasPredicateRange = false;
    // The decoded query vector: the only representation the FE keeps, and the one it ships.
    private float[] queryVector = new float[0];

    public boolean isEnableUseANN() {
        return enableUseANN;
    }

    public void setEnableUseANN(boolean enableUseANN) {
        this.enableUseANN = enableUseANN;
    }

    public boolean isRefineDistance() {
        return refineDistance;
    }

    public void setRefineDistance(boolean refineDistance) {
        this.refineDistance = refineDistance;
    }

    public void setSplitAtSegmentBoundary(boolean splitAtSegmentBoundary) {
        this.splitAtSegmentBoundary = splitAtSegmentBoundary;
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

    public float[] getQueryVector() {
        return queryVector;
    }

    public void setQueryVector(float[] queryVector) {
        this.queryVector = queryVector;
    }

    public void setPredicateRange(double predicateRange) {
        this.predicateRange = predicateRange;
        this.hasPredicateRange = true;
    }

    public void setResultOrder(boolean isAsc) {
        this.resultOrder = isAsc ? RESULT_ORDER_ASC : RESULT_ORDER_DESC;
    }

    /** The wire form: little-endian float32, which is also the BE's in-memory layout. */
    private static byte[] toLittleEndianBytes(float[] vector) {
        ByteBuffer buf = ByteBuffer.allocate(vector.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : vector) {
            buf.putFloat(v);
        }
        return buf.array();
    }

    /** The text wire form, one decimal string per dimension. Only reached with the switch off. */
    private static List<String> toDecimalStrings(float[] vector) {
        List<String> out = new ArrayList<>(vector.length);
        for (float v : vector) {
            out.add(Float.toString(v));
        }
        return out;
    }

    public TVectorSearchOptions toThrift() {
        TVectorSearchOptions opts = new TVectorSearchOptions();
        opts.setEnable_use_ann(true);
        opts.setVector_limit_k(limitK);
        opts.setVector_distance_column_name(distanceColumnName);
        opts.setVector_slot_id(distanceSlotId);
        // A BE only needs the text form when an operator turns the binary form off: BE/CN is
        // always upgraded before the FE, so a BE is never older than the FE talking to it.
        if (Config.enable_vector_query_binary) {
            opts.setQuery_vector_f32(toLittleEndianBytes(queryVector));
        } else {
            opts.setQuery_vector(toDecimalStrings(queryVector));
        }
        opts.setVector_range(predicateRange);
        opts.setHas_vector_range(hasPredicateRange);
        opts.setResult_order(resultOrder);
        opts.setRefine_distance(refineDistance);
        opts.setSplit_at_segment_boundary(splitAtSegmentBoundary);
        // Also set the deprecated use_ivfpq to the same value during the deprecation window: an older BE
        // (which only understands use_ivfpq) then runs the same path under a rolling upgrade. The two
        // flags always mean the same thing -- "run the refine path". Remove once no old BE remains.
        opts.setUse_ivfpq(refineDistance);
        return opts;
    }

    public String getExplainString(String prefix) {
        return prefix + "VECTORINDEX: ON" + "\n" +
                prefix + prefix +
                "Refine: " + (refineDistance ? "ON" : "OFF") + ", " +
                "Distance Column: <" + distanceSlotId + ":" + distanceColumnName + ">, " +
                "LimitK: " + limitK + ", " +
                "Order: " + (resultOrder == RESULT_ORDER_ASC ? "ASC" : "DESC") + ", " +
                "Query Vector: " + Arrays.toString(queryVector) + ", " +
                "Predicate Range: " + (hasPredicateRange ? Double.toString(predicateRange) : "N/A") +
                "\n";
    }
}
