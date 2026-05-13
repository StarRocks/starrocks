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

package com.starrocks.connector.paimon;

import com.starrocks.thrift.TPaimonVectorSearchCondition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Carries vector search parameters for Paimon single-round execution.
 * FE builds this from the optimizer rule and passes it through to PaimonScanNode,
 * which serializes it as a Thrift struct on each per-shard scan range.
 */
public class PaimonVectorSearchOptions {
    private static final int RESULT_ORDER_ASC = 0;
    private static final int RESULT_ORDER_DESC = 1;

    private String scoreFunctionName = "";
    private String vectorColumnName = "";
    private List<Double> queryVector = new ArrayList<>();
    private long limitPerShard = 0;
    private long limitGlobal = 0;
    private int resultOrder = RESULT_ORDER_ASC;
    private Map<String, String> searchParams = new HashMap<>();

    public String getScoreFunctionName() {
        return scoreFunctionName;
    }

    public void setScoreFunctionName(String scoreFunctionName) {
        this.scoreFunctionName = scoreFunctionName;
    }

    public String getVectorColumnName() {
        return vectorColumnName;
    }

    public void setVectorColumnName(String vectorColumnName) {
        this.vectorColumnName = vectorColumnName;
    }

    public List<Double> getQueryVector() {
        return queryVector;
    }

    public void setQueryVector(List<Double> queryVector) {
        this.queryVector = queryVector;
    }

    public long getLimitPerShard() {
        return limitPerShard;
    }

    public void setLimitPerShard(long limitPerShard) {
        this.limitPerShard = limitPerShard;
    }

    public long getLimitGlobal() {
        return limitGlobal;
    }

    public void setLimitGlobal(long limitGlobal) {
        this.limitGlobal = limitGlobal;
    }

    public void setResultOrder(boolean isAscending) {
        this.resultOrder = isAscending ? RESULT_ORDER_ASC : RESULT_ORDER_DESC;
    }

    public int getResultOrder() {
        return resultOrder;
    }

    public Map<String, String> getSearchParams() {
        return searchParams;
    }

    public void setSearchParams(Map<String, String> searchParams) {
        this.searchParams = searchParams;
    }

    /**
     * Build a Thrift struct for one shard's scan range.
     * The search condition fields are shared; shard routing fields are per-range.
     */
    public TPaimonVectorSearchCondition toThrift(int shardId, long rangeFrom, long rangeTo, String tablePath) {
        TPaimonVectorSearchCondition condition = new TPaimonVectorSearchCondition();
        condition.setScore_function_name(scoreFunctionName);
        condition.setVector_column_name(vectorColumnName);
        condition.setQuery_vector(queryVector);
        condition.setLimit_per_shard(limitPerShard);
        condition.setLimit_global(limitGlobal);
        condition.setResult_order(resultOrder);
        if (searchParams != null && !searchParams.isEmpty()) {
            condition.setSearch_params(searchParams);
        }
        condition.setShard_id(shardId);
        condition.setRange_from(rangeFrom);
        condition.setRange_to(rangeTo);
        condition.setTable_path(tablePath);
        return condition;
    }

    public String getExplainString(String prefix) {
        return prefix + "VECTOR-SEARCH-MODE: single-round\n" +
                prefix + "  Function: " + scoreFunctionName + "\n" +
                prefix + "  Column: " + vectorColumnName + "\n" +
                prefix + "  LimitPerShard: " + limitPerShard + "\n" +
                prefix + "  LimitGlobal: " + limitGlobal + "\n" +
                prefix + "  Order: " + (resultOrder == RESULT_ORDER_ASC ? "ASC" : "DESC") + "\n" +
                prefix + "  QueryVector: " + queryVector + "\n";
    }
}
