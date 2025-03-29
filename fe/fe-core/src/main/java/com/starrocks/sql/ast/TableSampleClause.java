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

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.ParseNode;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TTableSampleOptions;
import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * SQL Clause of: TABLE SAMPLE (...)
 */
public final class TableSampleClause implements ParseNode {

    // Properties supported by sampling
    // sample method: by_row/by_block/by_page
    private static final String SAMPLE_PROPERTY_METHOD = "METHOD";
    // random seed: default value is a random number
    private static final String SAMPLE_PROPERTY_SEED = "SEED";
    // percent of sampling: (0,100)
    private static final String SAMPLE_PROPERTY_PERCENT = "PERCENT";

    // Default values for sampling
    private static final SampleMethod DEFAULT_SAMPLE_METHOD = SampleMethod.BY_BLOCK;
    private static final long DEFAULT_RANDOM_PROBABILITY_PERCENT = 1;

    private final NodePosition pos;
    private boolean useSampling = true;
    private SampleMethod sampleMethod = DEFAULT_SAMPLE_METHOD;
    private long randomSeed = ThreadLocalRandom.current().nextLong();
    private long randomProbabilityPercent = DEFAULT_RANDOM_PROBABILITY_PERCENT;

    public TableSampleClause(NodePosition pos) {
        this.pos = pos;
    }

    public boolean isUseSampling() {
        return useSampling;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public void analyzeProperties(Map<String, String> properties) throws AnalysisException {
        for (var entry : properties.entrySet()) {
            String value = entry.getValue();
            switch (entry.getKey().toUpperCase()) {
                case SAMPLE_PROPERTY_METHOD:
                    this.sampleMethod = SampleMethod.mustParse(value);
                    break;
                case SAMPLE_PROPERTY_PERCENT:
                    long percent = ParseUtil.analyzeLongValue(value);
                    if (!(0 < percent && percent < 100)) {
                        throw new AnalysisException("invalid " + entry.getKey() + " which should in (0, 100)");
                    }
                    this.randomProbabilityPercent = percent;
                    break;
                case SAMPLE_PROPERTY_SEED:
                    this.randomSeed = ParseUtil.analyzeLongValue(value);
                    break;
                default:
                    throw new AnalysisException("unrecognized property: " + entry.getKey());
            }
        }
    }

    public void toThrift(TTableSampleOptions msg) {
        msg.setEnable_sampling(true);
        msg.setProbability_percent(randomProbabilityPercent);
        msg.setSample_method(sampleMethod.toThrift());
        msg.setRandom_seed(randomSeed);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TableSample{");
        sb.append("method=").append(sampleMethod);
        sb.append(", randomSeed=").append(randomSeed);
        sb.append(", randomProbability=").append(randomProbabilityPercent);
        sb.append('}');
        return sb.toString();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SAMPLE(");
        sb.append("'method'=").append(Strings.quote(sampleMethod.toString())).append(",");
        sb.append("'seed'=").append(Strings.quote(String.valueOf(randomSeed))).append(",");
        sb.append("'percent'=").append(Strings.quote(String.valueOf(randomProbabilityPercent)));
        sb.append(")");
        return sb.toString();
    }

    public String explain() {
        StringBuffer sb = new StringBuffer("SAMPLE: ");
        sb.append("method=").append(sampleMethod);
        sb.append(" seed=").append(randomSeed);
        sb.append(" probability=").append(randomProbabilityPercent);
        return sb.toString();
    }

    public enum SampleMethod {
        BY_BLOCK,   // BLOCK: about 1024 rows in a logical block
        BY_PAGE;    // PAGE: physical page, about 64KB, which is the most efficient but may not uniform

        private static final Map<SampleMethod, com.starrocks.thrift.SampleMethod> THRIFT_MAPPING =
                ImmutableMap.of(BY_BLOCK, com.starrocks.thrift.SampleMethod.BY_BLOCK,
                        BY_PAGE, com.starrocks.thrift.SampleMethod.BY_PAGE);

        public static SampleMethod mustParse(String value) throws AnalysisException {
            SampleMethod result = EnumUtils.getEnumIgnoreCase(SampleMethod.class, value);
            if (result == null) {
                throw new AnalysisException("unrecognized sample-method: " + value);
            }
            return result;
        }

        public com.starrocks.thrift.SampleMethod toThrift() {
            return THRIFT_MAPPING.get(this);
        }
    }
}
