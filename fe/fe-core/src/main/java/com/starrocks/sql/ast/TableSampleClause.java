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

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * SQL Clause of: TABLE SAMPLE (...)
 */
public class TableSampleClause implements ParseNode {

    // Properties supported by sampling
    private static final String SAMPLE_PROPERTY_METHOD = "METHOD";
    private static final String SAMPLE_PROPERTY_SEED = "SEED";
    private static final String SAMPLE_PROPERTY_PROBABILITY = "PROBABILITY";

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

    public SampleMethod getSampleMethod() {
        return sampleMethod;
    }

    public void setSampleMethod(SampleMethod sampleMethod) {
        this.sampleMethod = sampleMethod;
    }

    public long getRandomSeed() {
        return randomSeed;
    }

    public void setRandomSeed(long randomSeed) {
        this.randomSeed = randomSeed;
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
                case SAMPLE_PROPERTY_PROBABILITY:
                    long percent = ParseUtil.analyzeLongValue(value);
                    if (!(0 <= percent && percent <= 100)) {
                        throw new AnalysisException("invalid " + SAMPLE_PROPERTY_PROBABILITY + " which should in " +
                                "[0, 100]");
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
        final StringBuffer sb = new StringBuffer("TableSample{");
        sb.append("method=").append(sampleMethod);
        sb.append(", randomSeed=").append(randomSeed);
        sb.append(", randomProbability=").append(randomProbabilityPercent);
        sb.append('}');
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
        BY_ROW,
        BY_BLOCK;

        private static final Map<SampleMethod, com.starrocks.thrift.SampleMethod> THRIFT_MAPPING =
                ImmutableMap.of(BY_ROW, com.starrocks.thrift.SampleMethod.BY_ROW,
                        BY_BLOCK, com.starrocks.thrift.SampleMethod.BY_BLOCK);

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
