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

package com.starrocks.qe;

import org.apache.commons.lang3.EnumUtils;

public class SessionVariableConstants {

    private SessionVariableConstants() {}

    public static final String AUTO = "auto";

    public static final String FORCE_STREAMING = "force_streaming";

    public static final String FORCE_PREAGGREGATION = "force_preaggregation";

    public static final String LIMITED = "limited";

    public static final String PANIC = "panic";

    public static final String DOUBLE = "double";

    public static final String DECIMAL = "decimal";

    public static final String VARCHAR = "varchar";

    public static final String ALWAYS = "always";

    public static final String NEVER = "never";

    public enum ChooseInstancesMode {

        // the number of chosen instances is the same as the max number of instances from its children fragments
        LOCALITY,

        // auto increase or decrease the instances based on the processed data size
        AUTO,

        // choose more instances than the max number of instances from its children fragments
        // if the remote fragment needs process too much data
        ADAPTIVE_INCREASE,

        // choose fewer instances than the max number of instances from its children fragments
        // if the remote fragment doesn't need process too much data
        ADAPTIVE_DECREASE;

        public boolean enableIncreaseInstance() {
            return this == AUTO || this == ADAPTIVE_INCREASE;
        }

        public boolean enableDecreaseInstance() {
            return this == AUTO || this == ADAPTIVE_DECREASE;
        }
    }

    public enum ComputationFragmentSchedulingPolicy {
        
        // only select compute node in scheduler policy (default)
        COMPUTE_NODES_ONLY,

        // both select compute node and backend in scheduler policy
        ALL_NODES
    }

    public enum AggregationStage {
        AUTO,
        ONE_STAGE,
        TWO_STAGE,
        THREE_STAGE,
        FOUR_STAGE
    }

    // default, ndv, rewrite_by_hll_bitmap
    public enum CountDistinctImplMode {
        DEFAULT,                // default, keeps the original count distinct implementation
        NDV,                    // ndv, uses HyperLogLog to estimate the count distinct
        MULTI_COUNT_DISTINCT;
        public static String MODE_DEFAULT = DEFAULT.toString();
        public static CountDistinctImplMode parse(String str) {
            return EnumUtils.getEnumIgnoreCase(CountDistinctImplMode.class, str);
        }
    }
}
