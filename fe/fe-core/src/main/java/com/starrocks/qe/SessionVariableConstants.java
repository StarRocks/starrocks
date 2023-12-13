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

    public static final String ADAPTIVE_INCREASE = "adaptive_increase";
    public static final String ADAPTIVE_DECREASE = "adaptive_decrease";

    public enum ChooseInstancesMode {
        LOCALITY,
        AUTO,
        ADAPTIVE_INCREASE,
        ADAPTIVE_DECREASE;

        public boolean enableIncreaseInstance() {
            return this == AUTO || this == ADAPTIVE_INCREASE;
        }

        public boolean enableDecreaseInstance() {
            return this == AUTO || this == ADAPTIVE_DECREASE;
        }
    }
}
