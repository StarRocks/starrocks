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
<<<<<<< HEAD
=======

    public static final String VARCHAR = "varchar";

    public enum AggregationStage {
        AUTO,
        ONE_STAGE,
        TWO_STAGE,
        THREE_STAGE,
        FOUR_STAGE
    }
>>>>>>> 3fcdc4e1f4 ([Enhancement] support decimal eq string cast flag (#34208))
}
