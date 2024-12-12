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

package com.starrocks.sql.automv.pattern;

import java.util.Collections;

public class PlanPiecePatterns {
    private static final PlanPiecePattern ONE_ONE_MV_PATTERN;
    private static final PlanPiecePattern SPJG_PATTERN;

    static {
        PlanPiecePattern scanPat =
                PlanPiecePattern.repeat(PlanPiecePattern.node(PlanPiecePattern.NodeName.Scan), 1, 1000);
        PlanPiecePattern joinPat =
                PlanPiecePattern.repeat(PlanPiecePattern.node(PlanPiecePattern.NodeName.Join), 1, 1000);
        PlanPiecePattern aggPat =
                PlanPiecePattern.repeat(PlanPiecePattern.node(PlanPiecePattern.NodeName.Aggregate), 1, 1000);
        ONE_ONE_MV_PATTERN = PlanPiecePattern.consistOf(scanPat, joinPat, aggPat);
    }

    static {
        PlanPiecePattern scanPat =
                PlanPiecePattern.repeat(PlanPiecePattern.node(PlanPiecePattern.NodeName.Scan), 1, 1000);
        PlanPiecePattern joinPat =
                PlanPiecePattern.repeat(PlanPiecePattern.node(PlanPiecePattern.NodeName.Join), 1, 1000);
        SPJG_PATTERN =
                PlanPiecePattern.treeCapture(PlanPiecePattern.node(PlanPiecePattern.NodeName.Aggregate),
                        Collections.singletonList(PlanPiecePattern.consistOf(scanPat, joinPat)), null);
    }

    public static PlanPiecePattern get11MV() {
        return ONE_ONE_MV_PATTERN;
    }

    public static PlanPiecePattern getSPJG() {
        return SPJG_PATTERN;
    }
}
