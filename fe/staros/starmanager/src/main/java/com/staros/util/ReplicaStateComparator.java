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

package com.staros.util;

import com.staros.proto.ReplicaState;

public class ReplicaStateComparator {
    // REPLICA_OK < REPLICA_SCALE_IN < REPLICA_SCALE_OUT
    public static int compare(ReplicaState lhs, ReplicaState rhs) {
        if (lhs.equals(rhs)) {
            return 0;
        }
        if (ReplicaState.REPLICA_OK.equals(lhs)) {
            return -1;
        }
        if (ReplicaState.REPLICA_OK.equals((rhs))) {
            return 1;
        }
        // not equal, not REPLICA_OK, compare SCALE_IN and SCALE_OUT
        // reverse the ordinal comparison.
        return rhs.ordinal() - lhs.ordinal();
    }
}
