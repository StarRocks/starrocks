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

package com.starrocks.common.util.concurrent.lock;

public class LockType {
    public static final LockType READ = new LockType(0);
    public static final LockType WRITE = new LockType(1);
    public static final LockType INTENTION_SHARED = new LockType(2);
    public static final LockType INTENTION_EXCLUSIVE = new LockType(3);

    private static final boolean[][] CONFLICT_MATRIX = {
            // READ is held and there is a request for:
            {
                    true,   // READ
                    false,  // WRITE
                    true,   // INTENTION_SHARED
                    false,  // INTENTION_EXCLUSIVE
            },
            // WRITE is held and there is a request for:
            {
                    false,   // READ
                    false,   // WRITE
                    false,   // INTENTION_SHARED
                    false,   // INTENTION_EXCLUSIVE
            },
            // INTENTION_SHARED is held and there is a request for:
            {
                    true,    // READ
                    false,   // WRITE
                    true,    // INTENTION_SHARED
                    true,    // INTENTION_EXCLUSIVE
            },
            // INTENTION_EXCLUSIVE is held and there is a request for:
            {
                    false,    // READ
                    false,    // WRITE
                    true,     // INTENTION_SHARED
                    true,     // INTENTION_EXCLUSIVE
            }
    };

    private final int id;

    private LockType(int id) {
        this.id = id;
    }

    public final boolean isWriteLock() {
        return id == 1;
    }

    public boolean isConflict(LockType requestedType) {
        return !CONFLICT_MATRIX[id][requestedType.id];
    }

    @Override
    public String toString() {
        if (id == 0) {
            return "READ";
        } else if (id == 1) {
            return "WRITE";
        } else if (id == 2) {
            return "INTENTION_SHARED";
        } else if (id == 3) {
            return "INTENTION_EXCLUSIVE";
        } else {
            return "UNKNOWN";
        }
    }
}