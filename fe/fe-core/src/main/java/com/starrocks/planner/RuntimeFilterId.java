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


package com.starrocks.planner;

import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;

// The unique id for join runtime filter
public class RuntimeFilterId extends Id<RuntimeFilterId> {
    public RuntimeFilterId(int id) {
        super(id);
    }

    public static IdGenerator<RuntimeFilterId> createGenerator() {
        return new IdGenerator<RuntimeFilterId>() {
            @Override
            public RuntimeFilterId getNextId() {
                return new RuntimeFilterId(nextId++);
            }

            @Override
            public RuntimeFilterId getMaxId() {
                return new RuntimeFilterId(nextId - 1);
            }
        };
    }
}
