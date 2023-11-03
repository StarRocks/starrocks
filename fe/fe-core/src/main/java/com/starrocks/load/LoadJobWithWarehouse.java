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

package com.starrocks.load;

public interface LoadJobWithWarehouse {
    String getCurrentWarehouse();

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/load/LoadJobWithWarehouse.java
    boolean isFinal();
=======
#include <cstddef>
#include <vector>
>>>>>>> 2a51cf89f9 ([BugFix] Fix select ORC MapVector's filter nullptr bug (#34267)):be/src/storage/lake/key_index.h

    default boolean isInternalJob() {
        return false;
    }

    long getFinishTimestampMs();
}
