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

package com.starrocks.datacache;

import static com.google.common.base.Preconditions.checkArgument;

public enum DataCachePopulateMode {
    AUTO("auto"),
    NEVER("never"),
    ALWAYS("always");

    private final String modeName;

    private DataCachePopulateMode(String modeName) {
        this.modeName = modeName;
    }

    public static DataCachePopulateMode fromName(String modeName) {
        checkArgument(modeName != null, "Populate mode name is null");

        if (AUTO.modeName().equalsIgnoreCase(modeName)) {
            return AUTO;
        } else if (NEVER.modeName().equalsIgnoreCase(modeName)) {
            return NEVER;
        } else if (ALWAYS.modeName().equalsIgnoreCase(modeName)) {
            return ALWAYS;
        } else {
            throw new IllegalArgumentException(
                    "Unknown populate mode: " + modeName + ", only support auto, never and always");
        }
    }

    public String modeName() {
        return modeName;
    }

}
