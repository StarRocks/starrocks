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

public class DataCacheOptions {
    private final boolean enablePopulate;
    // todo remove later
    private final int priority;

    private DataCacheOptions(DataCacheOptionsBuilder builder) {
        this.enablePopulate = builder.enablePopulate;
        this.priority = builder.priority;
    }

    public int getPriority() {
        return priority;
    }

    public boolean isEnablePopulate() {
        return enablePopulate;
    }

    public static class DataCacheOptionsBuilder {
        private boolean enablePopulate = false;
        private int priority = 0;

        public static DataCacheOptionsBuilder builder() {
            return new DataCacheOptionsBuilder();
        }

        public DataCacheOptionsBuilder setPriority(int priority) {
            this.priority = priority;
            return this;
        }

        public DataCacheOptionsBuilder setEnablePopulate(boolean enablePopulate) {
            this.enablePopulate = enablePopulate;
            return this;
        }

        public DataCacheOptions build() {
            return new DataCacheOptions(this);
        }
    }
}
