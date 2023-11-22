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

package com.starrocks.sql.common;

public class OptimizerSetting {
    private final boolean reuseViewDef;

    public boolean isReuseViewDef() {
        return reuseViewDef;
    }

    private OptimizerSetting(Builder builder) {
        this.reuseViewDef = builder.reuseViewDef;
    }

    public static OptimizerSetting.Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private boolean reuseViewDef;

        public Builder setReuseViewDef(boolean reuseViewDef) {
            this.reuseViewDef = reuseViewDef;
            return this;
        }

        public OptimizerSetting build() {
            return new OptimizerSetting(this);
        }
    }
}
