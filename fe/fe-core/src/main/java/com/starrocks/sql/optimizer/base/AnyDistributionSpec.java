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


package com.starrocks.sql.optimizer.base;

import java.util.Objects;

public class AnyDistributionSpec extends DistributionSpec {

    public static final AnyDistributionSpec INSTANCE = new AnyDistributionSpec();
    private AnyDistributionSpec() {
        super(DistributionType.ANY);
    }

    public boolean isSatisfy(DistributionSpec spec) {
        return spec.type.equals(DistributionType.ANY);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        return obj instanceof AnyDistributionSpec;
    }
}
