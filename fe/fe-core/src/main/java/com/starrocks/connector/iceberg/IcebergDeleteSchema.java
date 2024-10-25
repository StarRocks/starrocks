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

package com.starrocks.connector.iceberg;

import java.util.List;
import java.util.Objects;

public class IcebergDeleteSchema {
    private final List<Integer> equalityIds;
    private final Integer specId;

    public static IcebergDeleteSchema of(List<Integer> equalityIds, Integer specId) {
        return new IcebergDeleteSchema(equalityIds, specId);
    }

    public IcebergDeleteSchema(List<Integer> equalityIds, Integer specId) {
        this.equalityIds = equalityIds;
        this.specId = specId;
    }

    public List<Integer> equalityIds() {
        return equalityIds;
    }

    public Integer specId() {
        return specId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergDeleteSchema that = (IcebergDeleteSchema) o;
        return Objects.equals(equalityIds, that.equalityIds) && Objects.equals(specId, that.specId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(equalityIds, specId);
    }
}
