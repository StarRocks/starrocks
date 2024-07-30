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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Objects;

public class ComplexTypeAccessPaths {
    private final ImmutableList<ComplexTypeAccessPath> accessPaths;

    public ComplexTypeAccessPaths(ImmutableList<ComplexTypeAccessPath> accessPaths) {
        Preconditions.checkNotNull(accessPaths, "accessPaths can't be null");
        this.accessPaths = accessPaths;
    }

    public int size() {
        return accessPaths.size();
    }

    public ComplexTypeAccessPath get(int idx) {
        return accessPaths.get(idx);
    }

    public boolean isEmpty() {
        return accessPaths.isEmpty();
    }

    public ImmutableList<ComplexTypeAccessPath> getAccessPaths() {
        return accessPaths;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ComplexTypeAccessPaths)) {
            return false;
        }

        ComplexTypeAccessPaths that = (ComplexTypeAccessPaths) o;
        return Objects.equals(accessPaths, that.accessPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(accessPaths);
    }
}
