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

package com.starrocks.common;

import com.starrocks.common.io.ParamsKey;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parameter key definitions for the GiST (Generalized Search Tree / R-Tree)
 * spatial index on GEOMETRY columns.
 */
public class GiSTIndexParams {

    /** Tree algorithm variant. Only RTREE is supported initially. */
    public enum GiSTIndexType {
        RTREE
    }

    public enum CommonIndexParamKey implements ParamsKey {
        INDEX_TYPE {
            private final Set<String> validTypes =
                    Arrays.stream(GiSTIndexType.values())
                            .map(t -> t.name().toUpperCase(Locale.ROOT))
                            .collect(Collectors.toSet());

            @Override
            public void check(String value) {
                if (!validTypes.contains(value.toUpperCase(Locale.ROOT))) {
                    throw new SemanticException(
                            String.format("Value of `index_type` for GIST must be one of (%s)",
                                    String.join(",", validTypes)));
                }
            }
        }
    }

    public enum IndexParamsKey implements ParamsKey {
        /** Maximum number of entries per R-Tree node. Default: 50. */
        NODE_CAPACITY {
            @Override
            public void check(String value) {
                try {
                    int v = Integer.parseInt(value.trim());
                    if (v < 4 || v > 1024) {
                        throw new SemanticException(
                                "Value of `node_capacity` for GIST must be between 4 and 1024");
                    }
                } catch (NumberFormatException e) {
                    throw new SemanticException(
                            "Value of `node_capacity` for GIST must be an integer");
                }
            }
        }
    }
}
