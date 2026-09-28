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

package com.starrocks.connector.index;

import java.util.Locale;
import java.util.Optional;

/** Canonical vector metrics shared by SQL recognition and connector index descriptors. */
public enum VectorIndexMetric {
    L2,
    COSINE,
    INNER_PRODUCT;

    public static Optional<VectorIndexMetric> fromOption(String value) {
        if (value == null) {
            return Optional.empty();
        }
        switch (value.trim().toLowerCase(Locale.ROOT).replace('-', '_')) {
            case "l2":
            case "euclidean":
            case "euclidean_distance":
                return Optional.of(L2);
            case "cosine":
            case "cosine_similarity":
                return Optional.of(COSINE);
            case "ip":
            case "inner_product":
                return Optional.of(INNER_PRODUCT);
            default:
                return Optional.empty();
        }
    }
}
