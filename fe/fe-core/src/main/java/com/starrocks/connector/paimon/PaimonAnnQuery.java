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

package com.starrocks.connector.paimon;

import java.util.List;

/** ANN intent for a single Paimon scan, before binding its snapshot and stable field ID. */
public record PaimonAnnQuery(String columnName, String metric, List<Float> queryVector, int k) {
    public PaimonAnnQuery {
        queryVector = List.copyOf(queryVector);
    }
}
