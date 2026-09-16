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

#pragma once

namespace starrocks {

enum RangeCondition {
    GT = 0, // greater than
    GE = 1, // greater or equal
    LT = 2, // less than
    LE = 3, // less or equal
};

enum MaterializeType {
    OLAP_MATERIALIZE_TYPE_UNKNOWN = 0,
    OLAP_MATERIALIZE_TYPE_PERCENTILE = 1,
    OLAP_MATERIALIZE_TYPE_HLL = 2,
    OLAP_MATERIALIZE_TYPE_BITMAP = 3,
    OLAP_MATERIALIZE_TYPE_COUNT = 4
};

enum PushType {
    PUSH_FOR_DELETE = 2, // for delete
    PUSH_NORMAL_V2 = 4,  // for spark load
};

} // namespace starrocks
