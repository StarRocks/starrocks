// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/column_mapping.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>

#include "column/datum.h"
#include "types/bitmap_value.h"
#include "types/hll.h"
#include "util/json.h"
#include "util/percentile_value.h"

namespace starrocks {

struct ColumnMapping {
    // <0: use default value
    // >=0: use origin column
    int32_t ref_column{-1};

    // The base reader selects part of the column
    // so it's different to ref_column
    int32_t ref_base_reader_column_index = -1;
    // materialize view transform function used in schema change
    std::string materialized_function;

    // the following data is used by default_value_datum, because default_value_datum only
    // have the reference. We need to keep the content has the same life cycle as the
    // default_value_datum;
    std::unique_ptr<HyperLogLog> default_hll;
    std::unique_ptr<BitmapValue> default_bitmap;
    std::unique_ptr<PercentileValue> default_percentile;
    std::unique_ptr<JsonValue> default_json;

    vectorized::Datum default_value_datum;
};

typedef std::vector<ColumnMapping> SchemaMapping;

} // namespace starrocks
