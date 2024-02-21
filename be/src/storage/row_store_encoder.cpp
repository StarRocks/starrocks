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

#include "storage/row_store_encoder.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "gutil/endian.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/primary_key_encoder.h"
#include "storage/row_store_encoder_util.h"
#include "storage/tablet_schema.h"
#include "types/date_value.hpp"

namespace starrocks {

bool RowStoreEncoder::is_field_supported(const Field& f) {
    switch (f.type()->type()) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_VARCHAR:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_CHAR:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_TIME:
    case TYPE_DECIMALV2:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        return true;
    default:
        return false;
    }
}

Status RowStoreEncoder::is_supported(const Schema& schema) {
    for (size_t i = schema.num_key_fields(); i < schema.num_fields(); i++) {
        if (!is_field_supported(*(schema.field(i).get()))) {
            return Status::NotSupported(StringPrintf("row encode type not supported: %s ",
                                                     logical_type_to_string(schema.field(i).get()->type()->type())));
        }
    }
    return Status::OK();
}

} // namespace starrocks