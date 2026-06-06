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

#include "storage/rowset/object_column_writer.h"

#include "base/status_fmt.hpp"
#include "column/column_helper.h"

namespace starrocks {
Status ObjectColumnWriter::append(const Column& column) {
    switch (type_info()->type()) {
#define M(TYPE)                                                                           \
    case TYPE: {                                                                          \
        const auto* object_column = ColumnHelper::get_data_column_by_type<TYPE>(&column); \
        object_column->build_slices(_serialize_buf, _slices);                             \
        return _scalar_column_writer->append(column, _slices);                            \
    }
        APPLY_FOR_ALL_OBJECT_TYPE(M)
#undef M
    default:
        return Status::InternalError("Not supported type for object column writer, type={}", type_info()->type());
    }
}
} // namespace starrocks
