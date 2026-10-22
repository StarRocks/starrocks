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

#include "formats/arrow/arrow_column_converter.h"

#include <arrow/type.h>

#include "column/arrow/arrow_to_starrocks_converter.h"
#include "common/object_pool.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "runtime/descriptors.h"

namespace starrocks {

Status create_arrow_column(const arrow::DataType* arrow_type, const SlotDescriptor* slot_desc, MutableColumnPtr* column,
                           ConvertFuncTree* conv_func, Expr** expr, ObjectPool& pool, bool strict_mode) {
    const auto& type_desc = slot_desc->type();
    TypeDescriptor raw_type_desc;
    bool need_cast = false;
    RETURN_IF_ERROR(build_arrow_column_convert_plan(arrow_type, &type_desc, slot_desc->is_nullable(), &raw_type_desc,
                                                    conv_func, need_cast, strict_mode));
    *column = create_arrow_column_convert_dest(type_desc, raw_type_desc, need_cast, slot_desc->is_nullable());
    auto* slot = pool.add(new ColumnRef(slot_desc));
    if (!need_cast) {
        *expr = slot;
        return Status::OK();
    }

    *expr = VectorizedCastExprFactory::from_type(raw_type_desc, type_desc, slot, &pool);
    if (*expr == nullptr) {
        return illegal_converting_error(arrow_type->name(), type_desc.debug_string());
    }
    return Status::OK();
}

} // namespace starrocks
