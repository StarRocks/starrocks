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

#include "storage/rowset/dictcode_column_iterator.h"

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "gutil/casts.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {

Status GlobalDictCodeColumnIterator::decode_dict_codes(const Column& codes, Column* words) {
    auto code_data = ColumnHelper::get_data_column(&codes);
    if (code_data->is_array()) {
        return decode_array_dict_codes(codes, words);
    }

    return decode_string_dict_codes(codes, words);
}

Status GlobalDictCodeColumnIterator::decode_array_dict_codes(const Column& codes, Column* words) {
    auto* code_array = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(&codes));
    auto* words_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(words));
    words_array->offsets_column()->resize(0); // array offset set 0 default
    words_array->offsets_column()->append(code_array->offsets(), 0, code_array->offsets().size());

    if (codes.is_nullable()) {
        DCHECK(words->is_nullable());
        auto* code_null = down_cast<const NullableColumn*>(&codes);
        auto* words_null = down_cast<NullableColumn*>(words);
        words_null->mutable_null_column()->append(code_null->null_column_ref(), 0, code_null->size());
        words_null->set_has_null(code_null->has_null());
    }

    return decode_string_dict_codes(*code_array->elements_column(), words_array->elements_column().get());
}

Status GlobalDictCodeColumnIterator::decode_string_dict_codes(const Column& codes, Column* words) {
    const auto& code_data = down_cast<const Int32Column*>(ColumnHelper::get_data_column(&codes))->get_data();
    const size_t size = code_data.size();

    auto* low_card = down_cast<LowCardDictColumn*>(ColumnHelper::get_data_column(words));
    low_card->resize_uninitialized(size);
    LowCardDictColumn::Container* container = &down_cast<LowCardDictColumn*>(low_card)->get_data();
    bool output_nullable = words->is_nullable();

    auto& res_data = *container;
#ifndef NDEBUG
    for (size_t i = 0; i < size; ++i) {
        DCHECK(code_data[i] <= _dict_size);
        if (code_data[i] < 0) {
            DCHECK(output_nullable);
        }
    }
#endif
    {
        // res_data[i] = _local_to_global[code_data[i]];
        SIMDGather::gather(res_data.data(), _local_to_global, code_data.data(), _dict_size, size);
    }

    if (output_nullable) {
        // reserve null data
        auto word_nulls = down_cast<NullableColumn*>(words)->mutable_null_column();
        down_cast<NullableColumn*>(words)->set_has_null(codes.has_null());
        const auto& null_data = down_cast<const NullableColumn&>(codes).immutable_null_column_data();
        word_nulls->resize(0);
        for (size_t i = 0; i < size; ++i) {
            word_nulls->append(null_data[i]);
        }
        if (codes.has_null()) {
            // assign code 0 if input data is null
            for (size_t i = 0; i < size; ++i) {
                res_data[i] = null_data[i] == 0 ? res_data[i] : 0;
            }
        }
    }

    return Status::OK();
}

Status GlobalDictCodeColumnIterator::build_code_convert_map(ColumnIterator* file_column_iter,
                                                            GlobalDictMap* global_dict,
                                                            std::vector<int16_t>* code_convert_map) {
    DCHECK(file_column_iter->all_page_dict_encoded());

    int dict_size = file_column_iter->dict_size();

    auto column = BinaryColumn::create();

    int dict_codes[dict_size];
    for (int i = 0; i < dict_size; ++i) {
        dict_codes[i] = i;
    }

    RETURN_IF_ERROR(file_column_iter->decode_dict_codes(dict_codes, dict_size, column.get()));

    code_convert_map->resize(dict_size + 2);
    std::fill(code_convert_map->begin(), code_convert_map->end(), 0);
    auto* local_to_global = code_convert_map->data() + 1;

    for (int i = 0; i < dict_size; ++i) {
        auto slice = column->get_slice(i);
        auto res = global_dict->find(slice);
        if (res == global_dict->end()) {
            if (slice.size > 0) {
                return Status::InternalError(fmt::format("not found slice:{} in global dict", slice.to_string()));
            }
        } else {
            local_to_global[dict_codes[i]] = res->second;
        }
    }
    return Status::OK();
}

MutableColumnPtr GlobalDictCodeColumnIterator::_new_local_dict_col(Column* src) {
    MutableColumnPtr res = Int32Column::create();
    auto code_data = ColumnHelper::get_data_column(src);
    if (code_data->is_array()) {
        res = ArrayColumn::create(NullableColumn::create(std::move(res), NullColumn::create()), UInt32Column::create());
    }

    if (src->is_nullable()) {
        res = NullableColumn::create(std::move(res), NullColumn::create());
    }
    return res;
}

void GlobalDictCodeColumnIterator::_swap_null_columns(Column* src, Column* dst) {
    DCHECK_EQ(src->is_nullable(), dst->is_nullable());
    if (src->is_nullable()) {
        auto src_column = down_cast<NullableColumn*>(src);
        auto dst_column = down_cast<NullableColumn*>(dst);
        dst_column->null_column_data().swap(src_column->null_column_data());
        dst_column->set_has_null(src_column->has_null());
    }
}

} // namespace starrocks
