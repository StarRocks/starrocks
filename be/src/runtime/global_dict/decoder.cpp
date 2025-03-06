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

#include "runtime/global_dict/decoder.h"

#include <utility>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/const_column.h"
#include "column/datum.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "runtime/global_dict/config.h"
#include "runtime/global_dict/types.h"
#include "runtime/types.h"

namespace starrocks {

template <typename Dict>
class GlobalDictDecoderBase : public GlobalDictDecoder {
public:
    using DictCppType = RunTimeCppType<LowCardDictType>;
    using DictColumnType = RunTimeColumnType<LowCardDictType>;
    using StringColumnType = RunTimeColumnType<TYPE_VARCHAR>;

    GlobalDictDecoderBase(Dict dict) : _dict(std::move(dict)) {}

    Status decode_string(const Column* in, Column* out) override;

    Status decode_array(const Column* in, Column* out) override;

private:
    Dict _dict;
};

template <typename Dict>
Status GlobalDictDecoderBase<Dict>::decode_array(const Column* in, Column* out) {
    DCHECK(in != nullptr);
    DCHECK(out != nullptr);

    TypeDescriptor stringType;
    stringType.type = TYPE_VARCHAR;
    if (in->is_constant()) {
        const auto* const_column = down_cast<const ConstColumn*>(in);
        const auto* in_array = down_cast<const ArrayColumn*>(const_column->data_column().get());
        auto in_element = in_array->elements_column();
        auto in_offsets = in_array->offsets_column();

        auto* out_array = down_cast<ArrayColumn*>(out);
        auto out_element = out_array->elements_column();
        auto out_offsets = out_array->offsets_column();
        out_offsets->swap_column(*in_offsets);
        return decode_string(in_element.get(), out_element.get());
    } else if (in->is_nullable()) {
        const auto* in_nullable = down_cast<const NullableColumn*>(in);
        const auto* in_array = down_cast<const ArrayColumn*>(in_nullable->data_column().get());
        auto in_null = in_nullable->null_column();
        auto in_element = in_array->elements_column();
        auto in_offsets = in_array->offsets_column();

        auto* out_nullable = down_cast<NullableColumn*>(out);
        auto* out_array = down_cast<ArrayColumn*>(out_nullable->data_column().get());
        auto out_null = out_nullable->null_column();
        auto out_element = out_array->elements_column();
        auto out_offsets = out_array->offsets_column();
        out_offsets->swap_column(*in_offsets);
        out_null->swap_column(*in_null);
        out_nullable->set_has_null(in_nullable->has_null());
        return decode_string(in_element.get(), out_element.get());
    } else {
        const auto* in_array = down_cast<const ArrayColumn*>(in);
        auto in_element = in_array->elements_column();
        auto in_offsets = in_array->offsets_column();

        auto* out_array = down_cast<ArrayColumn*>(out);
        auto out_element = out_array->elements_column();
        auto out_offsets = out_array->offsets_column();
        out_offsets->swap_column(*in_offsets);
        return decode_string(in_element.get(), out_element.get());
    }

    return Status::OK();
}

template <typename Dict>
Status GlobalDictDecoderBase<Dict>::decode_string(const Column* in, Column* out) {
    DCHECK(in != nullptr);
    DCHECK(out != nullptr);

    // handle const columns
    if (in->is_constant()) {
        auto id = in->get(0).get<DictCppType>();
        auto iter = _dict.find(id);
        if (iter == _dict.end()) {
            return Status::InternalError(fmt::format("Dict Decode failed, Dict can't take cover all key :{}", id));
        }
        out->append_datum(Datum(iter->second));
        out->assign(in->size(), 0);
        return Status::OK();
    }

    if (!in->is_nullable()) {
        auto* res_column = down_cast<StringColumnType*>(out);
        const auto* column = down_cast<const DictColumnType*>(in);
        for (size_t i = 0; i < in->size(); i++) {
            DictCppType key = column->get_data()[i];
            auto iter = _dict.find(key);
            if (iter == _dict.end()) {
                return Status::InternalError(fmt::format("Dict Decode failed, Dict can't take cover all key :{}", key));
            }
            res_column->append(iter->second);
        }
        return Status::OK();
    }

    const auto* column = down_cast<const NullableColumn*>(in);
    auto* res_column = down_cast<NullableColumn*>(out);
    res_column->null_column_data().resize(in->size());

    auto* res_data_column = down_cast<StringColumnType*>(res_column->data_column().get());
    const auto* data_column = down_cast<const DictColumnType*>(column->data_column().get());

    for (size_t i = 0; i < in->size(); i++) {
        if (column->null_column_data()[i] == 0) {
            res_column->null_column_data()[i] = 0;
            DictCppType key = data_column->get_data()[i];
            auto iter = _dict.find(key);
            if (iter == _dict.end()) {
                return Status::InternalError(fmt::format("Dict Decode failed, Dict can't take cover all key :{}", key));
            }
            res_data_column->append(iter->second);
        } else {
            res_data_column->append_default();
            res_column->set_null(i);
        }
    }
    return Status::OK();
}

template <typename DictType>
GlobalDictDecoderPtr create_global_dict_decoder(const DictType& dict) {
    return std::make_unique<GlobalDictDecoderBase<DictType>>(dict);
}

// explicit instantiation
template GlobalDictDecoderPtr create_global_dict_decoder<RGlobalDictMap>(const RGlobalDictMap& dict);

} // namespace starrocks
