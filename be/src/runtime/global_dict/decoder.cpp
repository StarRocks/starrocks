// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/global_dict/decoder.h"

#include "column/column_builder.h"
#include "column/type_traits.h"
#include "gutil/casts.h"
#include "runtime/global_dict/config.h"
#include "runtime/global_dict/types.h"

namespace starrocks::vectorized {

template <PrimitiveType type, typename Dict, PrimitiveType result_type>
class GlobalDictDecoderBase : public GlobalDictDecoder {
public:
    using FieldType = RunTimeCppType<type>;
    using ResultColumnType = RunTimeColumnType<result_type>;
    using ColumnType = RunTimeColumnType<type>;

    GlobalDictDecoderBase(const Dict& dict) : _dict(dict) {}

    Status decode(vectorized::Column* in, vectorized::Column* out) override;

private:
    Dict _dict;
};

template <PrimitiveType type, typename Dict, PrimitiveType result_type>
Status GlobalDictDecoderBase<type, Dict, result_type>::decode(vectorized::Column* in, vectorized::Column* out) {
    DCHECK(in != nullptr);
    DCHECK(out != nullptr);

    // handle const columns
    if (in->is_constant()) {
        if (in->only_null()) {
            bool res = out->append_nulls(in->size());
            DCHECK(res);
            return Status::OK();
        } else {
            out->append_datum(in->get(0));
            out->assign(in->size(), 0);
            return Status::OK();
        }
    }

    if (!in->is_nullable()) {
        auto res_column = down_cast<ResultColumnType*>(out);
        auto column = down_cast<ColumnType*>(in);
        for (size_t i = 0; i < in->size(); i++) {
            FieldType key = column->get_data()[i];
            auto iter = _dict.find(key);
            if (iter == _dict.end()) {
                return Status::InternalError(fmt::format("Dict Decode failed, Dict can't take cover all key :{}", key));
            }
            res_column->append(iter->second);
        }
        return Status::OK();
    }

    auto column = down_cast<NullableColumn*>(in);
    auto res_column = down_cast<NullableColumn*>(out);
    res_column->null_column_data().resize(in->size());

    auto res_data_column = down_cast<ResultColumnType*>(res_column->data_column().get());
    auto data_column = down_cast<ColumnType*>(column->data_column().get());

    for (size_t i = 0; i < in->size(); i++) {
        if (column->null_column_data()[i] == 0) {
            res_column->null_column_data()[i] = 0;
            FieldType key = data_column->get_data()[i];
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
    return std::make_unique<GlobalDictDecoderBase<LowCardDictType, DictType, TYPE_VARCHAR>>(dict);
}

// explicit instantiation
template GlobalDictDecoderPtr create_global_dict_decoder<RGlobalDictMap>(const RGlobalDictMap& dict);

} // namespace starrocks::vectorized
