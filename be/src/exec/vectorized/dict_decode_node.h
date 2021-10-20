// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#pragma once

#include "column/chunk.h"
#include "exec/exec_node.h"
#include "exec/olap_common.h"
#include "runtime/global_dicts.h"

namespace starrocks::vectorized {

class DictDecodeNode final : public ExecNode {
public:
    DictDecodeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~DictDecodeNode() override {}

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;

    template <PrimitiveType primitive_type, typename Dict, PrimitiveType result_primitive_type>
    struct DictDecoder {
        using FieldType = RunTimeCppType<primitive_type>;
        using ResultColumnType = RunTimeColumnType<result_primitive_type>;
        using ColumnType = RunTimeColumnType<primitive_type>;
        Dict dict;
        Status decode(vectorized::Column* in, vectorized::Column* out) {
            DCHECK(in != nullptr);
            DCHECK(out != nullptr);
            if (!in->is_nullable()) {
                auto res_column = down_cast<ResultColumnType*>(out);
                auto column = down_cast<ColumnType*>(in);
                for (size_t i = 0; i < in->size(); i++) {
                    FieldType key = column->get_data()[i];
                    auto iter = dict.find(key);
                    if (iter == dict.end()) {
                        return Status::InternalError(
                                fmt::format("Dict Decode failed, Dict can't take cover all key :{}", key));
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
                    auto iter = dict.find(key);
                    if (iter == dict.end()) {
                        return Status::InternalError(
                                fmt::format("Dict Decode failed, Dict can't take cover all key :{}", key));
                    }
                    res_data_column->append(iter->second);
                } else {
                    res_data_column->append_default();
                    res_column->set_null(i);
                }
            }
            return Status::OK();
        }
    };

protected:
    void debug_string(int indentation_level, std::stringstream* out) const override { *out << "DictDecodeNode"; }

private:
    void _init_counter();

    using DefaultDecoder = std::unique_ptr<DictDecoder<TYPE_INT, RGlobalDictMap, TYPE_VARCHAR>>;
    std::shared_ptr<vectorized::Chunk> _input_chunk;
    std::vector<int32_t> _encode_column_cids;
    std::vector<int32_t> _decode_column_cids;
    std::vector<DefaultDecoder> _decoders;
    TDecodeNode _decode_node;

    // profile
    RuntimeProfile::Counter* _decode_timer = nullptr;
};

} // namespace starrocks::vectorized
