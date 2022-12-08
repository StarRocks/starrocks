// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/chunk.h"
#include "serde/column_array_serde.h"

namespace starrocks::vectorized {

class ChunkExtraColumnsData;
using ChunkExtraColumnsDataPtr = std::shared_ptr<ChunkExtraColumnsData>;

class ChunkExtraColumnsData : public ChunkExtraData {
public:
    ChunkExtraColumnsData(std::vector<ChunkExtraDataMeta> extra_metas, Columns columns)
            : _data_metas(std::move(extra_metas)), _columns(std::move(columns)) {}
    virtual ~ChunkExtraColumnsData() = default;

    Columns columns() const { return _columns; }

    virtual std::vector<ChunkExtraDataMeta> chunk_data_metas() const override { return _data_metas; }

    void filter(const Buffer<uint8_t>& selection) const override {
        for (auto& col : _columns) {
            col->filter(selection);
        }
    }

    void filter_range(const Buffer<uint8_t>& selection, size_t from, size_t to) const override {
        for (auto& col : _columns) {
            col->filter_range(selection, from, to);
        }
    }

    ChunkExtraDataPtr clone_empty(size_t size) const override {
        Columns columns(_columns.size());
        for (size_t i = 0; i < _columns.size(); i++) {
            columns[i] = _columns[i]->clone_empty();
            columns[i]->reserve(size);
        }
        auto extra_data_metas = _data_metas;
        return std::make_shared<ChunkExtraColumnsData>(extra_data_metas, columns);
    }

    void append(const ChunkExtraDataPtr& src, size_t offset, size_t count) override {
        DCHECK(src);
        auto src_columns = dynamic_cast<ChunkExtraColumnsData*>(src.get())->columns();
        DCHECK_EQ(src_columns.size(), _columns.size());
        for (size_t i = 0; i < _columns.size(); ++i) {
            _columns[i]->append(*src_columns[i], offset, count);
        }
    }

    void append_selective(const ChunkExtraDataPtr& src, const uint32_t* indexes, uint32_t from,
                          uint32_t size) override {
        DCHECK(src);
        auto src_columns = dynamic_cast<ChunkExtraColumnsData*>(src.get())->columns();
        DCHECK_EQ(src_columns.size(), _columns.size());
        for (size_t i = 0; i < _columns.size(); ++i) {
            _columns[i]->append_selective(*src_columns[i], indexes, from, size);
        }
    }

    // TODO: only support encode_level=0
    int64_t max_serialized_size(const int encode_level = 0) override {
        DCHECK_EQ(encode_level, 0);
        int64_t serialized_size = 0;
        for (auto i = 0; i < _columns.size(); ++i) {
            serialized_size += serde::ColumnArraySerde::max_serialized_size(*_columns[i], 0);
        }
        return serialized_size;
    }

    uint8_t* serialize(uint8_t* buff, bool sorted = false, const int encode_level = 0) override {
        DCHECK_EQ(encode_level, 0);
        for (auto i = 0; i < _columns.size(); ++i) {
            buff = serde::ColumnArraySerde::serialize(*_columns[i], buff, sorted, encode_level);
        }
        return buff;
    }

    const uint8_t* deserialize(const uint8_t* buff, bool sorted = false, const int encode_level = 0) override {
        DCHECK_EQ(encode_level, 0);
        for (auto i = 0; i < _columns.size(); ++i) {
            buff = serde::ColumnArraySerde::deserialize(buff, _columns[i].get(), sorted, encode_level);
        }
        return buff;
    }

private:
    std::vector<ChunkExtraDataMeta> _data_metas;
    Columns _columns;
};
} // namespace starrocks::vectorized