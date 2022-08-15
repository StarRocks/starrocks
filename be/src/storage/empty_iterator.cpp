// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/empty_iterator.h"

namespace starrocks::vectorized {

class EmptyIterator final : public ChunkIterator {
public:
    explicit EmptyIterator(vectorized::Schema schema, int chunk_size) : ChunkIterator(std::move(schema), chunk_size) {}

    void close() override {}

protected:
    Status do_get_next(Chunk* chunk) override { return Status::EndOfFile("end of empty iterator"); }
    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override {
        return Status::EndOfFile("end of empty iterator");
    }
    Status do_get_next(Chunk* chunk, vector<RowSourceMask>* source_masks) override {
        return Status::EndOfFile("end of empty iterator");
    }
};

ChunkIteratorPtr new_empty_iterator(vectorized::Schema&& schema, int chunk_size) {
    return std::make_shared<EmptyIterator>(std::move(schema), chunk_size);
}

ChunkIteratorPtr new_empty_iterator(const vectorized::Schema& schema, int chunk_size) {
    return std::make_shared<EmptyIterator>(schema, chunk_size);
}

} // namespace starrocks::vectorized
