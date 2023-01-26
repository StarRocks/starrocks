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

#include "storage/empty_iterator.h"

namespace starrocks {

class EmptyIterator final : public ChunkIterator {
public:
    explicit EmptyIterator(Schema schema, int chunk_size) : ChunkIterator(std::move(schema), chunk_size) {}

    void close() override {}

protected:
    Status do_get_next(Chunk* chunk) override { return Status::EndOfFile("end of empty iterator"); }
    Status do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) override {
        return Status::EndOfFile("end of empty iterator");
    }
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override {
        return Status::EndOfFile("end of empty iterator");
    }
};

ChunkIteratorPtr new_empty_iterator(Schema&& schema, int chunk_size) {
    return std::make_shared<EmptyIterator>(std::move(schema), chunk_size);
}

ChunkIteratorPtr new_empty_iterator(const Schema& schema, int chunk_size) {
    return std::make_shared<EmptyIterator>(schema, chunk_size);
}

} // namespace starrocks
