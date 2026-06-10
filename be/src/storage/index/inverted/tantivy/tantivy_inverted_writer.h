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

#pragma once

#include <memory>
#include <string>

#include "common/status.h"
#include "storage/index/inverted/inverted_writer.h"
#include "storage/index/inverted/tantivy/tantivy_ffi_guards.h"
#include "storage/tablet_index.h"
#include "types/logical_type.h"

namespace starrocks {

class TantivyInvertedWriter : public InvertedWriter {
public:
    static Status create(const TypeInfoPtr& typeinfo, const std::string& field_name, const std::string& path,
                         TabletIndex* tablet_index, std::unique_ptr<InvertedWriter>* res);

    ~TantivyInvertedWriter() noexcept override;

    Status init() override;
    void add_values(const void* values, size_t count) override;
    void add_nulls(uint32_t count) override;
    Status finish(WritableFile* wfile, ColumnMetaPB* meta) override;
    bool need_compound() const override { return true; }
    StatusOr<CompoundIndexEntry> finish_compound(ColumnMetaPB* meta) override;
    uint64_t size() const override;

private:
    TantivyInvertedWriter(std::string field_name, std::string temp_dir, std::string tokenizer, int64_t index_id);

    std::string _field_name;
    std::string _temp_dir;
    std::string _tokenizer;
    int64_t _index_id = 0;

    TantivyWriterGuard _writer;
    roaring::Roaring _null_bitmap;
    uint32_t _rid = 0;
    uint64_t _estimated_size = 0;
    Status _error_status;
    // Set to true once finish_compound has handed _temp_dir's files off to the
    // compound packing flow. Until then, the destructor must clean up the dir
    // so a failed write doesn't leave a stale tantivy index that would cause
    // IndexAlreadyExists on the next attempt.
    bool _packed = false;
};

} // namespace starrocks
