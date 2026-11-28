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

#include <arrow/c/abi.h>
#include <paimon/global_index/global_indexer.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace starrocks::paimon_global_index {

inline const char* arrow_key_format(std::string_view type) {
    if (type == "boolean") return "b";
    if (type == "tinyint") return "c";
    if (type == "smallint") return "s";
    if (type == "int") return "i";
    if (type == "bigint") return "l";
    if (type == "string") return "u";
    return nullptr;
}

// Validate the opaque BTree metadata's length envelope before passing it to SDK
// deserialization. Preserve its bytes, including legacy and null-boundary flags.
inline bool valid_btree_metadata(std::string_view bytes) {
    if (bytes.size() < 9 || bytes.size() > 64 * 1024) return false;
    size_t offset = 0;
    for (int key = 0; key < 2; ++key) {
        if (bytes.size() - offset < 4) return false;
        // BTree metadata follows the SDK's native byte order, just like its keys.
        int32_t length = 0;
        std::memcpy(&length, bytes.data() + offset, sizeof(length));
        offset += sizeof(length);
        if (length < 0 || static_cast<size_t>(length) > bytes.size() - offset) return false;
        offset += length;
    }
    return offset < bytes.size();
}

inline paimon::Result<std::shared_ptr<paimon::GlobalIndexReader>> create_segment_reader(
        const paimon::GlobalIndexer& indexer, const std::string& field_name, const std::string& field_type,
        const std::shared_ptr<paimon::GlobalIndexFileReader>& file_reader,
        const std::vector<paimon::GlobalIndexIOMeta>& files, const std::shared_ptr<paimon::MemoryPool>& pool) {
    // Only Arrow's C data interface crosses into the lazily loaded SDK.
    ArrowSchema item{};
    item.format = "f";
    item.name = "item";
    item.flags = ARROW_FLAG_NULLABLE;
    item.release = [](ArrowSchema* value) { value->release = nullptr; };
    ArrowSchema* item_child = &item;
    ArrowSchema field{};
    field.format = field_type == "array<float>" ? "+l" : arrow_key_format(field_type);
    if (field.format == nullptr) return paimon::Status::Invalid("Unsupported global index field type");
    if (field_type == "array<float>") {
        field.n_children = 1;
        field.children = &item_child;
    }
    field.name = field_name.c_str();
    field.flags = ARROW_FLAG_NULLABLE;
    field.release = [](ArrowSchema* value) {
        if (value->n_children != 0 && value->children[0]->release != nullptr) {
            value->children[0]->release(value->children[0]);
        }
        value->release = nullptr;
    };
    ArrowSchema* child = &field;
    ArrowSchema schema{};
    schema.format = "+s";
    schema.n_children = 1;
    schema.children = &child;
    schema.release = [](ArrowSchema* value) {
        if (value->children[0]->release != nullptr) value->children[0]->release(value->children[0]);
        value->release = nullptr;
    };
    auto result = indexer.CreateReader(&schema, file_reader, files, pool);
    if (schema.release != nullptr) schema.release(&schema);
    return result;
}

// Public SDK file readers return segment-local IDs. Offset each segment before
// unioning a column's results, so AND/OR between columns uses global IDs.
class SegmentedIndexReader final : public paimon::GlobalIndexReader {
public:
    using Result = paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>>;
    void add(std::shared_ptr<paimon::GlobalIndexReader> reader, int64_t offset) {
        _segments.emplace_back(std::move(reader), offset);
    }
    Result VisitIsNotNull() override {
        return apply([&](const auto& reader) { return reader->VisitIsNotNull(); });
    }
    Result VisitIsNull() override {
        return apply([&](const auto& reader) { return reader->VisitIsNull(); });
    }
    Result VisitEqual(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitEqual(literal); });
    }
    Result VisitNotEqual(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitNotEqual(literal); });
    }
    Result VisitLessThan(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitLessThan(literal); });
    }
    Result VisitLessOrEqual(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitLessOrEqual(literal); });
    }
    Result VisitGreaterThan(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitGreaterThan(literal); });
    }
    Result VisitGreaterOrEqual(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitGreaterOrEqual(literal); });
    }
    Result VisitStartsWith(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitStartsWith(literal); });
    }
    Result VisitEndsWith(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitEndsWith(literal); });
    }
    Result VisitContains(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitContains(literal); });
    }
    Result VisitLike(const paimon::Literal& literal) override {
        return apply([&](const auto& reader) { return reader->VisitLike(literal); });
    }
    Result VisitIn(const std::vector<paimon::Literal>& literals) override {
        return apply([&](const auto& reader) { return reader->VisitIn(literals); });
    }
    Result VisitNotIn(const std::vector<paimon::Literal>& literals) override {
        return apply([&](const auto& reader) { return reader->VisitNotIn(literals); });
    }
    paimon::Result<std::shared_ptr<paimon::ScoredGlobalIndexResult>> VisitVectorSearch(
            const std::shared_ptr<paimon::VectorSearch>&) override {
        return paimon::Status::Invalid("Scalar BTree reader does not support vector search");
    }
    Result VisitFullTextSearch(const std::shared_ptr<paimon::FullTextSearch>&) override {
        return paimon::Status::Invalid("Scalar BTree reader does not support full-text search");
    }
    bool IsThreadSafe() const override { return false; }
    std::string GetIndexType() const override { return "btree"; }

private:
    template <class Action>
    Result apply(const Action& action) {
        std::shared_ptr<paimon::GlobalIndexResult> merged;
        for (const auto& [reader, offset] : _segments) {
            auto local = action(reader);
            if (!local.ok()) return local.status();
            if (local.value() == nullptr) return paimon::Status::Invalid("BTree reader returned a null result");
            auto global = local.value()->AddOffset(offset);
            if (!global.ok()) return global.status();
            if (merged == nullptr) {
                merged = std::move(global).value();
            } else {
                auto combined = merged->Or(global.value());
                if (!combined.ok()) return combined.status();
                merged = std::move(combined).value();
            }
        }
        return merged;
    }
    std::vector<std::pair<std::shared_ptr<paimon::GlobalIndexReader>, int64_t>> _segments;
};

} // namespace starrocks::paimon_global_index
