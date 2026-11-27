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

#include <paimon/global_index/global_index_result.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "base/statusor.h"
#include "connector/hive/paimon/paimon_query_allocator.h"

namespace starrocks::paimon_global_index {

inline int64_t ann_diskann_search_list_size(int32_t k, int64_t segment_rows) {
    // Small SQL limits still need a traversal window to find neighbors in the graph.
    return std::min(segment_rows, std::max<int64_t>(128, (3LL * k + 1) / 2));
}

inline bool ann_string(const rapidjson::Value& value, const char* key, std::string_view expected) {
    return value.HasMember(key) && value[key].IsString() &&
           std::string_view(value[key].GetString(), value[key].GetStringLength()) == expected;
}

inline Status validate_ann_vector(const rapidjson::Value& request) {
    const bool filtered = ann_string(request, "kind", "filtered_top_n");
    if ((!filtered && !ann_string(request, "kind", "unfiltered_top_n")) ||
        (!filtered &&
         (request.HasMember("predicate") || request.HasMember("indexes") || request.HasMember("partitionRanges"))) ||
        (filtered && (request.HasMember("predicate") != request.HasMember("indexes") ||
                      (!request.HasMember("predicate") && !request.HasMember("partitionRanges")))) ||
        !request.HasMember("vector") || !request["vector"].IsObject()) {
        return Status::InvalidArgument("Invalid Paimon ANN filter envelope");
    }
    const auto& vector = request["vector"];
    if (!vector.HasMember("fieldId") || !vector["fieldId"].IsInt() || vector["fieldId"].GetInt() < 0 ||
        !vector.HasMember("fieldName") || !vector["fieldName"].IsString() ||
        vector["fieldName"].GetStringLength() == 0 ||
        std::memchr(vector["fieldName"].GetString(), '\0', vector["fieldName"].GetStringLength()) != nullptr ||
        !ann_string(vector, "provider", "lumina") ||
        !(ann_string(vector, "metric", "l2") || ann_string(vector, "metric", "cosine") ||
          ann_string(vector, "metric", "inner_product")) ||
        !vector.HasMember("k") || !vector["k"].IsInt() || vector["k"].GetInt() <= 0 ||
        !vector.HasMember("queryVector") || !vector["queryVector"].IsArray() || vector["queryVector"].Empty() ||
        vector["queryVector"].Size() > 4096) {
        return Status::InvalidArgument("Invalid Paimon ANN vector binding");
    }
    if (4 + 12LL * vector["k"].GetInt() > request["resultBytesLimit"].GetInt64()) {
        return Status::InvalidArgument("Paimon ANN TopN exceeds the FE collection budget");
    }
    for (const auto& value : vector["queryVector"].GetArray()) {
        if (!value.IsNumber() || !std::isfinite(value.GetDouble()) ||
            std::fabs(value.GetDouble()) > std::numeric_limits<float>::max()) {
            return Status::InvalidArgument("Paimon ANN query must contain finite float32 values");
        }
    }
    return Status::OK();
}

inline Status validate_ann_partitions(const rapidjson::Value& request, int64_t from, int64_t to) {
    if (!request.HasMember("partitionRanges")) return Status::OK();
    const auto& ranges = request["partitionRanges"];
    if (!ranges.IsArray() || ranges.Empty()) return Status::InvalidArgument("Invalid ANN partition ranges");
    int64_t previous_to = -1;
    bool contained = false;
    for (const auto& range : ranges.GetArray()) {
        if (!range.IsObject() || !range.HasMember("from") || !range["from"].IsInt64() || !range.HasMember("to") ||
            !range["to"].IsInt64() || range["from"].GetInt64() < 0 ||
            range["to"].GetInt64() < range["from"].GetInt64() ||
            range["to"].GetInt64() == std::numeric_limits<int64_t>::max() || range["from"].GetInt64() <= previous_to) {
            return Status::InvalidArgument("ANN partition ranges must be sorted, disjoint and finite");
        }
        contained |= from >= range["from"].GetInt64() && to <= range["to"].GetInt64();
        previous_to = range["to"].GetInt64();
    }
    return contained ? Status::OK() : Status::InvalidArgument("ANN shard crosses selected partition boundaries");
}

// Query-bound immutable membership used concurrently by SDK search workers.
// Consecutive global row IDs are compressed; the explicit bound limits an
// adversarial sparse bitmap independently of the query MemTracker.
class AnnCandidateRanges {
public:
    explicit AnnCandidateRanges(std::shared_ptr<MemTracker> tracker)
            : _ranges(PaimonQueryAllocator<std::pair<int64_t, int64_t>>(std::move(tracker))) {}
    static constexpr size_t MAX_RANGES = 65536;
    Status add(int64_t id) {
        if (id < 0 || (!_ranges.empty() && id <= _ranges.back().second)) {
            return Status::InternalError("ANN scalar candidates must be distinct, ascending global row IDs");
        }
        if (!_ranges.empty() && _ranges.back().second != std::numeric_limits<int64_t>::max() &&
            id == _ranges.back().second + 1) {
            _ranges.back().second = id;
        } else {
            if (_ranges.size() == MAX_RANGES) return Status::MemoryLimitExceeded("ANN candidate range budget exceeded");
            _ranges.emplace_back(id, id);
        }
        return Status::OK();
    }
    bool empty() const { return _ranges.empty(); }
    bool allows_local(int64_t local, int64_t from, int64_t to) const {
        if (local < 0 || from < 0 || to < from || local > to - from) return false;
        const int64_t global = from + local;
        auto next = std::upper_bound(_ranges.begin(), _ranges.end(), global,
                                     [](int64_t id, const auto& range) { return id < range.first; });
        return next != _ranges.begin() && global <= std::prev(next)->second;
    }

private:
    std::vector<std::pair<int64_t, int64_t>, PaimonQueryAllocator<std::pair<int64_t, int64_t>>> _ranges;
};

// Preserve original metadata bytes. These checks bound the public SDK parser and
// reject provider configurations outside the initial unfiltered ANN contract.
inline Status validate_ann_metadata(std::string_view bytes, const rapidjson::Value& vector) {
    if (bytes.empty() || bytes.size() > 64 * 1024) {
        return Status::InvalidArgument("Invalid Paimon ANN metadata size");
    }
    rapidjson::Document metadata;
    if (metadata.Parse(bytes.data(), bytes.size()).HasParseError() || !metadata.IsObject()) {
        return Status::InvalidArgument("Invalid Paimon ANN metadata JSON");
    }
    std::set<std::string_view> keys;
    for (const auto& member : metadata.GetObject()) {
        std::string_view key(member.name.GetString(), member.name.GetStringLength());
        if (!member.value.IsString() || !keys.insert(key).second || key.starts_with("extension.build.tag.") ||
            key == "extension.search_with_tag") {
            return Status::InvalidArgument("Unsupported Paimon ANN metadata options");
        }
    }
    if (!ann_string(metadata, "index.dimension", std::to_string(vector["queryVector"].Size())) ||
        !(ann_string(metadata, "index.type", "bruteforce") || ann_string(metadata, "index.type", "diskann")) ||
        !ann_string(metadata, "distance.metric",
                    std::string_view(vector["metric"].GetString(), vector["metric"].GetStringLength())) ||
        !ann_string(metadata, "encoding.type", "rawf32")) {
        return Status::InvalidArgument("Paimon ANN metadata does not match the vector request");
    }
    return Status::OK();
}

inline StatusOr<std::string> serialize_ann_result(const std::shared_ptr<paimon::ScoredGlobalIndexResult>& result,
                                                  int32_t k, int64_t from, int64_t to, bool inner_product,
                                                  int64_t byte_limit, const std::function<Status()>& check) {
    if (result == nullptr) return Status::InternalError("Paimon ANN returned a null scored result");
    if (k <= 0 || from < 0 || to < from || to == std::numeric_limits<int64_t>::max() || 4 + 12LL * k > byte_limit) {
        return Status::InvalidArgument("Invalid Paimon ANN result bounds");
    }
    auto iterator = result->CreateScoredIterator();
    if (!iterator.ok()) return Status::InternalError(iterator.status().ToString());
    if (iterator.value() == nullptr) return Status::InternalError("Paimon ANN returned a null scored iterator");
    std::vector<std::pair<int64_t, float>> hits;
    std::set<int64_t> ids;
    while (iterator.value()->HasNext()) {
        RETURN_IF_ERROR(check());
        auto [local, score] = iterator.value()->NextWithScore();
        if (hits.size() >= static_cast<size_t>(k) || local < 0 || local > to - from || !std::isfinite(score) ||
            !ids.insert(local).second) {
            return Status::InternalError("Invalid Paimon ANN scored row");
        }
        hits.emplace_back(from + local, inner_product ? -score : score);
    }
    std::sort(hits.begin(), hits.end(), [](const auto& a, const auto& b) {
        return a.second == b.second ? a.first < b.first : a.second < b.second;
    });
    std::string bytes;
    bytes.reserve(4 + 12 * hits.size());
    auto append_le = [&](uint64_t value, int length) {
        for (int i = 0; i < length; ++i) bytes.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
    };
    append_le(hits.size(), 4);
    for (const auto& [id, score] : hits) {
        append_le(static_cast<uint64_t>(id), 8);
        uint32_t bits;
        static_assert(sizeof(bits) == sizeof(score));
        std::memcpy(&bits, &score, sizeof(bits));
        append_le(bits, 4);
    }
    RETURN_IF_ERROR(check());
    return bytes;
}

} // namespace starrocks::paimon_global_index
