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

#include "storage/index/vector/vector_ann_index.h"

#include "types/bitmap_value_detail.h"

namespace starrocks {

BitmapRowIdFilter::BitmapRowIdFilter(std::unique_ptr<detail::Roaring64Map> bitmap) : _bitmap(std::move(bitmap)) {}

BitmapRowIdFilter::~BitmapRowIdFilter() = default;

bool BitmapRowIdFilter::is_member(int64_t row_id) const {
    return _bitmap && _bitmap->contains(static_cast<uint64_t>(row_id));
}

int64_t BitmapRowIdFilter::cardinality() const {
    return _bitmap ? static_cast<int64_t>(_bitmap->cardinality()) : 0;
}

// Default filtered_search: oversample + post-filter.
Status VectorAnnIndex::filtered_search(const VectorQuery& query, const RowIdFilter& filter, VectorAnnResult* result) {
    constexpr int32_t kDefaultOversampleFactor = 3;
    VectorQuery oversampled = query;
    oversampled.top_k = query.top_k * kDefaultOversampleFactor;

    VectorAnnResult raw;
    RETURN_IF_ERROR(search(oversampled, &raw));

    result->clear();
    result->reserve(query.top_k);
    for (int32_t i = 0; i < raw.result_count && result->result_count < query.top_k; ++i) {
        if (filter.is_member(raw.row_ids[i])) {
            result->add(raw.row_ids[i], raw.scores[i]);
        }
    }
    return Status::OK();
}

} // namespace starrocks
