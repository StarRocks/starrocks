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

#include <vector>

#include "storage/rowset/common.h"
#include "types/bitmap_value_detail.h"
#include "util/bitmap_update_context.h"

namespace starrocks {

class Encoder;

class PostingList {
public:
    PostingList(const PostingList& rhs) = delete;
    PostingList& operator=(const PostingList& rhs) = delete;

    PostingList(PostingList&& rhs) noexcept;
    PostingList& operator=(PostingList&& rhs) noexcept;

    explicit PostingList();
    ~PostingList();

    void add_posting(rowid_t doc_id, rowid_t pos);

    uint32_t get_num_doc_ids() const;

    roaring::Roaring get_all_doc_ids() const;

    roaring::Roaring get_positions(rowid_t doc_id) const;

private:
    std::unique_ptr<BitmapUpdateContextRefOrSingleValue<uint64_t>> _postings = nullptr;
};

} // namespace starrocks
