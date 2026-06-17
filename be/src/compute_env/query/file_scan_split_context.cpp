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

#include "compute_env/query/file_scan_split_context.h"

namespace starrocks {

void merge_file_scan_split_tasks(std::vector<FileScanSplitContextPtr>* split_tasks, size_t max_split_size) {
    if (split_tasks == nullptr || split_tasks->size() < 2) return;

    // Prerequisites: ranges are sorted, and no ranges overlap.
    std::vector<FileScanSplitContextPtr> new_split_tasks;

    auto do_merge = [&](size_t start, size_t end) {
        auto start_ctx = (*split_tasks)[start].get();
        auto end_ctx = (*split_tasks)[end].get();
        auto new_ctx = start_ctx->clone();
        new_ctx->start_offset = start_ctx->start_offset;
        new_ctx->end_offset = end_ctx->end_offset;
        new_split_tasks.emplace_back(std::move(new_ctx));
    };

    size_t head = 0;
    for (size_t i = 1; i < split_tasks->size(); i++) {
        bool cut = false;

        auto prev_ctx = (*split_tasks)[i - 1].get();
        auto ctx = (*split_tasks)[i].get();
        auto head_ctx = (*split_tasks)[head].get();

        if ((ctx->start_offset != prev_ctx->end_offset) ||
            (ctx->end_offset - head_ctx->start_offset > max_split_size)) {
            cut = true;
        }

        if (cut) {
            do_merge(head, i - 1);
            head = i;
        }
    }
    do_merge(head, split_tasks->size() - 1);

    // If the tail range is small and consecutive, merge it into the previous range.
    size_t new_size = new_split_tasks.size();
    if (new_size >= 2) {
        auto tail_ctx = new_split_tasks[new_size - 1].get();
        size_t tail_size = tail_ctx->end_offset - tail_ctx->start_offset;
        if ((tail_size * 2) < max_split_size) {
            auto last_ctx = new_split_tasks[new_size - 2].get();
            if (last_ctx->end_offset == tail_ctx->start_offset) {
                last_ctx->end_offset = tail_ctx->end_offset;
                new_split_tasks.pop_back();
            }
        }
    }

    split_tasks->swap(new_split_tasks);
}

} // namespace starrocks
