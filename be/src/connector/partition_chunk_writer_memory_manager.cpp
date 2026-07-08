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

#include "connector/partition_chunk_writer_memory_manager.h"

#include "common/logging.h"

namespace starrocks::connector {

Status PartitionChunkWriterMemoryManager::init(std::vector<PartitionChunkWriterPtr>* writers,
                                               formats::AsyncFlushStreamPoller* io_poller) {
    _candidate_lists.clear();
    _candidate_lists.push_back(writers);
    _io_poller = io_poller;
    return Status::OK();
}

void PartitionChunkWriterMemoryManager::add_candidates(std::vector<PartitionChunkWriterPtr>* writers) {
    if (writers == nullptr) {
        return;
    }
    _candidate_lists.push_back(writers);
}

bool PartitionChunkWriterMemoryManager::kill_victim() {
    // Find a target file writer to flush across all registered candidate lists.
    // For buffered partition writer, choose the the writer with the largest file size.
    // For spillable partition writer, choose the the writer with the largest memory size that can be spilled.
    PartitionChunkWriterPtr victim = nullptr;
    for (auto* candidates : _candidate_lists) {
        if (candidates == nullptr) {
            continue;
        }
        for (auto& writer : *candidates) {
            int64_t flushable_bytes = writer->get_flushable_bytes();
            if (flushable_bytes == 0) {
                continue;
            }
            if (victim && flushable_bytes < victim->get_flushable_bytes()) {
                continue;
            }
            victim = writer;
        }
    }
    if (victim == nullptr) {
        return false;
    }

    // The flush will decrease the writer flushable memory bytes, so it usually
    // will not be choosed in a short time.
    const auto filename = victim->out_stream()->filename();
    size_t flush_bytes = victim->get_flushable_bytes();
    const auto result = victim->flush();
    LOG(INFO) << "kill victim: " << filename << ", result: " << result << ", flushable_bytes: " << flush_bytes;
    return true;
}

int64_t PartitionChunkWriterMemoryManager::update_releasable_memory() {
    int64_t releasable_memory = _io_poller->releasable_memory();
    _releasable_memory.store(releasable_memory);
    return releasable_memory;
}

int64_t PartitionChunkWriterMemoryManager::update_writer_occupied_memory() {
    int64_t writer_occupied_memory = 0;
    for (auto* candidates : _candidate_lists) {
        if (candidates == nullptr) {
            continue;
        }
        for (auto& writer : *candidates) {
            writer_occupied_memory += writer->get_flushable_bytes();
        }
    }
    _writer_occupied_memory.store(writer_occupied_memory);
    return writer_occupied_memory;
}

} // namespace starrocks::connector
