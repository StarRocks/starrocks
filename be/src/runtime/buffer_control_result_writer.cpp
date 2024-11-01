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

#include "runtime/buffer_control_result_writer.h"

#include "runtime/buffer_control_block.h"

namespace starrocks {

BufferControlResultWriter::BufferControlResultWriter(BufferControlBlock* sinker, RuntimeProfile* parent_profile)
        : _sinker(sinker), _parent_profile(parent_profile) {}

bool BufferControlResultWriter::is_full() const {
    return _sinker->is_full();
}

Status BufferControlResultWriter::add_to_write_buffer(Chunk* chunk) {
    ASSIGN_OR_RETURN(auto results, process_chunk(chunk));
    SCOPED_TIMER(_result_send_timer);
    size_t num_rows = 0;
    for (auto& result : results) {
        num_rows += result->result_batch.rows.size();
    }

    auto status = _sinker->add_to_result_buffer(std::move(results));
    if (status.ok()) {
        // rows append to sink result buffer
        _written_rows += num_rows;
    } else {
        LOG(WARNING) << "Append result batch to sink failed: status=" << status.to_string();
    }
    return status;
    return Status::OK();
}

void BufferControlResultWriter::cancel() {
    _sinker->cancel_pending_rpc();
}

Status BufferControlResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

void BufferControlResultWriter::_init_profile() {
    _append_chunk_timer = ADD_TIMER(_parent_profile, "AppendChunkTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendChunkTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendChunkTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

} // namespace starrocks