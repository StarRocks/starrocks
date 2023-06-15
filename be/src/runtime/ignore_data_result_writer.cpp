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

#include "runtime/ignore_data_result_writer.h"

#include "column/chunk.h"

namespace starrocks {

IgnoreDataResultWriter::IgnoreDataResultWriter(BufferControlBlock* sinker) : _sinker(sinker) {}

Status IgnoreDataResultWriter::init(RuntimeState* state) {
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }
    return Status::OK();
}

Status IgnoreDataResultWriter::append_chunk(Chunk* chunk) {
    CHECK(false) << "Unreachable IgnoreDataResultWriter::append_chunk()";
    return Status::OK();
}

Status IgnoreDataResultWriter::close() {
    return Status::OK();
}

StatusOr<TFetchDataResultPtrs> IgnoreDataResultWriter::process_chunk(Chunk* chunk) {
    _written_rows += chunk->num_rows();
    return Status::Unknown("Ignore data");
}

StatusOr<bool> IgnoreDataResultWriter::try_add_batch(TFetchDataResultPtrs& results) {
    CHECK(false) << "Unreachable IgnoreDataResultWriter::try_add_batch()";
    return Status::OK();
}

} // namespace starrocks