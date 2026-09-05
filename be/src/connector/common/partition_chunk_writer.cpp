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

#include "connector/common/partition_chunk_writer.h"

#include <algorithm>

#include "base/failpoint/fail_point.h"
#include "column/chunk.h"
#include "formats/file_writer.h"
#include "formats/io/async_flush_stream_poller.h"

namespace starrocks::connector {

DEFINE_FAIL_POINT(parquet_chunk_writer_init_failed);

PartitionChunkWriter::PartitionChunkWriter(std::string partition, std::vector<int8_t> partition_field_null_list,
                                           const std::shared_ptr<PartitionChunkWriterContext>& ctx)
        : _partition(std::move(partition)),
          _partition_field_null_list(std::move(partition_field_null_list)),
          _file_writer_factory(ctx->file_writer_factory),
          _location_provider(ctx->location_provider),
          _max_file_size(ctx->max_file_size),
          _is_default_partition(ctx->is_default_partition) {
    _commit_extra_data.resize(_partition_field_null_list.size(), '0');
    std::transform(_partition_field_null_list.begin(), _partition_field_null_list.end(), _commit_extra_data.begin(),
                   [](int8_t b) { return b + '0'; });
}

Status PartitionChunkWriter::create_file_writer_if_needed() {
    if (!_file_writer) {
        std::string path = _is_default_partition ? _location_provider->get() : _location_provider->get(_partition);
        ASSIGN_OR_RETURN(auto new_writer_and_stream, _file_writer_factory->create(path));
        _file_writer = std::move(new_writer_and_stream.writer);
        _out_stream = std::move(new_writer_and_stream.stream);
        RETURN_IF_ERROR(_file_writer->init());

        FAIL_POINT_TRIGGER_EXECUTE(parquet_chunk_writer_init_failed,
                                   { return Status::InternalError("Create file writer failed due to fail point"); });
        _io_poller->enqueue(_out_stream);
    }
    return Status::OK();
}

Status PartitionChunkWriter::commit_file() {
    if (!_file_writer) {
        return Status::OK();
    }
    SCOPED_TIMER(_sink_profile ? _sink_profile->commit_file_timer : nullptr);
    auto file_result = _file_writer->close();
    const auto io_status = file_result.io_status;
    const auto file_size = file_result.file_statistics.file_size;
    CommitResult result{.file_result = std::move(file_result)};
    result.set_partition_null_fingerprint(_commit_extra_data);
    _commit_callback(result);
    _file_writer = nullptr;
    VLOG(3) << "commit to remote file, filename: " << _out_stream->filename() << ", size: " << file_size;
    _out_stream = nullptr;
    return io_status;
}

Status BufferPartitionChunkWriter::init() {
    RETURN_IF_ERROR(create_file_writer_if_needed());
    return Status::OK();
}

Status BufferPartitionChunkWriter::write(const ChunkPtr& chunk) {
    if (_file_writer && _file_writer->get_written_bytes() >= _max_file_size) {
        RETURN_IF_ERROR(commit_file());
    }
    RETURN_IF_ERROR(create_file_writer_if_needed());
    SCOPED_TIMER(_sink_profile ? _sink_profile->write_file_timer : nullptr);
    return _file_writer->write(chunk.get());
}

Status BufferPartitionChunkWriter::flush() {
    return commit_file();
}

Status BufferPartitionChunkWriter::wait_flush() {
    return Status::OK();
}

Status BufferPartitionChunkWriter::finish() {
    return commit_file();
}

} // namespace starrocks::connector
