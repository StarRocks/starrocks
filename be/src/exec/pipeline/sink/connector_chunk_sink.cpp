//
// Created by Letian Jiang on 2024/1/16.
//

#include "connector_chunk_sink.h"

#include <formats/parquet/file_writer.h>
#include <future>

namespace starrocks::pipeline {

std::future<Status> FilesChunkSink::add(ChunkPtr chunk) {
    std::string partition = _partitioned_write ? "test" : DEFAULT_PARTITION;

    // TODO: file writer factory
    if (_partition_writers.count(partition) == 0) {
        string location = _path + "_" + fmt::format("{}_{}_{}_{}.parquet", print_id(state->query_id()), state->be_number(), _driver_sequence, _next_id++);
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(_path, FSOptions(&_cloud_conf)));
        WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto writable_file, fs->new_writable_file(options, location));
        auto writer = std::make_unique<parquet::ParquetFileWriter>(std::move(writable_file), ::parquet::default_writer_properties(), _parquet_file_schema, _output_exprs, 1 << 30);
        RETURN_IF_ERROR(writer->init());
        _partition_writers[partition] = writer;
    }

    auto& writer = _partition_writers[partition];
    // poc: unpartitoned writer
    // commit writer if exceeds target file size
    if (writer->getWrittenBytes() > 1 << 30) {
        writer->commitAsync([&, fragment_ctx = _fragment_ctx, state = state](FileWriter::CommitResult result) {
            if (!result.io_status.ok()) {
                LOG(WARNING) << "cancel fragment instance " << state->fragment_instance_id() << ": " << result.io_status;
                fragment_ctx->cancel(result.io_status);
                return;
            }
            state->update_num_rows_load_sink(result.file_metrics.record_count);
            _rollback_actions.push(result.rollback_action);
        });

        string location = _path + "_" + fmt::format("{}_{}_{}_{}.parquet", print_id(state->query_id()), state->be_number(), _driver_sequence, _next_id++);
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(location, FSOptions(&_cloud_conf)));
        WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto writable_file, fs->new_writable_file(options, location));
        _partition_writers[partition] = std::make_unique<parquet::ParquetFileWriter>(std::move(writable_file), ::parquet::default_writer_properties(), _parquet_file_schema, _output_exprs, 1 << 30);
        // RETURN_IF_ERROR(_file_writer->init());
    }

    return _partition_writers[partition]->write(chunk);
}

}


