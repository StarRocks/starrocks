//
// Created by Letian Jiang on 2024/1/29.
//

#include "file_chunk_sink.h"

#include <future>

#include "column/datum.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "util/url_coding.h"
#include "utils.h"

namespace starrocks::connector {

FileChunkSink::FileChunkSink(const std::vector<std::string>& partition_columns,
                             const std::vector<TExpr>& partition_exprs,
                             std::unique_ptr<LocationProvider> location_provider,
                             std::unique_ptr<formats::FileWriterFactory> file_writer_factory, int64_t max_file_size,
                             RuntimeState* runtime_state)
        : _partition_exprs(partition_exprs),
          _partition_column_names(partition_columns),
          _location_provider(std::move(location_provider)),
          _file_writer_factory(std::move(file_writer_factory)),
          _max_file_size(max_file_size),
          _runtime_state(runtime_state) {}

FileChunkSink::~FileChunkSink() {
    Expr::close(_partition_expr_ctxs, _runtime_state);
}

Status FileChunkSink::init() {
    RETURN_IF_ERROR(Expr::create_expr_trees(_runtime_state->obj_pool(), _partition_exprs, &_partition_expr_ctxs,
                                            _runtime_state));
    RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, _runtime_state));
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, _runtime_state));
    return Status::OK();
}

// requires that input chunk belongs to a single partition (see LocalKeyPartitionExchange)
StatusOr<ConnectorChunkSink::Futures> FileChunkSink::add(ChunkPtr chunk) {
    std::string partition;
    if (_partition_exprs.empty()) {
        partition = DEFAULT_PARTITION;
    } else {
        ASSIGN_OR_RETURN(partition,
                         HiveUtils::make_partition_name(_partition_column_names, _partition_expr_ctxs, chunk));
    }

    // create writer if not found
    if (_partition_writers[partition] == nullptr) {
        auto path = _partition_exprs.empty() ? _location_provider->get() : _location_provider->get(partition);
        ASSIGN_OR_RETURN(_partition_writers[partition], _file_writer_factory->create(path));
        RETURN_IF_ERROR(_partition_writers[partition]->init());
    }

    Futures futures;
    auto& writer = _partition_writers[partition];
    if (writer->get_written_bytes() > _max_file_size) {
        auto f = writer->commit();
        futures.commit_file_future.push_back(std::move(f));
        auto path = _partition_exprs.empty() ? _location_provider->get() : _location_provider->get(partition);
        ASSIGN_OR_RETURN(_partition_writers[partition], _file_writer_factory->create(path));
    }

    auto f = writer->write(chunk);
    futures.add_chunk_future.push_back(std::move(f));
    return futures;
}

ConnectorChunkSink::Futures FileChunkSink::finish() {
    Futures futures;
    for (auto& [_, writer] : _partition_writers) {
        auto f = writer->commit();
        futures.commit_file_future.push_back(std::move(f));
    }
    return futures;
}

std::function<void(const formats::FileWriter::CommitResult& result)> FileChunkSink::callback_on_success() {
    return [state = _runtime_state](const formats::FileWriter::CommitResult& result) {
        DCHECK(result.io_status.ok());
        state->update_num_rows_load_sink(result.file_metrics.record_count);
    };
}

} // namespace starrocks::connector
