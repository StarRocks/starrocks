// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/jdbc_scan_operator.h"

#include <sstream>

#include "common/config.h"
#include "exec/vectorized/jdbc_scanner.h"
#include "runtime/descriptors.h"
#include "runtime/jdbc_driver_manager.h"
#include "util/defer_op.h"

namespace starrocks {
namespace pipeline {

JDBCScanOperator::JDBCScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node,
                                   const TJDBCScanNode& jdbc_scan_node)
        : ScanOperator(factory, id, scan_node),
          _jdbc_scan_node(jdbc_scan_node),
          _conjunct_ctxs(scan_node->conjunct_ctxs()),
          _limit(scan_node->limit()),
          _result_chunks(config::doris_scanner_queue_size) {}

bool JDBCScanOperator::has_output() const {
    if (_is_finished.load()) {
        return false;
    }
    // some chunks are not being consumed
    if (!_result_chunks.empty()) {
        return true;
    }

    // no new chunks and scanner thread finished successfully
    Status scanner_status;
    bool scanner_finished = _is_scanner_finished(&scanner_status);
    if (scanner_finished && scanner_status.ok()) {
        return false;
    }
    // Here are two situation
    // 1. scanner thread is still running
    // 2. scanner thread finished in error, we should make sure `pull_chunk` has a chance to get scanner status
    return true;
}

bool JDBCScanOperator::pending_finish() const {
    return !_is_finished.load();
}

bool JDBCScanOperator::is_finished() const {
    Status scanner_status;
    bool scanner_finished = _is_scanner_finished(&scanner_status);
    if (scanner_finished) {
        // scanner thread finished and all chunks have been consumed
        if (scanner_status.ok() && _result_chunks.empty()) {
            return true;
        }
        // scanner thread finished in error, we should make sure `pull_chunk` has a chance to get scanner status
        if (!scanner_status.ok()) {
            return false;
        }
    }
    return _is_finished.load();
}

Status JDBCScanOperator::_start_scanner_thread(RuntimeState* state) {
    // @TODO(silverbullet233): need more test for thread_local variable memory leaks
    _scanner_thread.reset(new std::thread(&JDBCScanOperator::_start_scanner, this, state));
    return Status::OK();
}

void JDBCScanOperator::_start_scanner(RuntimeState* state) {
    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(_jdbc_scan_node.tuple_id);
    const auto* jdbc_table = down_cast<const JDBCTableDescriptor*>(tuple_desc->table_desc());

    Status status;
    vectorized::JDBCScanContext scan_ctx;
    std::string driver_name = jdbc_table->jdbc_driver_name();
    std::string driver_url = jdbc_table->jdbc_driver_url();
    std::string driver_checksum = jdbc_table->jdbc_driver_checksum();
    std::string driver_class = jdbc_table->jdbc_driver_class();
    std::string driver_location;

    if (status = JDBCDriverManager::getInstance()->get_driver_location(driver_name, driver_url, driver_checksum,
                                                                       &driver_location);
        !status.ok()) {
        LOG(ERROR) << fmt::format("Get JDBC Driver[{}] error, error is {}", driver_name, status.to_string());
        _set_scanner_state(true, status);
        return;
    }

    scan_ctx.driver_path = driver_location;
    scan_ctx.driver_class_name = driver_class;
    scan_ctx.jdbc_url = jdbc_table->jdbc_url();
    scan_ctx.user = jdbc_table->jdbc_user();
    scan_ctx.passwd = jdbc_table->jdbc_passwd();
    scan_ctx.sql = get_jdbc_sql(jdbc_table->jdbc_table(), _jdbc_scan_node.columns, _jdbc_scan_node.filters, _limit);

    _scanner.reset(new vectorized::JDBCScanner(scan_ctx, _result_tuple_desc));

    if (status = _scanner->open(state); !status.ok()) {
        _set_scanner_state(true, status);
        _scanner->close(state);
        return;
    }
    if (status = _fetch_chunks(); !status.ok()) {
        _set_scanner_state(true, status);
        _scanner->close(state);
        return;
    }
    _scanner->close(state);
    _set_scanner_state(true, Status::OK());
}

void JDBCScanOperator::set_finishing(RuntimeState* state) {
    _is_finished.store(true);
}

StatusOr<vectorized::ChunkPtr> JDBCScanOperator::pull_chunk(RuntimeState* state) {
    if (_is_finished.load()) {
        return nullptr;
    }
    Status scanner_status;
    bool scanner_finished = _is_scanner_finished(&scanner_status);
    if (scanner_finished && !scanner_status.ok()) {
        return scanner_status;
    }
    ChunkPtr chunk;
    int ret = _result_chunks.try_get(&chunk);
    switch (ret) {
    case -1: {
        return nullptr;
    }
    case 0: {
        // no new chunks and scanner thread is finished
        if (scanner_finished) {
            _is_finished.store(true);
        }
        return nullptr;
    }
    case 1: {
        return chunk;
    }
    default: {
        break;
    }
    }
    DCHECK(false) << "unreachable path";
    return nullptr;
}

Status JDBCScanOperator::do_prepare(RuntimeState* state) {
    _state = state;
    _result_tuple_desc = state->desc_tbl().get_tuple_descriptor(_jdbc_scan_node.tuple_id);
    _start_scanner_thread(state);
    return Status::OK();
}

void JDBCScanOperator::do_close(RuntimeState* state) {
    if (_scanner_thread) {
        _is_finished.store(true);
        _scanner_thread->join();
    }
}

ChunkSourcePtr JDBCScanOperator::create_chunk_source(MorselPtr morsel) {
    // this function is not be used in JDBCScanOperator, just ignore it
    return nullptr;
}

void JDBCScanOperator::_set_scanner_state(bool is_finished, const Status& new_status) {
    std::lock_guard<SpinLock> l(_scanner_state_mutex);
    _scanner_finished = is_finished;
    _scanner_status = new_status;
}

bool JDBCScanOperator::_is_scanner_finished(Status* status) const {
    std::lock_guard<SpinLock> l(_scanner_state_mutex);
    if (status) {
        *status = _scanner_status;
    }
    return _scanner_finished;
}

Status JDBCScanOperator::_fetch_chunks() {
    bool eof = false;
    while (!_is_finished.load() && !eof) {
        ChunkPtr chunk;
        RETURN_IF_ERROR(_scanner->get_next(_state, &chunk, &eof));
        if (!_result_chunks.blocking_put(chunk)) {
            break;
        }
    }
    return Status::OK();
}

std::string JDBCScanOperator::get_jdbc_sql(const std::string& table, const std::vector<std::string>& columns,
                                           const std::vector<std::string>& filters, int64_t limit) {
    std::ostringstream oss;
    oss << "SELECT";
    for (size_t i = 0; i < columns.size(); i++) {
        oss << (i == 0 ? "" : ",") << " " << columns[i];
    }
    oss << " FROM " << table;
    if (!filters.empty()) {
        oss << " WHERE ";
        for (size_t i = 0; i < filters.size(); i++) {
            oss << (i == 0 ? "" : " AND") << "(" << filters[i] << ")";
        }
    }
    if (limit != -1) {
        oss << " LIMIT " << limit;
    }
    return oss.str();
}

JDBCScanOperatorFactory::JDBCScanOperatorFactory(int32_t id, ScanNode* scan_node, const TJDBCScanNode& jdbc_scan_node)
        : ScanOperatorFactory(id, scan_node), _jdbc_scan_node(jdbc_scan_node) {}

Status JDBCScanOperatorFactory::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void JDBCScanOperatorFactory::do_close(RuntimeState* state) {}

OperatorPtr JDBCScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<JDBCScanOperator>(this, _id, _scan_node, _jdbc_scan_node);
}

} // namespace pipeline
} // namespace starrocks