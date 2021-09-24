// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/es_http_scanner.h"

#include <fmt/format.h>

#include <memory>

#include "exec/exec_node.h"
#include "exprs/expr.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
EsHttpScanner::EsHttpScanner(RuntimeState* state, RuntimeProfile* profile, TupleId tuple_id,
                             std::map<std::string, std::string> properties, std::vector<ExprContext*> conjunct_ctxs,
                             const std::map<std::string, std::string>& docvalue_context, bool doc_value_mode)
        : _state(state),
          _profile(profile),
          _tuple_id(tuple_id),
          _properties(std::move(properties)),
          _conjunct_ctxs(std::move(conjunct_ctxs)),
          _docvalue_context(docvalue_context),
          _next_range(0),
          _line_eof(true),
          _batch_eof(false),
          _mem_tracker(new MemTracker(-1, "EsHttp FileScanner", state->instance_mem_tracker())),
          _mem_pool(_state->instance_mem_tracker()),
          _tuple_desc(nullptr),
          _es_reader(nullptr),
          _es_scroll_parser(nullptr),
          _doc_value_mode(doc_value_mode),
          _rows_read_counter(nullptr),
          _read_timer(nullptr),
          _materialize_timer(nullptr) {}

EsHttpScanner::~EsHttpScanner() {
    close();
}

Status EsHttpScanner::open() {
    _tuple_desc = _state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError(fmt::format("Unknown tuple descriptor, tuple_id={}", _tuple_id));
    }

    const std::string& host = _properties.at(ESScanReader::KEY_HOST_PORT);
    _es_reader = std::make_unique<ESScanReader>(host, _properties, _doc_value_mode);
    RETURN_IF_ERROR(_es_reader->open());

    _rows_read_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_profile, "MaterializeTupleTime(*)");
    return Status::OK();
}

Status EsHttpScanner::get_next(RuntimeState* runtime_state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_read_timer);
    if (_line_eof && _batch_eof) {
        *eos = true;
        return Status::OK();
    }

    while (!_batch_eof) {
        if (_line_eof || _es_scroll_parser == nullptr) {
            RETURN_IF_ERROR(_es_reader->get_next(&_batch_eof, _es_scroll_parser));
            _es_scroll_parser->set_params(_tuple_desc, &_docvalue_context);
            if (_batch_eof) {
                *eos = true;
                return Status::OK();
            }
        }

        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
        RETURN_IF_ERROR(_es_scroll_parser->fill_chunk(chunk, &_line_eof));

        if (chunk->get() != nullptr) {
            ExecNode::eval_conjuncts(_conjunct_ctxs, chunk->get());
        }

        if (!_line_eof) {
            break;
        }
    }

    return Status::OK();
}

void EsHttpScanner::close() {
    if (_es_reader != nullptr) {
        _es_reader->close();
    }
    // es reader
    Expr::close(_conjunct_ctxs, _state);
}
} // namespace starrocks::vectorized
