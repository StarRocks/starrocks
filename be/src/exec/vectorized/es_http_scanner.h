// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "common/global_types.h"
#include "exec/es/es_scan_reader.h"
#include "exec/vectorized/es_http_components.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"

namespace starrocks {
class RuntimeState;
class RuntimeProfile;
class ExprContext;
class TupleDescriptor;

namespace vectorized {
struct EsScanCounter {
    EsScanCounter() {}

    int64_t num_rows_returned{0};
    int64_t num_rows_filtered{0};
};

class EsHttpScanner {
public:
    EsHttpScanner(RuntimeState* state, RuntimeProfile* profile, TupleId tuple_id,
                  std::map<std::string, std::string> properties, std::vector<ExprContext*> conjunct_ctxs,
                  const std::map<std::string, std::string>& docvalue_context, bool doc_value_mode);
    ~EsHttpScanner();

    const EsScanCounter& scan_counter() { return _counter; }
    Status open();
    Status get_next(RuntimeState* runtime_state, ChunkPtr* chunk, bool* eos);
    void close();

    RuntimeState* runtime_state() { return _state; }

private:
    RuntimeState* _state;
    RuntimeProfile* _profile;
    TupleId _tuple_id;
    std::map<std::string, std::string> _properties;
    std::vector<ExprContext*> _conjunct_ctxs;
    const std::map<std::string, std::string>& _docvalue_context;

    int _next_range;
    bool _line_eof;
    bool _batch_eof;
    MemPool _mem_pool;

    const TupleDescriptor* _tuple_desc;
    EsScanCounter _counter;
    std::unique_ptr<ESScanReader> _es_reader;
    std::unique_ptr<vectorized::ScrollParser> _es_scroll_parser;

    bool _doc_value_mode;

    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _materialize_timer;
};
} // namespace vectorized

} // namespace starrocks
