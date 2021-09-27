// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <orc/OrcFile.hh>

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
namespace orc {
namespace proto {
class ColumnStatistics;
}
} // namespace orc

namespace starrocks {
class RuntimeState;
}
namespace starrocks::vectorized {

using FillColumnFunction = void (*)(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                    const TypeDescriptor& type_desc, void* ctx);

// OrcScannerAdapter is a bridge between apache/orc and Column
// It mainly does 4 things:
// 1. create chunk according to schema
// 2. read orc data and convert to chunk
// 3. do some conversion on chunk according to schema
// 4. passing predicate down to apache/orc
class OrcScannerAdapter {
public:
    // src slot descriptors should exactly matches columns in row readers.
    explicit OrcScannerAdapter(const std::vector<SlotDescriptor*>& src_slot_descriptors);
    ~OrcScannerAdapter();
    Status init(std::unique_ptr<orc::InputStream> input_stream);
    Status init(std::unique_ptr<orc::Reader> reader);
    Status read_next();
    // create sample chunk
    ChunkPtr create_chunk();
    // copy from cvb to chunk
    Status fill_chunk(ChunkPtr* chunk);
    // some type cast & conversion.
    ChunkPtr cast_chunk(ChunkPtr* chunk);
    // call them before calling init.
    void set_read_chunk_size(uint64_t v) { _read_chunk_size = v; }
    void set_row_reader_filter(std::shared_ptr<orc::RowReaderFilter> filter);
    void set_conjuncts(const std::vector<Expr*>& conjuncts);
    void set_conjuncts_and_runtime_filters(const std::vector<Expr*>& conjuncts,
                                           RuntimeFilterProbeCollector* rf_collector);
    Status set_timezone(const std::string& tz);
    int num_columns() const { return _src_slot_descriptors.size(); }

    // to decode min and max value from column stats.
    static Status decode_min_max_value(SlotDescriptor* slot, const orc::proto::ColumnStatistics&, ColumnPtr min_col,
                                       ColumnPtr max_col);
    Status apply_dict_filter_eval_cache(const std::unordered_map<SlotId, FilterPtr>& dict_filter_eval_cache);
    size_t get_cvb_size();
    int64_t tzoffset_in_seconds() { return _tzoffset_in_seconds; }
    const cctz::time_zone& tzinfo() { return _tzinfo; }
    void drop_nanoseconds_in_datetime() { _drop_nanoseconds_in_datetime = true; }
    bool use_nanoseconds_in_datetime() { return !_drop_nanoseconds_in_datetime; }
    // methods related to broker load.
    void set_broker_load_mode(bool strict_mode) {
        _broker_load_mode = true;
        _strict_mode = strict_mode;
    }
    void disable_broker_load_mode() { _broker_load_mode = false; }
    size_t get_num_rows_filtered() const { return _num_rows_filtered; }
    bool get_broker_load_mode() const { return _broker_load_mode; }
    bool get_strict_mode() const { return _strict_mode; }
    std::shared_ptr<Column::Filter> get_broker_load_fiter() { return _broker_load_filter; }

    void set_hive_column_names(const std::vector<std::string>* v) {
        if (v != nullptr && v->size() != 0) {
            _hive_column_names = v;
        }
    }

    static void build_column_name_to_id_mapping(std::unordered_map<std::string, int>* mapping,
                                                const std::vector<std::string>* hive_column_names,
                                                const orc::Type& root_type);
    static void build_column_name_set(std::unordered_set<std::string>* name_set,
                                      const std::vector<std::string>* hive_column_names, const orc::Type& root_type);

    void set_runtime_state(RuntimeState* state) { _state = state; }
    void set_current_slot(SlotDescriptor* slot) { _current_slot = slot; }
    const SlotDescriptor* get_current_slot() const { return _current_slot; }
    void set_current_file_name(const std::string& name) { _current_file_name = name; }
    void report_error_message(const std::string& reason, const std::string& raw_data);
    int get_column_id_by_name(const std::string& name);

private:
    bool _ok_to_add_conjunct(const Expr* conjunct);
    void _add_conjunct(const Expr* conjunct, std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    bool _add_runtime_filter(const SlotDescriptor* slot_desc, const JoinRuntimeFilter* rf,
                             std::unique_ptr<orc::SearchArgumentBuilder>& builder);

    std::unique_ptr<orc::ColumnVectorBatch> _batch;
    std::unique_ptr<orc::Reader> _reader;
    std::unique_ptr<orc::RowReader> _row_reader;
    orc::ReaderOptions _reader_options;
    orc::RowReaderOptions _row_reader_options;
    const std::vector<SlotDescriptor*>& _src_slot_descriptors;
    std::unordered_map<SlotId, SlotDescriptor*> _slot_id_to_desc;
    std::vector<TypeDescriptor> _src_types;
    // _src_slot index to position in orc
    std::vector<int> _position_in_orc;
    // slot id to position in orc.
    std::unordered_map<SlotId, int> _slot_id_to_position;
    std::vector<Expr*> _cast_exprs;
    std::vector<FillColumnFunction> _fill_functions;
    Status _init_include_columns();
    Status _init_position_in_orc();
    Status _init_src_types();
    Status _init_cast_exprs();
    Status _init_fill_functions();
    // holding Expr* in cast_exprs;
    ObjectPool _pool;
    uint64_t _read_chunk_size;
    cctz::time_zone _tzinfo;
    int64_t _tzoffset_in_seconds;
    bool _drop_nanoseconds_in_datetime;

    // fields related to broker load.
    bool _broker_load_mode;
    bool _strict_mode;
    std::shared_ptr<Column::Filter> _broker_load_filter;
    size_t _num_rows_filtered;
    const std::vector<std::string>* _hive_column_names = nullptr;
    std::unordered_map<std::string, int> _name_to_column_id;
    RuntimeState* _state;
    SlotDescriptor* _current_slot;
    std::string _current_file_name;
    int _error_message_counter;
};

} // namespace starrocks::vectorized