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

#pragma once

#include <boost/algorithm/string.hpp>
#include <orc/OrcFile.hh>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter_bank.h"
#include "formats/orc/column_reader.h"
#include "formats/orc/orc_mapping.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace orc::proto {
class ColumnStatistics;
} // namespace orc::proto

namespace starrocks {
class RandomAccessFile;
class RuntimeState;
} // namespace starrocks
namespace starrocks {

// OrcChunkReader is a bridge between apache/orc and Column
// It mainly does 4 things:
// 1. create chunk according to schema
// 2. read orc data and convert to chunk
// 3. do some conversion on chunk according to schema
// 4. passing predicate down to apache/orc
class OrcChunkReader {
public:
    struct LazyLoadContext {
        std::vector<SlotDescriptor*> active_load_slots;
        std::vector<int> active_load_indices;
        std::vector<int> active_load_orc_positions;
        std::vector<SlotDescriptor*> lazy_load_slots;
        std::vector<int> lazy_load_indices;
        std::vector<int> lazy_load_orc_positions;
    };

    // src slot descriptors should exactly matches columns in row readers.
    explicit OrcChunkReader(int chunk_size, std::vector<SlotDescriptor*> src_slot_descriptors);
    ~OrcChunkReader();
    Status init(std::unique_ptr<orc::InputStream> input_stream);
    Status init(std::unique_ptr<orc::Reader> reader);
    Status read_next(orc::RowReader::ReadPosition* pos = nullptr);
    // create sample chunk
    ChunkPtr create_chunk();
    // copy from cvb to chunk
    Status fill_chunk(ChunkPtr* chunk);
    // some type cast & conversion.
    StatusOr<ChunkPtr> cast_chunk_checked(ChunkPtr* chunk);
    ChunkPtr cast_chunk(ChunkPtr* chunk) { return cast_chunk_checked(chunk).value(); }
    // call them before calling init.
    void set_read_chunk_size(uint64_t v) { _read_chunk_size = v; }
    void set_row_reader_filter(std::shared_ptr<orc::RowReaderFilter> filter);
    Status set_conjuncts(const std::vector<Expr*>& conjuncts);
    Status set_conjuncts_and_runtime_filters(const std::vector<Expr*>& conjuncts,
                                             const RuntimeFilterProbeCollector* rf_collector);
    Status set_timezone(const std::string& tz);
    size_t num_columns() const { return _src_slot_descriptors.size(); }

    Status apply_dict_filter_eval_cache(const std::unordered_map<SlotId, FilterPtr>& dict_filter_eval_cache,
                                        Filter* filter);
    size_t get_cvb_size();
    int64_t tzoffset_in_seconds() { return _tzoffset_in_seconds; }
    const cctz::time_zone& tzinfo() { return _tzinfo; }
    void drop_nanoseconds_in_datetime() { _drop_nanoseconds_in_datetime = true; }
    bool use_nanoseconds_in_datetime() { return !_drop_nanoseconds_in_datetime; }
    void set_use_orc_column_names(bool use_orc_column_names) { _use_orc_column_names = use_orc_column_names; }
    // methods related to broker load.
    void set_broker_load_mode(bool strict_mode) {
        _broker_load_mode = true;
        _strict_mode = strict_mode;
        set_use_orc_column_names(true);
    }
    void disable_broker_load_mode() {
        _broker_load_mode = false;
        set_use_orc_column_names(false);
    }
    size_t get_num_rows_filtered() const { return _num_rows_filtered; }
    bool get_broker_load_mode() const { return _broker_load_mode; }
    bool get_strict_mode() const { return _strict_mode; }
    std::shared_ptr<Filter> get_broker_load_fiter() { return _broker_load_filter; }

    void set_hive_column_names(const std::vector<std::string>* v) {
        if (v != nullptr && v->size() != 0) {
            _hive_column_names = v;
        }
    }
    void set_case_sensitive(bool case_sensitive) { _case_sensitive = case_sensitive; }

    static void build_column_name_to_id_mapping(std::unordered_map<std::string, int>* mapping,
                                                const std::vector<std::string>* hive_column_names,
                                                const orc::Type& root_type, bool case_sensitive);
    static void build_column_name_set(std::unordered_set<std::string>* name_set,
                                      const std::vector<std::string>* hive_column_names, const orc::Type& root_type,
                                      bool case_sensitive);
    static std::string format_column_name(const std::string& col_name, bool case_sensitive) {
        return case_sensitive ? col_name : boost::algorithm::to_lower_copy(col_name);
    }

    void set_runtime_state(RuntimeState* state) { _state = state; }
    RuntimeState* runtime_state() { return _state; }
    void set_current_slot(SlotDescriptor* slot) { _current_slot = slot; }
    SlotDescriptor* get_current_slot() const { return _current_slot; }
    void set_current_file_name(const std::string& name) { _current_file_name = name; }
    void report_error_message(const std::string& error_msg);
    int get_column_id_by_slot_name(const std::string& name) const;

    void set_lazy_load_context(LazyLoadContext* ctx) { _lazy_load_ctx = ctx; }
    bool has_lazy_load_context() { return _lazy_load_ctx != nullptr; }
    StatusOr<ChunkPtr> get_chunk();
    StatusOr<ChunkPtr> get_active_chunk();
    Status lazy_read_next(size_t numValues);
    Status lazy_seek_to(uint64_t rowInStripe);
    void lazy_filter_on_cvb(Filter* filter);
    StatusOr<ChunkPtr> get_lazy_chunk();
    ColumnPtr get_row_delete_filter(const std::set<int64_t>& deleted_pos);

    bool is_implicit_castable(TypeDescriptor& starrocks_type, const TypeDescriptor& orc_type);

private:
    ChunkPtr _create_chunk(const std::vector<SlotDescriptor*>& slots, const std::vector<int>* indices);
    Status _fill_chunk(ChunkPtr* chunk, const std::vector<SlotDescriptor*>& slots, const std::vector<int>* indices);
    StatusOr<ChunkPtr> _cast_chunk(ChunkPtr* chunk, const std::vector<SlotDescriptor*>& slots,
                                   const std::vector<int>* indices);

    bool _ok_to_add_conjunct(const Expr* conjunct);
    Status _add_conjunct(const Expr* conjunct, std::unique_ptr<orc::SearchArgumentBuilder>& builder);
    bool _add_runtime_filter(const SlotDescriptor* slot_desc, const JoinRuntimeFilter* rf,
                             std::unique_ptr<orc::SearchArgumentBuilder>& builder);

    void _try_implicit_cast(TypeDescriptor* from, const TypeDescriptor& to);

    std::unique_ptr<orc::ColumnVectorBatch> _batch;
    std::unique_ptr<orc::Reader> _reader;
    std::unique_ptr<orc::RowReader> _row_reader;
    orc::ReaderOptions _reader_options;
    orc::RowReaderOptions _row_reader_options;
    std::vector<SlotDescriptor*> _src_slot_descriptors;
    std::unordered_map<SlotId, SlotDescriptor*> _slot_id_to_desc;

    // Access ORC columns by name. By default,
    // columns in ORC files are accessed by their ordinal position in the Hive table definition.
    // Only affect first level behavior, about struct subfield, we still accessed by subfield name rather than position.
    // This value now is fixed, in future, it can be passed from FE.
    // NOTICE: In broker mode, this value will be set true.
    // We make the same behavior as Trino & Presto.
    // https://trino.io/docs/current/connector/hive.html?highlight=hive#orc-format-configuration-properties
    bool _use_orc_column_names = false;
    OrcMappingOptions _orc_mapping_options;
    std::unique_ptr<OrcMapping> _root_selected_mapping;
    std::vector<TypeDescriptor> _src_types;
    // slot id to position in orc.
    std::unordered_map<SlotId, int> _slot_id_to_position;
    std::vector<Expr*> _cast_exprs;
    std::vector<std::unique_ptr<ORCColumnReader>> _column_readers;
    Status _init_include_columns(const std::unique_ptr<OrcMapping>& mapping);
    Status _init_position_in_orc();
    Status _init_src_types(const std::unique_ptr<OrcMapping>& mapping);
    Status _init_cast_exprs();
    Status _init_column_readers();
    // holding Expr* in cast_exprs;
    ObjectPool _pool;
    uint64_t _read_chunk_size;
    cctz::time_zone _tzinfo;
    int64_t _tzoffset_in_seconds;
    bool _drop_nanoseconds_in_datetime;

    // Only used for UT, used after init reader
    const std::vector<bool>& TEST_get_selected_column_id_list();
    // Only used for UT, used after init reader
    const std::vector<bool>& TEST_get_lazyload_column_id_list();

    // fields related to broker load.
    bool _broker_load_mode;
    bool _strict_mode;
    std::shared_ptr<Filter> _broker_load_filter;
    size_t _num_rows_filtered;
    const std::vector<std::string>* _hive_column_names = nullptr;
    bool _case_sensitive = false;
    // Key is slot name formatted with case sensitive
    std::unordered_map<std::string, int> _formatted_slot_name_to_column_id;
    RuntimeState* _state = nullptr;
    SlotDescriptor* _current_slot = nullptr;
    std::string _current_file_name;
    int _error_message_counter;
    LazyLoadContext* _lazy_load_ctx;
};

} // namespace starrocks
