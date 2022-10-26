// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <boost/algorithm/string.hpp>
#include <orc/OrcFile.hh>

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/buffered_stream.h"

namespace orc {
namespace proto {
class ColumnStatistics;
}
} // namespace orc

namespace starrocks {
class RandomAccessFile;
class RuntimeState;
} // namespace starrocks
namespace starrocks::vectorized {

using FillColumnFunction = void (*)(orc::ColumnVectorBatch* cvb, ColumnPtr& col, int from, int size,
                                    const TypeDescriptor& type_desc, void* ctx);

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
    explicit OrcChunkReader(RuntimeState* state, const std::vector<SlotDescriptor*>& src_slot_descriptors);
    ~OrcChunkReader();
    Status init(std::unique_ptr<orc::InputStream> input_stream);
    Status init(std::unique_ptr<orc::Reader> reader);
    Status read_next(orc::RowReader::ReadPosition* pos = nullptr);
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
                                           const RuntimeFilterProbeCollector* rf_collector);
    Status set_timezone(const std::string& tz);
    size_t num_columns() const { return _src_slot_descriptors.size(); }

    // to decode min and max value from column stats.
    Status decode_min_max_value(SlotDescriptor* slot, const orc::proto::ColumnStatistics&, ColumnPtr min_col,
                                ColumnPtr max_col, int64_t tz_offset_in_seconds);
    Status apply_dict_filter_eval_cache(const std::unordered_map<SlotId, FilterPtr>& dict_filter_eval_cache,
                                        Filter* filter);
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
    int get_column_id_by_name(const std::string& name) const;

    void set_lazy_load_context(LazyLoadContext* ctx) { _lazy_load_ctx = ctx; }
    bool has_lazy_load_context() { return _lazy_load_ctx != nullptr; }
    StatusOr<ChunkPtr> get_chunk();
    StatusOr<ChunkPtr> get_active_chunk();
    void lazy_read_next(size_t numValues);
    void lazy_seek_to(uint64_t rowInStripe);
    void lazy_filter_on_cvb(Filter* filter);
    StatusOr<ChunkPtr> get_lazy_chunk();

private:
    ChunkPtr _create_chunk(const std::vector<SlotDescriptor*>& slots, const std::vector<int>* indices);
    Status _fill_chunk(ChunkPtr* chunk, const std::vector<SlotDescriptor*>& slots, const std::vector<int>* indices);
    ChunkPtr _cast_chunk(ChunkPtr* chunk, const std::vector<SlotDescriptor*>& slots, const std::vector<int>* indices);

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
    Status _slot_to_orc_column_name(const SlotDescriptor* slot,
                                    const std::unordered_map<int, std::string>& column_id_to_orc_name,
                                    std::string* orc_column_name);
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
    bool _case_sensitive = false;
    std::unordered_map<std::string, int> _name_to_column_id;
    RuntimeState* _state;
    SlotDescriptor* _current_slot;
    std::string _current_file_name;
    int _error_message_counter;
    LazyLoadContext* _lazy_load_ctx;
};

class ORCHdfsFileStream : public orc::InputStream {
public:
    // |file| must outlive ORCHdfsFileStream
    ORCHdfsFileStream(RandomAccessFile* file, uint64_t length);

    ~ORCHdfsFileStream() override = default;

    uint64_t getLength() const override { return _length; }

    // refers to paper `Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores`
    uint64_t getNaturalReadSize() const override { return config::orc_natural_read_size; }

    // It's for read size after doing seek.
    // When doing read after seek, we make assumption that we are doing random read because of seeking row group.
    // And if we still use NaturalReadSize we probably read many row groups
    // after the row group we want to read, and that will amplify read IO bytes.

    // So the best way is to reduce read size, hopefully we just read that row group in one shot.
    // We also have chance that we may not read enough at this shot, then we fallback to NaturalReadSize to read.
    // The cost is, there is a extra IO, and we read 1/4 of NaturalReadSize more data.
    // And the potential gain is, we save 3/4 of NaturalReadSize IO bytes.

    // Normally 256K can cover a row group of a column(like integer or double, but maybe not string)
    // And this value can not be too small because if we can not read a row group in a single shot,
    // we will fallback to read in normal size, and we pay cost of a extra read.

    uint64_t getNaturalReadSizeAfterSeek() const override { return config::orc_natural_read_size / 4; }

    void prepareCache(orc::InputStream::PrepareCacheScope scope, uint64_t offset, uint64_t length) override;
    void read(void* buf, uint64_t length, uint64_t offset) override;

    const std::string& getName() const override;

    bool isIORangesEnabled() const override { return config::orc_coalesce_read_enable; }
    void clearIORanges() override;
    void setIORanges(std::vector<orc::InputStream::IORange>& io_ranges) override;

    void set_enable_block_cache(bool v) { _buffer_stream.set_enable_block_cache(v); }

private:
    void doRead(void* buf, uint64_t length, uint64_t offset, bool direct);
    bool canUseCacheBuffer(uint64_t offset, uint64_t length);

    RandomAccessFile* _file;
    uint64_t _length;
    std::vector<char> _cache_buffer;
    uint64_t _cache_offset;
    SharedBufferedInputStream _buffer_stream;
    bool _buffer_stream_enabled = false;
};

} // namespace starrocks::vectorized
