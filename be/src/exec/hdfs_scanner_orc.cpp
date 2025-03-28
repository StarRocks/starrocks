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

#include "exec/hdfs_scanner_orc.h"

#include <utility>

#include "exec/exec_node.h"
#include "exec/iceberg/iceberg_delete_builder.h"
#include "exec/paimon/paimon_delete_file_builder.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_input_stream.h"
#include "formats/orc/orc_memory_pool.h"
#include "formats/orc/orc_min_max_decoder.h"
#include "formats/orc/utils.h"
#include "gen_cpp/orc_proto.pb.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "util/runtime_profile.h"
#include "util/timezone_utils.h"

namespace starrocks {

struct SplitContext : public HdfsSplitContext {
    std::shared_ptr<std::string> footer;

    HdfsSplitContextPtr clone() override {
        auto ctx = std::make_unique<SplitContext>();
        ctx->footer = footer;
        return ctx;
    }
};

class OrcRowReaderFilter : public orc::RowReaderFilter {
public:
    OrcRowReaderFilter(const HdfsScannerContext& scanner_ctx, OrcChunkReader* reader);
    bool filterOnOpeningStripe(uint64_t stripeIndex, const orc::proto::StripeInformation* stripeInformation) override;
    bool filterOnPickRowGroup(size_t rowGroupIdx, const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                              const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilters) override;
    bool filterMinMax(size_t rowGroupIdx, const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                      const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilter);
    bool filterOnPickStringDictionary(const std::unordered_map<uint64_t, orc::StringDictionary*>& sdicts) override;

    bool is_slot_evaluated(SlotId id) { return _dict_filter_eval_cache.find(id) != _dict_filter_eval_cache.end(); }
    void onStartingPickRowGroups() override;
    void onEndingPickRowGroups() override;
    void setWriterTimezone(const std::string& tz) override;

private:
    const HdfsScannerContext& _scanner_ctx;
    uint64_t _current_stripe_index{0};
    bool _init_use_dict_filter_slots{false};
    std::vector<pair<SlotDescriptor*, uint64_t>> _use_dict_filter_slots;
    friend class HdfsOrcScanner;
    std::unordered_map<SlotId, FilterPtr> _dict_filter_eval_cache;
    bool _can_do_filter_on_orc_cvb{true}; // cvb: column vector batch.
    OrcChunkReader* _reader;
    int64_t _writer_tzoffset_in_seconds;
};

void OrcRowReaderFilter::onStartingPickRowGroups() {}

void OrcRowReaderFilter::onEndingPickRowGroups() {}

void OrcRowReaderFilter::setWriterTimezone(const std::string& tz) {
    cctz::time_zone writer_tzinfo;
    if (tz == "" || !TimezoneUtils::find_cctz_time_zone(tz, writer_tzinfo)) {
        _writer_tzoffset_in_seconds = _reader->tzoffset_in_seconds();
    } else {
        _writer_tzoffset_in_seconds = TimezoneUtils::to_utc_offset(writer_tzinfo);
    }
}

OrcRowReaderFilter::OrcRowReaderFilter(const HdfsScannerContext& scanner_ctx, OrcChunkReader* reader)
        : _scanner_ctx(scanner_ctx), _reader(reader), _writer_tzoffset_in_seconds(reader->tzoffset_in_seconds()) {
    if (_scanner_ctx.min_max_tuple_desc != nullptr) {
        VLOG_FILE << "OrcRowReaderFilter: min_max_tuple_desc = " << _scanner_ctx.min_max_tuple_desc->debug_string();
        for (ExprContext* ctx : _scanner_ctx.min_max_conjunct_ctxs) {
            VLOG_FILE << "OrcRowReaderFilter: min_max_ctx = " << ctx->root()->debug_string();
        }
    }
}

bool OrcRowReaderFilter::filterOnOpeningStripe(uint64_t stripeIndex,
                                               const orc::proto::StripeInformation* stripeInformation) {
    _current_stripe_index = stripeIndex;
    uint64_t offset = stripeInformation->offset();

    const auto* scan_range = _scanner_ctx.scan_range;
    size_t scan_start = scan_range->offset;
    size_t scan_end = scan_range->length + scan_start;

    if (offset >= scan_start && offset < scan_end) {
        return false;
    }
    return true;
}

bool OrcRowReaderFilter::filterMinMax(size_t rowGroupIdx,
                                      const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                                      const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilter) {
    const TupleDescriptor* min_max_tuple_desc = _scanner_ctx.min_max_tuple_desc;
    ChunkPtr min_chunk = ChunkHelper::new_chunk(*min_max_tuple_desc, 0);
    ChunkPtr max_chunk = ChunkHelper::new_chunk(*min_max_tuple_desc, 0);
    for (size_t i = 0; i < min_max_tuple_desc->slots().size(); i++) {
        SlotDescriptor* slot = min_max_tuple_desc->slots()[i];
        const orc::Type* orc_type = _reader->get_orc_type_by_slot_id(slot->id());
        int32_t column_index = -1;
        if (orc_type != nullptr) {
            column_index = orc_type->getColumnId();
        }
        if (column_index >= 0) {
            const auto& row_idx_iter = rowIndexes.find(column_index);
            // there is no column stats, skip filter process.
            if (row_idx_iter == rowIndexes.end()) {
                return false;
            }
            const orc::proto::ColumnStatistics& stats = row_idx_iter->second.entry(rowGroupIdx).statistics();
            ColumnPtr min_col = min_chunk->columns()[i];
            ColumnPtr max_col = max_chunk->columns()[i];
            DCHECK(!min_col->is_constant() && !max_col->is_constant());
            int64_t tz_offset_in_seconds = _reader->tzoffset_in_seconds() - _writer_tzoffset_in_seconds;
            Status st = OrcMinMaxDecoder::decode(slot, orc_type, stats, min_col, max_col, tz_offset_in_seconds);
            if (!st.ok()) {
                LOG(INFO) << strings::Substitute(
                        "OrcMinMaxDecoder decode failed, may occur performance degradation. Because SR's column($0) "
                        "can't convert to orc file's column($1)",
                        slot->debug_string(), orc_type->toString());
                return false;
            }
        } else {
            // search partition columns.
            int part_idx = 0;
            const int part_size = _scanner_ctx.partition_columns.size();
            for (part_idx = 0; part_idx < part_size; part_idx++) {
                if (_scanner_ctx.partition_columns[part_idx].name() == slot->col_name()) {
                    break;
                }
            }
            // not found in partition columns.
            if (part_idx == part_size) {
                min_chunk->columns()[i]->append_nulls(1);
                max_chunk->columns()[i]->append_nulls(1);
            } else {
                auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(_scanner_ctx.partition_values[part_idx]);
                ColumnPtr data_column = const_column->data_column();
                if (data_column->is_nullable()) {
                    min_chunk->columns()[i]->append_nulls(1);
                    max_chunk->columns()[i]->append_nulls(1);
                } else {
                    min_chunk->columns()[i]->append(*data_column, 0, 1);
                    max_chunk->columns()[i]->append(*data_column, 0, 1);
                }
            }
        }
    }

    VLOG_FILE << "stripe = " << _current_stripe_index << ", row_group = " << rowGroupIdx
              << ", min_chunk = " << min_chunk->debug_row(0) << ", max_chunk = " << max_chunk->debug_row(0);
    for (auto& min_max_conjunct_ctx : _scanner_ctx.min_max_conjunct_ctxs) {
        // TODO: add a warning log here
        auto min_col = EVALUATE_NULL_IF_ERROR(min_max_conjunct_ctx, min_max_conjunct_ctx->root(), min_chunk.get());
        auto max_col = EVALUATE_NULL_IF_ERROR(min_max_conjunct_ctx, min_max_conjunct_ctx->root(), max_chunk.get());
        if (min_col->get(0).is_null() || max_col->get(0).is_null()) {
            continue;
        }
        auto min = min_col->get(0).get_int8();
        auto max = max_col->get(0).get_int8();
        if (min == 0 && max == 0) {
            // Means this row group dont stastisfy min-max predicates, we can filter this row group.
            return true;
        }
    }
    return false;
}
bool OrcRowReaderFilter::filterOnPickRowGroup(size_t rowGroupIdx,
                                              const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                                              const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilters) {
    if (_scanner_ctx.min_max_tuple_desc != nullptr) {
        if (filterMinMax(rowGroupIdx, rowIndexes, bloomFilters)) {
            VLOG_FILE << "OrcRowReaderFilter: skip row group " << rowGroupIdx << ", stripe " << _current_stripe_index;
            return true;
        }
    }
    return false;
}

bool OrcRowReaderFilter::filterOnPickStringDictionary(
        const std::unordered_map<uint64_t, orc::StringDictionary*>& sdicts) {
    _dict_filter_eval_cache.clear();

    if (sdicts.empty()) return false;

    if (!_init_use_dict_filter_slots) {
        for (const auto& col : _scanner_ctx.materialized_columns) {
            SlotDescriptor* slot = col.slot_desc;
            if (!_scanner_ctx.can_use_dict_filter_on_slot(slot)) {
                continue;
            }
            const orc::Type* orc_type = _reader->get_orc_type_by_slot_id(slot->id());
            if (orc_type == nullptr) {
                continue;
            }
            uint64_t column_id = orc_type->getColumnId();
            _use_dict_filter_slots.emplace_back(slot, column_id);
        }
        _init_use_dict_filter_slots = true;
    }

    for (const auto& p : _use_dict_filter_slots) {
        SlotDescriptor* slot_desc = p.first;
        SlotId slot_id = slot_desc->id();
        uint64_t column_id = p.second;
        const auto& it = sdicts.find(column_id);
        if (it == sdicts.end()) {
            continue;
        }
        // create chunk
        orc::StringDictionary* dict = it->second;
        if (dict->dictionaryOffset.size() > _reader->runtime_state()->chunk_size()) {
            continue;
        }
        ChunkPtr dict_value_chunk = std::make_shared<Chunk>();
        // always assume there is a possibility of null value in ORC column.
        // and we evaluate with null always.
        ColumnPtr column_ptr = ColumnHelper::create_column(slot_desc->type(), true);
        dict_value_chunk->append_column(column_ptr, slot_id);

        auto* nullable_column = down_cast<NullableColumn*>(column_ptr.get());
        auto* dict_value_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());

        // copy dict and offset to column.
        Bytes& bytes = dict_value_column->get_bytes();
        Offsets& offsets = dict_value_column->get_offset();

        const char* content_data = dict->dictionaryBlob.data();
        size_t content_size = dict->dictionaryBlob.size();
        bytes.reserve(content_size);
        const auto* start = reinterpret_cast<const uint8_t*>(content_data);
        const auto* end = reinterpret_cast<const uint8_t*>(content_data + content_size);

        size_t offset_size = dict->dictionaryOffset.size();
        size_t dict_size = offset_size - 1;
        const int64_t* offset_data = dict->dictionaryOffset.data();
        offsets.resize(offset_size);

        if (slot_desc->type().type == TYPE_CHAR) {
            // for char type, dict strings are also padded with spaces.
            // we also have to strip space off. For example
            // | hello      |  world      | yes     |, we have to compact to
            // | hello | world | yes |
            size_t total_size = 0;
            const char* p_start = reinterpret_cast<const char*>(start);
            for (size_t i = 0; i < dict_size; i++) {
                const char* s = p_start + offset_data[i];
                size_t old_size = offset_data[i + 1] - offset_data[i];
                size_t new_size = remove_trailing_spaces(s, old_size);
                bytes.insert(bytes.end(), s, s + new_size);
                offsets[i] = total_size;
                total_size += new_size;
            }
            offsets[dict_size] = total_size;
        } else {
            bytes.insert(bytes.end(), start, end);
            // type mismatch, have to use loop to assign.
            for (size_t i = 0; i < offset_size; i++) {
                offsets[i] = offset_data[i];
            }
        }

        // first (dict_size) th items are all not-null
        nullable_column->null_column()->append_default(dict_size);
        // and last one is null.
        nullable_column->append_default();
        DCHECK(nullable_column->size() == (dict_size + 1));

        VLOG_FILE << "OrcRowReaderFilter: stripe = " << _current_stripe_index
                  << ", slot = " << slot_desc->debug_string()
                  << ", dict values = " << dict_value_column->debug_string();

        FilterPtr* filter_ptr = nullptr;
        if (_can_do_filter_on_orc_cvb) {
            // not sure it's best practice.
            FilterPtr tmp;
            _dict_filter_eval_cache[slot_id] = tmp;
            filter_ptr = &_dict_filter_eval_cache[slot_id];
        }

        // do evaluation with dictionary.
        Status status = ExecNode::eval_conjuncts(_scanner_ctx.conjunct_ctxs_by_slot.at(slot_id), dict_value_chunk.get(),
                                                 filter_ptr);
        if (!status.ok()) {
            LOG(WARNING) << "eval conjuncts fails: " << status.message();
            _dict_filter_eval_cache.erase(slot_id);
            return false;
        }

        if (dict_value_chunk->num_rows() == 0) {
            // release memory early.
            _dict_filter_eval_cache.clear();
            VLOG_FILE << "OrcRowReaderFilter: skip stripe by dict filter, stripe " << _current_stripe_index
                      << ", on slot = " << slot_desc->debug_string();
            return true;
        }
        DCHECK(filter_ptr->get() != nullptr);
    }

    return false;
}

Status HdfsOrcScanner::build_iceberg_delete_builder() {
    if (_scanner_params.deletes.empty()) return Status::OK();
    SCOPED_RAW_TIMER(&_app_stats.iceberg_delete_file_build_ns);
    const auto iceberg_delete_builder =
            std::make_unique<IcebergDeleteBuilder>(_skip_rows_ctx, _runtime_state, _scanner_params);

    for (const auto& delete_file : _scanner_params.deletes) {
        if (delete_file->file_content == TIcebergFileContent::POSITION_DELETES) {
            RETURN_IF_ERROR(iceberg_delete_builder->build_orc(*delete_file));
        } else {
            const auto s = strings::Substitute("Unsupported iceberg file content: $0 in the scanner thread",
                                               delete_file->file_content);
            LOG(WARNING) << s;
            return Status::InternalError(s);
        }
    }

    _app_stats.iceberg_delete_files_per_scan += _scanner_params.deletes.size();
    return Status::OK();
}

Status HdfsOrcScanner::build_paimon_delete_file_builder() {
    if (_scanner_params.paimon_deletion_file == nullptr) {
        return Status::OK();
    }
    std::unique_ptr<PaimonDeleteFileBuilder> paimon_delete_file_builder(
            new PaimonDeleteFileBuilder(_scanner_params.fs, _skip_rows_ctx));
    RETURN_IF_ERROR(paimon_delete_file_builder->build(_scanner_params.paimon_deletion_file.get()));
    return Status::OK();
}

Status HdfsOrcScanner::build_stripes(orc::Reader* reader, std::vector<DiskRange>* stripes) {
    uint64_t stripe_number = reader->getNumberOfStripes();
    std::vector<DiskRange> stripe_disk_ranges{};

    const auto* scan_range = _scanner_ctx.scan_range;
    size_t scan_start = scan_range->offset;
    size_t scan_end = scan_range->length + scan_start;

    for (uint64_t idx = 0; idx < stripe_number; idx++) {
        const auto& stripeInfo = reader->getStripeInOrcFormat(idx);
        int64_t offset = stripeInfo.offset();

        if (offset >= scan_start && offset < scan_end) {
            int64_t length = stripeInfo.datalength() + stripeInfo.indexlength() + stripeInfo.footerlength();
            stripes->emplace_back(offset, length);
            _app_stats.orc_stripe_sizes.push_back(length);
        }
    }
    return Status::OK();
}

Status HdfsOrcScanner::build_io_ranges(ORCHdfsFileStream* file_stream, const std::vector<DiskRange>& stripes) {
    bool tiny_stripe_read = true;
    for (const DiskRange& disk_range : stripes) {
        if (disk_range.length() > config::orc_tiny_stripe_threshold_size) {
            tiny_stripe_read = false;
            break;
        }
    }
    // we need to start tiny stripe optimization if all stripe's size smaller than config::orc_tiny_stripe_threshold_size
    if (tiny_stripe_read) {
        std::vector<io::SharedBufferedInputStream::IORange> io_ranges{};
        std::vector<DiskRange> merged_disk_ranges{};
        DiskRangeHelper::merge_adjacent_disk_ranges(stripes, config::io_coalesce_read_max_distance_size,
                                                    config::orc_tiny_stripe_threshold_size, merged_disk_ranges);
        for (const auto& disk_range : merged_disk_ranges) {
            io_ranges.emplace_back(disk_range.offset(), disk_range.length());
        }
        for (const auto& it : io_ranges) {
            _app_stats.orc_total_tiny_stripe_size += it.size;
        }
        RETURN_IF_ERROR(file_stream->setIORanges(io_ranges));
    }
    return Status::OK();
}

Status HdfsOrcScanner::resolve_columns(orc::Reader* reader) {
    std::unordered_set<std::string> known_column_names;
    OrcChunkReader::build_column_name_set(&known_column_names, _scanner_ctx.hive_column_names, reader->getType(),
                                          _scanner_ctx.case_sensitive, _scanner_ctx.orc_use_column_names);
    RETURN_IF_ERROR(_scanner_ctx.update_materialized_columns(known_column_names));
    ASSIGN_OR_RETURN(auto skip, _scanner_ctx.should_skip_by_evaluating_not_existed_slots());
    if (skip) {
        LOG(INFO) << "HdfsOrcScanner: do_open. skip file for non existed slot conjuncts.";
        _should_skip_file = true;
        return Status::OK();
    }

    int src_slot_index = 0;
    for (const auto& column : _scanner_ctx.materialized_columns) {
        auto col_name = Utils::format_name(column.name(), _scanner_ctx.case_sensitive);
        if (known_column_names.find(col_name) == known_column_names.end()) continue;
        bool is_lazy_slot = _scanner_params.is_lazy_materialization_slot(column.slot_id());
        if (is_lazy_slot) {
            _lazy_load_ctx.lazy_load_slots.emplace_back(column.slot_desc);
            _lazy_load_ctx.lazy_load_indices.emplace_back(src_slot_index);
            // reserve room for later set in `OrcChunkReader`
            _lazy_load_ctx.lazy_load_orc_positions.emplace_back(0);
        } else {
            _lazy_load_ctx.active_load_slots.emplace_back(column.slot_desc);
            _lazy_load_ctx.active_load_indices.emplace_back(src_slot_index);
            // reserve room for later set in `OrcChunkReader`
            _lazy_load_ctx.active_load_orc_positions.emplace_back(0);
        }

        // put materialized columns' conjunctions into _eval_conjunct_ctxs_by_materialized_slot
        // for example, partition column's conjunctions will not put into _eval_conjunct_ctxs_by_materialized_slot
        {
            auto it = _scanner_params.conjunct_ctxs_by_slot.find(column.slot_id());
            if (it != _scanner_params.conjunct_ctxs_by_slot.end()) {
                _eval_conjunct_ctxs_by_materialized_slot.emplace(it->first, it->second);
            }
        }

        _src_slot_descriptors.emplace_back(column.slot_desc);
        src_slot_index++;
    }
    return Status::OK();
}

Status HdfsOrcScanner::build_split_tasks(orc::Reader* reader, const std::vector<DiskRange>& stripes) {
    // we can split task if we enable split tasks feature and have >= 2 stripes.
    // but if we have splitted tasks before, we don't want to split again, to avoid infinite loop.
    bool enable_split_tasks =
            (_scanner_ctx.enable_split_tasks && stripes.size() >= 2) && (_scanner_ctx.split_context == nullptr);
    if (!enable_split_tasks) return Status::OK();

    auto footer = std::make_shared<std::string>(reader->getSerializedFileTail());
    for (const auto& info : stripes) {
        auto ctx = std::make_unique<SplitContext>();
        ctx->footer = footer;
        ctx->split_start = info.offset();
        ctx->split_end = info.offset() + info.length();
        _scanner_ctx.split_tasks.emplace_back(std::move(ctx));
    }
    _scanner_ctx.merge_split_tasks();
    // if only one split task, clear it, no need to do split work.
    if (_scanner_ctx.split_tasks.size() <= 1) {
        _scanner_ctx.split_tasks.clear();
    }
    VLOG_OPERATOR << "HdfsOrcScanner: do_open. split task for " << _file->filename()
                  << ", split_tasks.size = " << _scanner_ctx.split_tasks.size();

    return Status::OK();
}

Status HdfsOrcScanner::do_open(RuntimeState* runtime_state) {
    // create wrapped input stream.
    RETURN_IF_ERROR(open_random_access_file());
    if (_input_stream == nullptr) {
        _input_stream = std::make_unique<ORCHdfsFileStream>(_file.get(), _file->get_size().value(),
                                                            _shared_buffered_input_stream.get());
        _input_stream->set_lazy_column_coalesce_counter(_scanner_ctx.lazy_column_coalesce_counter);
        _input_stream->set_app_stats(&_app_stats);
    }
    ORCHdfsFileStream* orc_hdfs_file_stream = _input_stream.get();

    // create orc reader on this input stream.
    SCOPED_RAW_TIMER(&_app_stats.reader_init_ns);
    std::unique_ptr<orc::Reader> reader;
    try {
        errno = 0;
        orc::ReaderOptions options;
        options.setMemoryPool(*getOrcMemoryPool());
        if (_scanner_ctx.split_context != nullptr) {
            auto* split_context = down_cast<const SplitContext*>(_scanner_ctx.split_context);
            options.setSerializedFileTail(*(split_context->footer.get()));
        }
        reader = orc::createReader(std::move(_input_stream), options);
    } catch (std::exception& e) {
        bool is_not_found = (errno == ENOENT);
        auto s = strings::Substitute("HdfsOrcScanner::do_open failed. reason = $0", e.what());
        LOG(WARNING) << s;
        if (is_not_found || s.find("404") != std::string::npos) {
            return Status::RemoteFileNotFound(s);
        }
        return Status::InternalError(s);
    }

    // select stripes to read and resolve columns aganist this orc file.
    std::vector<DiskRange> stripes;
    RETURN_IF_ERROR(build_stripes(reader.get(), &stripes));
    RETURN_IF_ERROR(build_split_tasks(reader.get(), stripes));
    if (_scanner_ctx.split_tasks.size() > 0) {
        _scanner_ctx.has_split_tasks = true;
        _should_skip_file = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(build_io_ranges(orc_hdfs_file_stream, stripes));
    RETURN_IF_ERROR(resolve_columns(reader.get()));
    if (_should_skip_file) {
        return Status::OK();
    }

    // create orc chunk reader .
    _orc_reader = std::make_unique<OrcChunkReader>(runtime_state->chunk_size(), _src_slot_descriptors);
    _orc_row_reader_filter = std::make_shared<OrcRowReaderFilter>(_scanner_ctx, _orc_reader.get());
    _orc_reader->disable_broker_load_mode();
    _orc_reader->set_row_reader_filter(_orc_row_reader_filter);
    _orc_reader->set_read_chunk_size(runtime_state->chunk_size());
    _orc_reader->set_runtime_state(runtime_state);
    _orc_reader->set_current_file_name(_file->filename());
    RETURN_IF_ERROR(_orc_reader->set_timezone(_scanner_ctx.timezone));
    _orc_reader->set_hive_column_names(_scanner_ctx.hive_column_names);
    _orc_reader->set_case_sensitive(_scanner_ctx.case_sensitive);
    _orc_reader->set_use_orc_column_names(_scanner_ctx.orc_use_column_names);
    // for hive table, we set this flag
    _orc_reader->set_invalid_as_null(true);
    if (config::enable_orc_late_materialization && _lazy_load_ctx.lazy_load_slots.size() != 0 &&
        _lazy_load_ctx.active_load_slots.size() != 0) {
        _orc_reader->set_lazy_load_context(&_lazy_load_ctx);
    }

    std::vector<Expr*> conjuncts{};
    if (_use_orc_sargs) {
        for (const auto& it : _scanner_ctx.conjunct_ctxs_by_slot) {
            for (const auto& it2 : it.second) {
                conjuncts.push_back(it2->root());
            }
        }
        // add scanner's conjunct also, because SearchArgumentBuilder can support it
        for (const auto& it : _scanner_params.scanner_conjunct_ctxs) {
            conjuncts.push_back(it->root());
        }
    }
    const OrcPredicates orc_predicates{&conjuncts, _scanner_ctx.runtime_filter_collector};
    RETURN_IF_ERROR(_orc_reader->init(std::move(reader), &orc_predicates));

    // create iceberg delete builder at last
    RETURN_IF_ERROR(build_iceberg_delete_builder());
    RETURN_IF_ERROR(build_paimon_delete_file_builder());
    return Status::OK();
}

void HdfsOrcScanner::do_close(RuntimeState* runtime_state) noexcept {
    _orc_reader.reset(nullptr);
}

Status HdfsOrcScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    CHECK(chunk != nullptr);
    if (_should_skip_file) {
        return Status::EndOfFile("");
    }

    size_t rows_read = 0;

    if (_scanner_ctx.return_count_column) {
        ASSIGN_OR_RETURN(rows_read, _do_get_next_count(chunk));
    } else {
        ASSIGN_OR_RETURN(rows_read, _do_get_next(chunk));
    }

    _scanner_ctx.append_or_update_partition_column_to_chunk(chunk, rows_read);
    _scanner_ctx.append_or_update_extended_column_to_chunk(chunk, rows_read);

    // check after partition/extended column added
    DCHECK_EQ(rows_read, chunk->get()->num_rows());

    return Status::OK();
}

StatusOr<size_t> HdfsOrcScanner::_do_get_next_count(ChunkPtr* chunk) {
    size_t read_num_values = 0;
    Status st = Status::OK();
    while (true) {
        {
            SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
            orc::RowReader::ReadPosition position;
            st = _orc_reader->read_next(&position);
            if (!st.ok()) {
                break;
            }
        }
        read_num_values += _orc_reader->get_cvb_size();
        if (_skip_rows_ctx != nullptr && _skip_rows_ctx->has_skip_rows()) {
            read_num_values -= _orc_reader->get_row_delete_number(_skip_rows_ctx);
        }
    }

    if (!st.is_end_of_file()) return st;
    if (read_num_values == 0) return Status::EndOfFile("No more rows to read");
    _scanner_ctx.append_or_update_count_column_to_chunk(chunk, read_num_values);
    return 1;
}

StatusOr<size_t> HdfsOrcScanner::_do_get_next(ChunkPtr* chunk) {
    ChunkPtr& ck = *chunk;
    // this infinite for loop is for retry.
    while (true) {
        orc::RowReader::ReadPosition position;
        size_t read_num_values = 0;
        bool has_used_dict_filter = false;
        MutableColumnPtr row_delete_filter = BooleanColumn::create();
        {
            SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
            RETURN_IF_ERROR(_orc_reader->read_next(&position));
            {
                SCOPED_RAW_TIMER(&_app_stats.build_rowid_filter_ns);
                ASSIGN_OR_RETURN(row_delete_filter, _orc_reader->get_row_delete_filter(_skip_rows_ctx));
            }
            // read num values is how many rows actually read before doing dict filtering.
            read_num_values = position.num_values;
            RETURN_IF_ERROR(_orc_reader->apply_dict_filter_eval_cache(_orc_row_reader_filter->_dict_filter_eval_cache,
                                                                      &_dict_filter));
            if (_orc_reader->get_cvb_size() != read_num_values) {
                has_used_dict_filter = true;
                row_delete_filter->filter(_dict_filter);
            }
        }

        size_t rows_read = 0;
        size_t origin_rows_read = 0;
        if (_orc_reader->get_cvb_size() != 0) {
            rows_read = _orc_reader->get_cvb_size();
            origin_rows_read = rows_read;
            {
                StatusOr<ChunkPtr> ret;
                SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);
                if (!_orc_reader->has_lazy_load_context()) {
                    ret = _orc_reader->get_chunk();
                } else {
                    ret = _orc_reader->get_active_chunk();
                }
                RETURN_IF_ERROR(ret);
                *chunk = std::move(ret.value());
            }

            // we need to append none existed column before do eval, just for count(*) optimization
            RETURN_IF_ERROR(_scanner_ctx.append_or_update_not_existed_columns_to_chunk(chunk, rows_read));

            // do stats before we filter rows which does not match.
            _app_stats.raw_rows_read += rows_read;
            _chunk_filter.assign(rows_read, 1);
            {
                SCOPED_RAW_TIMER(&_app_stats.expr_filter_ns);
                for (auto& it : _eval_conjunct_ctxs_by_materialized_slot) {
                    // do evaluation.
                    if (_orc_row_reader_filter->is_slot_evaluated(it.first)) {
                        continue;
                    }
                    ASSIGN_OR_RETURN(rows_read,
                                     ExecNode::eval_conjuncts_into_filter(it.second, ck.get(), &_chunk_filter));
                    if (rows_read == 0) {
                        // If rows_read = 0, we need to set chunk size = 0 and bypass filter chunk directly
                        ck->set_num_rows(0);
                        break;
                    }
                }
            }

            if (rows_read != 0) {
                ColumnHelper::merge_two_filters(std::move(row_delete_filter), &_chunk_filter, nullptr);
                rows_read = SIMD::count_nonzero(_chunk_filter);
                if (rows_read == 0) {
                    // If rows_read = 0, we need to set chunk size = 0 and bypass filter chunk directly
                    ck->set_num_rows(0);
                }
            }

            if (rows_read != 0 && rows_read != ck->num_rows()) {
                ck->filter(_chunk_filter);
            }
        }

        if (!_orc_reader->has_lazy_load_context()) {
            return rows_read;
        }

        // if has lazy load fields, skip it if chunk_size == 0
        bool require_load_lazy_columns = rows_read > 0;
        if (require_load_lazy_columns) {
            // still need to load lazy column
            _scanner_ctx.lazy_column_coalesce_counter->fetch_add(1, std::memory_order_relaxed);
        } else {
            // dont need to load lazy column
            _scanner_ctx.lazy_column_coalesce_counter->fetch_sub(1, std::memory_order_relaxed);
            _app_stats.late_materialize_skip_rows += origin_rows_read;
            continue;
        }

        {
            SCOPED_RAW_TIMER(&_app_stats.column_read_ns);
            RETURN_IF_ERROR(_orc_reader->lazy_seek_to(position.row_in_stripe));
            RETURN_IF_ERROR(_orc_reader->lazy_read_next(read_num_values));
        }
        {
            SCOPED_RAW_TIMER(&_app_stats.column_convert_ns);
            if (has_used_dict_filter) {
                _orc_reader->lazy_filter_on_cvb(&_dict_filter);
            }
            _orc_reader->lazy_filter_on_cvb(&_chunk_filter);
            StatusOr<ChunkPtr> ret = _orc_reader->get_lazy_chunk();
            RETURN_IF_ERROR(ret);
            Chunk& ret_ck = *(ret.value());
            ck->merge(std::move(ret_ck));
        }
        return rows_read;
    }
    return Status::InternalError("Unreachable code");
}

Status HdfsOrcScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    _should_skip_file = false;
    _use_orc_sargs = true;
    return Status::OK();
}

void HdfsOrcScanner::do_update_counter(HdfsScanProfile* profile) {
    // if we have split tasks, we don't need to update counter
    // and we will update those counters in sub io tasks.
    if (has_split_tasks()) {
        return;
    }
    const std::string orcProfileSectionPrefix = "ORC";

    RuntimeProfile* root_profile = profile->runtime_profile;
    ADD_COUNTER(root_profile, orcProfileSectionPrefix, TUnit::NONE);

    do_update_iceberg_v2_counter(root_profile, orcProfileSectionPrefix);

    RuntimeProfile::Counter* total_stripe_size_counter = root_profile->add_child_counter(
            "TotalStripeSize", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TCounterAggregateType::SUM),
            orcProfileSectionPrefix);
    RuntimeProfile::Counter* total_stripe_number_counter = root_profile->add_child_counter(
            "TotalStripeNumber", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TCounterAggregateType::SUM),
            orcProfileSectionPrefix);
    RuntimeProfile::Counter* total_tiny_stripe_size_counter = root_profile->add_child_counter(
            "TotalTinyStripeSize", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TCounterAggregateType::SUM),
            orcProfileSectionPrefix);

    size_t total_stripe_size = 0;
    for (const auto& v : _app_stats.orc_stripe_sizes) {
        total_stripe_size += v;
    }
    COUNTER_UPDATE(total_stripe_size_counter, total_stripe_size);
    COUNTER_UPDATE(total_stripe_number_counter, _app_stats.orc_stripe_sizes.size());

    COUNTER_UPDATE(total_tiny_stripe_size_counter, _app_stats.orc_total_tiny_stripe_size);

    RuntimeProfile::Counter* stripe_active_lazy_coalesce_together_counter = root_profile->add_child_counter(
            "StripeActiveLazyColumnIOCoalesceTogether", TUnit::UNIT,
            RuntimeProfile::Counter::create_strategy(TCounterAggregateType::SUM), orcProfileSectionPrefix);
    RuntimeProfile::Counter* stripe_active_lazy_coalesce_seperately_counter = root_profile->add_child_counter(
            "StripeActiveLazyColumnIOCoalesceSeperately", TUnit::UNIT,
            RuntimeProfile::Counter::create_strategy(TCounterAggregateType::SUM), orcProfileSectionPrefix);
    COUNTER_UPDATE(stripe_active_lazy_coalesce_together_counter, _app_stats.orc_stripe_active_lazy_coalesce_together);
    COUNTER_UPDATE(stripe_active_lazy_coalesce_seperately_counter,
                   _app_stats.orc_stripe_active_lazy_coalesce_seperately);

    if (_orc_reader != nullptr) {
        // _orc_reader is nullptr for split task
        root_profile->add_info_string("ORCSearchArgument: ", _orc_reader->get_search_argument_string());
    }
}

} // namespace starrocks
