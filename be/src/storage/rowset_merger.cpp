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

#include "storage/rowset_merger.h"

#include <memory>
#include <queue>

#include "column/binary_column.h"
#include "gutil/stl_util.h"
#include "storage/chunk_helper.h"
#include "storage/empty_iterator.h"
#include "storage/merge_iterator.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet.h"
#include "storage/union_iterator.h"
#include "util/pretty_printer.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

class RowsetMerger {
public:
    RowsetMerger() = default;

    virtual ~RowsetMerger() = default;

    virtual Status do_merge(Tablet& tablet, const starrocks::TabletSchemaCSPtr& tablet_schema, int64_t version,
                            const Schema& schema, const vector<RowsetSharedPtr>& rowsets, RowsetWriter* writer,
                            const MergeConfig& cfg) = 0;
};

template <class T>
struct MergeEntry {
    const T* pk_cur = nullptr;
    const T* pk_last = nullptr;
    const T* pk_start = nullptr;
    uint32_t rowset_seg_id = 0;
    ColumnPtr chunk_pk_column;
    ChunkPtr chunk;
    ChunkIteratorPtr segment_itr;
    std::unique_ptr<RowsetReleaseGuard> rowset_release_guard;
    // set |encode_schema| if require encode chunk pk columns
    const Schema* encode_schema = nullptr;
    uint16_t order;
    std::vector<RowSourceMask>* source_masks = nullptr;

    MergeEntry() = default;
    ~MergeEntry() { close(); }

    string debug_string() {
        string ret;
        StringAppendF(&ret, "%u: %ld/%ld : ", rowset_seg_id, offset(pk_cur), offset(pk_last) + 1);
        for (const T* cur = pk_cur; cur <= pk_last; cur++) {
            if constexpr (std::is_arithmetic_v<T>) {
                StringAppendF(&ret, " %ld", (long int)*cur);
            } else {
                // must be Slice
                StringAppendF(&ret, " %s", cur->to_string().c_str());
            }
        }
        return ret;
    }

    ptrdiff_t offset(const T* p) const { return p - pk_start; }

    bool at_start() const { return pk_cur == pk_start; }

    void close() {
        chunk_pk_column.reset();
        chunk.reset();
        if (segment_itr != nullptr) {
            segment_itr->close();
            segment_itr.reset();
        }
        rowset_release_guard.reset();
    }

    Status init() {
        if (segment_itr == nullptr) {
            return Status::EndOfFile("End of merge entry iterator");
        }
        return next();
    }

    Status next() {
        DCHECK(pk_cur == nullptr || pk_cur > pk_last);
        chunk->reset();
        auto st = segment_itr->get_next(chunk.get(), source_masks);
        if (st.ok()) {
            // 1. setup chunk_pk_column
            if (encode_schema != nullptr) {
                // need to encode
                chunk_pk_column->reset_column();
                PrimaryKeyEncoder::encode_sort_key(*encode_schema, *chunk, 0, chunk->num_rows(), chunk_pk_column.get());
            } else {
                // just use chunk's first column
                chunk_pk_column = chunk->get_column_by_index(chunk->schema()->sort_key_idxes()[0]);
            }
            DCHECK(chunk_pk_column->size() > 0);
            DCHECK(chunk_pk_column->size() == chunk->num_rows());
            // 2. setup pk cursor
            pk_start = reinterpret_cast<const T*>(chunk_pk_column->raw_data());
            pk_cur = pk_start;
            pk_last = pk_start + chunk_pk_column->size() - 1;
            return Status::OK();
        } else if (st.is_end_of_file()) {
            return Status::EndOfFile("End of merge entry iterator");
        } else {
            // error
            return st;
        }
    }
};

template <class T>
struct MergeEntryCmp {
    bool operator()(const MergeEntry<T>* lhs, const MergeEntry<T>* rhs) const {
        return *(lhs->pk_cur) > *(rhs->pk_cur);
    }
};

// heap based rowset merger used for updatable tablet's compaction
template <class T>
class RowsetMergerImpl : public RowsetMerger {
public:
    RowsetMergerImpl() = default;

    ~RowsetMergerImpl() override = default;

    Status _fill_heap(MergeEntry<T>* entry) {
        auto st = entry->next();
        if (st.ok()) {
            _heap.push(entry);
        } else if (!st.is_end_of_file()) {
            return st;
        }
        return Status::OK();
    }

    Status get_next(Chunk* chunk, vector<RowSourceMask>* source_masks) {
        size_t nrow = 0;
        while (!_heap.empty() && nrow < _chunk_size) {
            MergeEntry<T>& top = *_heap.top();
            //LOG(INFO) << "m" << _heap.size() << " top: " << top.debug_string();
            DCHECK_LE(top.pk_cur, top.pk_last);
            _heap.pop();
            if (_heap.empty() || *(top.pk_last) < *(_heap.top()->pk_cur)) {
                if (nrow == 0 && top.at_start()) {
                    chunk->swap_chunk(*top.chunk);
                    if (source_masks) {
                        source_masks->insert(source_masks->end(), chunk->num_rows(), RowSourceMask{top.order, false});
                    }
                    top.pk_cur = top.pk_last + 1;
                    return _fill_heap(&top);
                } else {
                    // TODO(cbl): make dest chunk size larger, so we can copy all rows at once
                    int nappend = std::min((int)(top.pk_last - top.pk_cur + 1), (int)(_chunk_size - nrow));
                    auto start_offset = top.offset(top.pk_cur);
                    chunk->append(*top.chunk, start_offset, nappend);
                    if (source_masks) {
                        source_masks->insert(source_masks->end(), nappend, RowSourceMask{top.order, false});
                    }
                    top.pk_cur += nappend;
                    if (top.pk_cur > top.pk_last) {
                        //LOG(INFO) << "  append all " << nappend << "  get_next batch";
                        return _fill_heap(&top);
                    } else {
                        //LOG(INFO) << "  append all " << nappend << "  ";
                        _heap.push(&top);
                    }
                    return Status::OK();
                }
            }

            auto start = top.pk_cur;
            while (true) {
                nrow++;
                top.pk_cur++;
                if (source_masks) {
                    source_masks->emplace_back(RowSourceMask{top.order, false});
                }
                if (top.pk_cur > top.pk_last) {
                    auto start_offset = top.offset(start);
                    auto end_offset = top.offset(top.pk_cur);
                    chunk->append(*top.chunk, start_offset, end_offset - start_offset);
                    DCHECK(chunk->num_rows() == nrow);
                    //LOG(INFO) << "  append " << end_offset - start_offset << "  get_next batch";
                    return _fill_heap(&top);
                }
                if (nrow >= _chunk_size || !(*(top.pk_cur) < *(_heap.top()->pk_cur))) {
                    auto start_offset = top.offset(start);
                    auto end_offset = top.offset(top.pk_cur);
                    chunk->append(*top.chunk, start_offset, end_offset - start_offset);
                    DCHECK(chunk->num_rows() == nrow);
                    //if (nrow >= _chunk_size) {
                    //	LOG(INFO) << "  append " << end_offset - start_offset << "  chunk full";
                    //} else {
                    //	LOG(INFO) << "  append " << end_offset - start_offset
                    //			  << "  other entry is smaller";
                    //}
                    _heap.push(&top);
                    if (nrow >= _chunk_size) {
                        return Status::OK();
                    }
                    break;
                }
            }
        }
        return Status::EndOfFile("merge end");
    }

    Status do_merge(Tablet& tablet, const starrocks::TabletSchemaCSPtr& tablet_schema, int64_t version,
                    const Schema& schema, const vector<RowsetSharedPtr>& rowsets, RowsetWriter* writer,
                    const MergeConfig& cfg) override {
        _chunk_size = cfg.chunk_size;

        size_t total_input_size = 0;
        size_t total_rows = 0;
        size_t total_chunk = 0;
        OlapReaderStatistics stats;
        vector<vector<uint32_t>> column_groups;
        MonotonicStopWatch timer;
        timer.start();
        if (cfg.algorithm == VERTICAL_COMPACTION) {
            CompactionUtils::split_column_into_groups(schema.num_fields(), schema.sort_key_idxes(),
                                                      config::vertical_compaction_max_columns_per_group,
                                                      &column_groups);
            RETURN_IF_ERROR(_do_merge_vertically(tablet, tablet_schema, version, rowsets, writer, cfg, column_groups,
                                                 &total_input_size, &total_rows, &total_chunk, &stats));
        } else {
            RETURN_IF_ERROR(_do_merge_horizontally(tablet, tablet_schema, version, schema, rowsets, writer, cfg,
                                                   &total_input_size, &total_rows, &total_chunk, &stats));
        }
        timer.stop();

        StarRocksMetrics::instance()->update_compaction_deltas_total.increment(rowsets.size());
        StarRocksMetrics::instance()->update_compaction_bytes_total.increment(total_input_size);
        StarRocksMetrics::instance()->update_compaction_outputs_total.increment(1);
        StarRocksMetrics::instance()->update_compaction_outputs_bytes_total.increment(writer->total_data_size());
        LOG(INFO) << "compaction merge finished. tablet=" << tablet.tablet_id()
                  << " #key=" << schema.sort_key_idxes().size()
                  << " algorithm=" << CompactionUtils::compaction_algorithm_to_string(cfg.algorithm)
                  << " column_group_size=" << column_groups.size() << " input("
                  << "entry=" << _entries.size() << " rows=" << stats.raw_rows_read
                  << " del=" << stats.rows_del_vec_filtered << " actual=" << stats.raw_rows_read
                  << " bytes=" << PrettyPrinter::print(total_input_size, TUnit::BYTES) << ") output(rows=" << total_rows
                  << " chunk=" << total_chunk
                  << " bytes=" << PrettyPrinter::print(writer->total_data_size(), TUnit::BYTES)
                  << ") duration: " << timer.elapsed_time() / 1000000 << "ms";
        return Status::OK();
    }

private:
    Status _do_merge_horizontally(Tablet& tablet, const starrocks::TabletSchemaCSPtr& tablet_schema, int64_t version,
                                  const Schema& schema, const vector<RowsetSharedPtr>& rowsets, RowsetWriter* writer,
                                  const MergeConfig& cfg, size_t* total_input_size, size_t* total_rows,
                                  size_t* total_chunk, OlapReaderStatistics* stats,
                                  RowSourceMaskBuffer* mask_buffer = nullptr,
                                  std::vector<std::unique_ptr<RowSourceMaskBuffer>>* rowsets_mask_buffer = nullptr) {
        std::unique_ptr<Column> sort_column;
        if (schema.sort_key_idxes().size() > 1) {
            if (!PrimaryKeyEncoder::create_column(schema, &sort_column, schema.sort_key_idxes()).ok()) {
                LOG(FATAL) << "create column for primary key encoder failed";
            }
        } else if (schema.sort_key_idxes().size() == 1 && schema.field(schema.sort_key_idxes()[0])->is_nullable()) {
            sort_column = std::make_unique<BinaryColumn>();
        }
        std::vector<std::unique_ptr<vector<RowSourceMask>>> rowsets_source_masks;
        uint16_t order = 0;
        for (const auto& rowset : rowsets) {
            *total_input_size += rowset->data_disk_size();
            _entries.emplace_back(new MergeEntry<T>());
            MergeEntry<T>& entry = *_entries.back();
            entry.rowset_release_guard = std::make_unique<RowsetReleaseGuard>(rowset);
            auto res = rowset->get_segment_iterators2(schema, tablet_schema, tablet.data_dir()->get_meta(), version,
                                                      stats);
            if (!res.ok()) {
                return res.status();
            }
            entry.rowset_seg_id = rowset->rowset_meta()->get_rowset_seg_id();
            entry.chunk = ChunkHelper::new_chunk(schema, _chunk_size);
            if (res.value().empty()) {
                entry.segment_itr = new_empty_iterator(schema, _chunk_size);
            } else {
                entry.segment_itr = std::move(new_heap_merge_iterator(res.value()));
            }
            if (sort_column) {
                entry.encode_schema = &schema;
                entry.chunk_pk_column = sort_column->clone_shared();
                entry.chunk_pk_column->reserve(_chunk_size);
            }
            if (rowsets_mask_buffer && res.value().size() > 1) {
                std::unique_ptr<vector<RowSourceMask>> rowset_source_masks = std::make_unique<vector<RowSourceMask>>();
                rowsets_source_masks.emplace_back(std::move(rowset_source_masks));
                entry.source_masks = rowsets_source_masks.back().get();
            } else if (rowsets_mask_buffer) {
                std::unique_ptr<vector<RowSourceMask>> rowset_source_masks = std::make_unique<vector<RowSourceMask>>();
                rowsets_source_masks.emplace_back(std::move(rowset_source_masks));
            }
            entry.order = order++;
            auto st = entry.init();
            if (!st.ok()) {
                if (st.is_end_of_file()) {
                    entry.close();
                } else {
                    return st;
                }
            } else {
                _heap.push(&entry);
            }
        }

        auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

        vector<uint32_t> column_indexes;
        std::unique_ptr<vector<RowSourceMask>> source_masks;
        if (mask_buffer) {
            source_masks = std::make_unique<vector<RowSourceMask>>();
            column_indexes = tablet_schema->sort_key_idxes();
        }

        auto chunk = ChunkHelper::new_chunk(schema, _chunk_size);
        while (true) {
            chunk->reset();
            Status status = get_next(chunk.get(), source_masks.get());
            if (!status.ok()) {
                if (status.is_end_of_file()) {
                    break;
                } else {
                    LOG(WARNING) << "reader get next error. tablet=" << tablet.tablet_id()
                                 << ", err=" << status.to_string();
                    return Status::InternalError("reader get_next error.");
                }
            }

            ChunkHelper::padding_char_columns(char_field_indexes, schema, tablet_schema, chunk.get());

            *total_rows += chunk->num_rows();
            (*total_chunk)++;

            if (mask_buffer) {
                if (auto st = writer->add_columns(*chunk, column_indexes, true); !st.ok()) {
                    LOG(WARNING) << "writer add_columns error, tablet=" << tablet.tablet_id() << ", err=" << st;
                    return st;
                }

                if (!source_masks->empty()) {
                    RETURN_IF_ERROR(mask_buffer->write(*source_masks));
                    source_masks->clear();
                }
            } else {
                if (auto st = writer->add_chunk(*chunk); !st.ok()) {
                    LOG(WARNING) << "writer add_chunk error, tablet=" << tablet.tablet_id() << ", err=" << st;
                    return st;
                }
            }

            if (rowsets_mask_buffer) {
                for (size_t i = 0; i < rowsets_source_masks.size(); ++i) {
                    if (!rowsets_source_masks[i]->empty()) {
                        RETURN_IF_ERROR((*rowsets_mask_buffer)[i]->write(*(rowsets_source_masks[i])));
                        rowsets_source_masks[i]->clear();
                    }
                }
            }
        }

        if (mask_buffer) {
            if (auto st = writer->flush_columns(); !st.ok()) {
                LOG(WARNING) << "failed to flush columns when merging rowsets of tablet " << tablet.tablet_id()
                             << ", err=" << st;
                return st;
            }

            RETURN_IF_ERROR(mask_buffer->flush());
        } else {
            if (auto st = writer->flush(); !st.ok()) {
                LOG(WARNING) << "failed to flush rowset when merging rowsets of tablet " << tablet.tablet_id()
                             << ", err=" << st;
                return st;
            }
        }

        if (rowsets_mask_buffer) {
            for (auto& i : *rowsets_mask_buffer) {
                RETURN_IF_ERROR(i->flush());
            }
        }

        if (stats->raw_rows_read != *total_rows) {
            string msg = strings::Substitute("update compaction rows read($0) != rows written($1)",
                                             stats->raw_rows_read, *total_rows);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }

        return Status::OK();
    }

    Status _do_merge_vertically(Tablet& tablet, const starrocks::TabletSchemaCSPtr& tablet_schema, int64_t version,
                                const vector<RowsetSharedPtr>& rowsets, RowsetWriter* writer, const MergeConfig& cfg,
                                const vector<vector<uint32_t>>& column_groups, size_t* total_input_size,
                                size_t* total_rows, size_t* total_chunk, OlapReaderStatistics* stats) {
        DCHECK_GT(column_groups.size(), 1);
        // merge key columns
        auto mask_buffer = std::make_unique<RowSourceMaskBuffer>(tablet.tablet_id(), tablet.data_dir()->path());
        std::vector<std::unique_ptr<RowSourceMaskBuffer>> rowsets_mask_buffer;
        for (size_t i = 0; i < rowsets.size(); ++i) {
            auto rowset_mask_buffer =
                    std::make_unique<RowSourceMaskBuffer>(tablet.tablet_id(), tablet.data_dir()->path());
            rowsets_mask_buffer.emplace_back(std::move(rowset_mask_buffer));
        }
        {
            Schema schema = tablet_schema->sort_key_idxes().empty()
                                    ? ChunkHelper::convert_schema(tablet_schema, column_groups[0])
                                    : ChunkHelper::get_sort_key_schema(tablet_schema);
            // NOTE: although we switch to horizontal merge, the writer used is still VerticalRowsetWriter
            // so it's important to make sure VerticalRowsetWriter can function properly when used in horizontal way
            RETURN_IF_ERROR(_do_merge_horizontally(tablet, tablet_schema, version, schema, rowsets, writer, cfg,
                                                   total_input_size, total_rows, total_chunk, stats, mask_buffer.get(),
                                                   &rowsets_mask_buffer));
        }

        // merge non key columns
        auto source_masks = std::make_unique<vector<RowSourceMask>>();
        for (size_t i = 1; i < column_groups.size(); ++i) {
            // read mask buffer from the beginning
            mask_buffer->flip_to_read();

            _entries.clear();
            _entries.reserve(rowsets.size());
            vector<ChunkIteratorPtr> iterators;
            iterators.reserve(rowsets.size());
            OlapReaderStatistics non_key_stats;
            Schema schema = ChunkHelper::convert_schema(tablet_schema, column_groups[i]);
            for (size_t j = 0; j < rowsets.size(); j++) {
                const auto& rowset = rowsets[j];
                rowsets_mask_buffer[j]->flip_to_read();
                _entries.emplace_back(new MergeEntry<T>());
                MergeEntry<T>& entry = *_entries.back();
                entry.rowset_release_guard = std::make_unique<RowsetReleaseGuard>(rowset);
                auto res = rowset->get_segment_iterators2(schema, tablet_schema, tablet.data_dir()->get_meta(), version,
                                                          &non_key_stats);
                if (!res.ok()) {
                    return res.status();
                }
                vector<ChunkIteratorPtr> segment_iters;
                for (const auto& segment_iter : res.value()) {
                    if (segment_iter) {
                        segment_iters.emplace_back(segment_iter);
                    }
                }
                if (segment_iters.empty()) {
                    iterators.emplace_back(new_empty_iterator(schema, _chunk_size));
                } else {
                    iterators.emplace_back(new_mask_merge_iterator(segment_iters, rowsets_mask_buffer[j].get()));
                }
            }

            CHECK_EQ(rowsets.size(), iterators.size());
            std::shared_ptr<ChunkIterator> iter = new_mask_merge_iterator(iterators, mask_buffer.get());
            iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS);

            auto chunk = ChunkHelper::new_chunk(schema, _chunk_size);
            auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

            while (true) {
                chunk->reset();
                Status status = iter->get_next(chunk.get(), source_masks.get());
                if (!status.ok()) {
                    if (status.is_end_of_file()) {
                        break;
                    } else {
                        LOG(WARNING) << "reader get next error. tablet=" << tablet.tablet_id()
                                     << ", err=" << status.to_string();
                        return Status::InternalError("reader get_next error.");
                    }
                }

                ChunkHelper::padding_char_columns(char_field_indexes, schema, tablet_schema, chunk.get());

                if (auto st = writer->add_columns(*chunk, column_groups[i], false); !st.ok()) {
                    LOG(WARNING) << "writer add_columns error, tablet=" << tablet.tablet_id() << ", err=" << st;
                    return st;
                }

                if (!source_masks->empty()) {
                    source_masks->clear();
                }
            }

            if (auto st = writer->flush_columns(); !st.ok()) {
                LOG(WARNING) << "failed to flush columns when merging rowsets of tablet " << tablet.tablet_id()
                             << ", err=" << st;
                return st;
            }

            if (non_key_stats.raw_rows_read != *total_rows) {
                string msg =
                        strings::Substitute("update compaction rows read($0) != rows written($1) when merging non keys",
                                            non_key_stats.raw_rows_read, *total_rows);
                LOG(WARNING) << msg;
                return Status::InternalError(msg);
            }
        }

        if (auto st = writer->final_flush(); !st.ok()) {
            LOG(WARNING) << "failed to final flush rowset when merging rowsets of tablet " << tablet.tablet_id()
                         << ", err=" << st;
            return st;
        }

        return Status::OK();
    }

    size_t _chunk_size = 0;
    std::vector<std::unique_ptr<MergeEntry<T>>> _entries;
    using Heap = std::priority_queue<MergeEntry<T>*, std::vector<MergeEntry<T>*>, MergeEntryCmp<T>>;
    Heap _heap;
};

Status compaction_merge_rowsets(Tablet& tablet, int64_t version, const vector<RowsetSharedPtr>& rowsets,
                                RowsetWriter* writer, const MergeConfig& cfg,
                                const starrocks::TabletSchemaCSPtr& cur_tablet_schema) {
    auto final_tablet_schema = cur_tablet_schema == nullptr ? tablet.tablet_schema() : cur_tablet_schema;
    Schema schema = [&final_tablet_schema, &tablet]() {
        if (final_tablet_schema->sort_key_idxes().empty()) {
            return ChunkHelper::get_sort_key_schema_by_primary_key(final_tablet_schema);
        } else {
            return ChunkHelper::convert_schema(final_tablet_schema);
        }
    }();
    std::unique_ptr<RowsetMerger> merger;
    auto key_type = PrimaryKeyEncoder::encoded_primary_key_type(schema, schema.sort_key_idxes());
    switch (key_type) {
    case TYPE_BOOLEAN:
        merger = std::make_unique<RowsetMergerImpl<uint8_t>>();
        break;
    case TYPE_TINYINT:
        merger = std::make_unique<RowsetMergerImpl<int8_t>>();
        break;
    case TYPE_SMALLINT:
        merger = std::make_unique<RowsetMergerImpl<int16_t>>();
        break;
    case TYPE_INT:
        merger = std::make_unique<RowsetMergerImpl<int32_t>>();
        break;
    case TYPE_BIGINT:
        merger = std::make_unique<RowsetMergerImpl<int64_t>>();
        break;
    case TYPE_LARGEINT:
        merger = std::make_unique<RowsetMergerImpl<int128_t>>();
        break;
    case TYPE_VARCHAR:
        merger = std::make_unique<RowsetMergerImpl<Slice>>();
        break;
    case TYPE_DATE:
        merger = std::make_unique<RowsetMergerImpl<int32_t>>();
        break;
    case TYPE_DATETIME:
        merger = std::make_unique<RowsetMergerImpl<int64_t>>();
        break;
    default:
        return Status::NotSupported(StringPrintf("primary key type not support: %s", logical_type_to_string(key_type)));
    }
    return merger->do_merge(tablet, final_tablet_schema, version, schema, rowsets, writer, cfg);
}

} // namespace starrocks
