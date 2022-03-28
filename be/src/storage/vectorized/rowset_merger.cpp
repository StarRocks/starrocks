// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/rowset_merger.h"

#include <memory>

#include "gutil/stl_util.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/pretty_printer.h"
#include "util/starrocks_metrics.h"

namespace starrocks::vectorized {

class RowsetMerger {
public:
    RowsetMerger() = default;

    virtual ~RowsetMerger() = default;

    virtual Status do_merge(Tablet& tablet, int64_t version, const Schema& schema,
                            const vector<RowsetSharedPtr>& rowsets, RowsetWriter* writer, const MergeConfig& cfg) = 0;
};

template <class T>
struct MergeEntry {
    const T* pk_cur = nullptr;
    const T* pk_last = nullptr;
    const T* pk_start = nullptr;
    uint32_t cur_segment_idx = 0;
    uint32_t rowset_seg_id = 0;
    ColumnPtr chunk_pk_column;
    ChunkPtr chunk;
    vector<ChunkIteratorPtr> segment_itrs;
    std::unique_ptr<RowsetReleaseGuard> rowset_release_guard;
    // set |encode_schema| if require encode chunk pk columns
    const vectorized::Schema* encode_schema = nullptr;

    MergeEntry() = default;
    ~MergeEntry() { close(); }

    string debug_string() {
        string ret;
        StringAppendF(&ret, "%u: %ld/%ld %u/%zu : ", rowset_seg_id, offset(pk_cur), offset(pk_last) + 1,
                      cur_segment_idx, segment_itrs.size());
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
        for (auto& itr : segment_itrs) {
            if (itr) {
                itr->close();
                itr.reset();
            }
        }
        STLClearObject(&segment_itrs);
        rowset_release_guard.reset();
    }

    Status init() {
        while (cur_segment_idx < segment_itrs.size() && !segment_itrs[cur_segment_idx]) {
            cur_segment_idx++;
        }
        return next();
    }

    Status next() {
        if (cur_segment_idx >= segment_itrs.size()) {
            return Status::EndOfFile("End of merge entry iterator");
        }
        DCHECK(pk_cur == nullptr || pk_cur > pk_last);
        chunk->reset();
        while (true) {
            auto& itr = segment_itrs[cur_segment_idx];
            auto st = itr->get_next(chunk.get());
            if (st.ok()) {
                // 1. setup chunk_pk_column
                if (encode_schema != nullptr) {
                    // need to encode
                    chunk_pk_column->reset_column();
                    PrimaryKeyEncoder::encode(*encode_schema, *chunk, 0, chunk->num_rows(), chunk_pk_column.get());
                } else {
                    // just use chunk's first column
                    chunk_pk_column = chunk->get_column_by_index(0);
                }
                DCHECK(chunk_pk_column->size() > 0);
                DCHECK(chunk_pk_column->size() == chunk->num_rows());
                // 2. setup pk cursor
                pk_start = reinterpret_cast<const T*>(chunk_pk_column->raw_data());
                pk_cur = pk_start;
                pk_last = pk_start + chunk_pk_column->size() - 1;
                return Status::OK();
            } else if (st.is_end_of_file()) {
                itr->close();
                itr.reset();
                while (true) {
                    cur_segment_idx++;
                    rowset_seg_id++;
                    if (cur_segment_idx == segment_itrs.size()) {
                        return Status::EndOfFile("End of merge entry iterator");
                    }
                    if (segment_itrs[cur_segment_idx]) {
                        break;
                    }
                }
                continue;
            } else {
                // error
                return st;
            }
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

    Status get_next(Chunk* chunk, vector<uint32_t>* rssids) {
        size_t nrow = 0;
        while (!_heap.empty() && nrow < _chunk_size) {
            MergeEntry<T>& top = *_heap.top();
            //LOG(INFO) << "m" << _heap.size() << " top: " << top.debug_string();
            DCHECK_LE(top.pk_cur, top.pk_last);
            _heap.pop();
            if (_heap.empty() || *(top.pk_last) < *(_heap.top()->pk_cur)) {
                if (nrow == 0 && top.at_start()) {
                    chunk->swap_chunk(*top.chunk);
                    rssids->insert(rssids->end(), chunk->num_rows(), top.rowset_seg_id);
                    top.pk_cur = top.pk_last + 1;
                    return _fill_heap(&top);
                } else {
                    // TODO(cbl): make dest chunk size larger, so we can copy all rows at once
                    int nappend = std::min((int)(top.pk_last - top.pk_cur + 1), (int)(_chunk_size - nrow));
                    auto start_offset = top.offset(top.pk_cur);
                    chunk->append(*top.chunk, start_offset, nappend);
                    rssids->insert(rssids->end(), nappend, top.rowset_seg_id);
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
                rssids->push_back(top.rowset_seg_id);
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

    Status do_merge(Tablet& tablet, int64_t version, const Schema& schema, const vector<RowsetSharedPtr>& rowsets,
                    RowsetWriter* writer, const MergeConfig& cfg) override {
        MonotonicStopWatch timer;
        timer.start();
        _chunk_size = cfg.chunk_size;
        OlapReaderStatistics stats;
        std::unique_ptr<vectorized::Column> pk_column;
        if (schema.num_key_fields() > 1) {
            if (!PrimaryKeyEncoder::create_column(schema, &pk_column).ok()) {
                LOG(FATAL) << "create column for primary key encoder failed";
            }
        }
        size_t total_input_size = 0;
        for (const auto& i : rowsets) {
            total_input_size += i->data_disk_size();
            _entries.emplace_back(new MergeEntry<T>());
            MergeEntry<T>& entry = *_entries.back();
            entry.rowset_release_guard = std::make_unique<RowsetReleaseGuard>(i);
            auto rowset = i.get();
            auto beta_rowset = down_cast<BetaRowset*>(rowset);
            auto res = beta_rowset->get_segment_iterators2(schema, tablet.data_dir()->get_meta(), version, &stats);
            if (!res.ok()) {
                return res.status();
            }
            entry.rowset_seg_id = rowset->rowset_meta()->get_rowset_seg_id();
            entry.segment_itrs.swap(res.value());
            entry.chunk = ChunkHelper::new_chunk(schema, _chunk_size);
            if (pk_column) {
                entry.encode_schema = &schema;
                entry.chunk_pk_column = pk_column->clone_shared();
                entry.chunk_pk_column->reserve(_chunk_size);
            }
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

        size_t total_rows = 0;
        size_t total_chunk = 0;
        auto chunk = ChunkHelper::new_chunk(schema, _chunk_size);
        vector<uint32_t> rssids;
        rssids.reserve(_chunk_size);
        while (true) {
            chunk->reset();
            rssids.clear();
            Status status = get_next(chunk.get(), &rssids);
            if (!status.ok()) {
                if (status.is_end_of_file()) {
                    break;
                } else {
                    LOG(WARNING) << "reader get next error. tablet=" << tablet.tablet_id()
                                 << ", err=" << status.to_string();
                    return Status::InternalError(fmt::format("reader get_next error: {}", status.to_string()));
                }
            }

            ChunkHelper::padding_char_columns(char_field_indexes, schema, tablet.tablet_schema(), chunk.get());

            total_rows += chunk->num_rows();
            total_chunk++;
            OLAPStatus olap_status = writer->add_chunk_with_rssid(*chunk, rssids);
            if (olap_status != OLAP_SUCCESS) {
                LOG(WARNING) << "writer add_chunk error, err=" << olap_status;
                return Status::InternalError("writer add_chunk error.");
            }
        }
        OLAPStatus olap_status = writer->flush();
        if (olap_status != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to flush rowset when merging rowsets of tablet " + tablet.full_name()
                         << ", err=" << olap_status;
            return Status::InternalError("failed to flush rowset when merging rowsets of tablet error.");
        }
        timer.stop();
        if (stats.raw_rows_read - stats.rows_del_vec_filtered != total_rows) {
            string msg = Substitute("update compaction rows read($0) != rows written($1)",
                                    stats.raw_rows_read - stats.rows_del_vec_filtered, total_rows);
            DCHECK(false) << msg;
            LOG(WARNING) << msg;
        }
        StarRocksMetrics::instance()->update_compaction_deltas_total.increment(rowsets.size());
        StarRocksMetrics::instance()->update_compaction_bytes_total.increment(total_input_size);
        StarRocksMetrics::instance()->update_compaction_outputs_total.increment(1);
        StarRocksMetrics::instance()->update_compaction_outputs_bytes_total.increment(writer->total_data_size());
        LOG(INFO) << "compaction merge finished. tablet:" << tablet.tablet_id() << " #key:" << schema.num_key_fields()
                  << " input("
                  << "entry=" << _entries.size() << " rows=" << stats.raw_rows_read
                  << " del=" << stats.rows_del_vec_filtered
                  << " actual=" << stats.raw_rows_read - stats.rows_del_vec_filtered
                  << " bytes=" << PrettyPrinter::print(total_input_size, TUnit::BYTES) << ") output(rows=" << total_rows
                  << " chunk=" << total_chunk
                  << " bytes=" << PrettyPrinter::print(writer->total_data_size(), TUnit::BYTES)
                  << ") duration: " << timer.elapsed_time() / 1000000 << "ms";
        return Status::OK();
    }

private:
    size_t _chunk_size = 0;
    std::vector<std::unique_ptr<MergeEntry<T>>> _entries;
    using Heap = std::priority_queue<MergeEntry<T>*, std::vector<MergeEntry<T>*>, MergeEntryCmp<T>>;
    Heap _heap;
};

Status compaction_merge_rowsets(Tablet& tablet, int64_t version, const vector<RowsetSharedPtr>& rowsets,
                                RowsetWriter* writer, const MergeConfig& cfg) {
    Schema schema = ChunkHelper::convert_schema_to_format_v2(tablet.tablet_schema());
    std::unique_ptr<RowsetMerger> merger;
    auto key_type = PrimaryKeyEncoder::encoded_primary_key_type(schema);
    switch (key_type) {
    case OLAP_FIELD_TYPE_BOOL:
        merger = std::make_unique<RowsetMergerImpl<uint8_t>>();
        break;
    case OLAP_FIELD_TYPE_TINYINT:
        merger = std::make_unique<RowsetMergerImpl<int8_t>>();
        break;
    case OLAP_FIELD_TYPE_SMALLINT:
        merger = std::make_unique<RowsetMergerImpl<int16_t>>();
        break;
    case OLAP_FIELD_TYPE_INT:
        merger = std::make_unique<RowsetMergerImpl<int32_t>>();
        break;
    case OLAP_FIELD_TYPE_BIGINT:
        merger = std::make_unique<RowsetMergerImpl<int64_t>>();
        break;
    case OLAP_FIELD_TYPE_LARGEINT:
        merger = std::make_unique<RowsetMergerImpl<int128_t>>();
        break;
    case OLAP_FIELD_TYPE_VARCHAR:
        merger = std::make_unique<RowsetMergerImpl<Slice>>();
        break;
    case OLAP_FIELD_TYPE_DATE_V2:
        merger = std::make_unique<RowsetMergerImpl<int32_t>>();
        break;
    case OLAP_FIELD_TYPE_TIMESTAMP:
        merger = std::make_unique<RowsetMergerImpl<int64_t>>();
        break;
    default:
        return Status::NotSupported(StringPrintf("primary key type not support: %s", field_type_to_string(key_type)));
    }
    return merger->do_merge(tablet, version, schema, rowsets, writer, cfg);
}

} // namespace starrocks::vectorized
