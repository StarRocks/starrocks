// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/tablets_channel.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/tablets_channel.h"

#include "exec/tablet_info.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "storage/delta_writer.h"
#include "storage/memtable.h"
#include "storage/vectorized/delta_writer.h"
#include "storage/vectorized/memtable.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

std::atomic<uint64_t> TabletsChannel::_s_tablet_writer_count;

TabletsChannel::TabletsChannel(const TabletsChannelKey& key, MemTracker* mem_tracker)
        : _key(key), _state(kInitialized), _closed_senders(64) {
    _mem_tracker = std::make_unique<MemTracker>(-1, "tablets channel", mem_tracker, true);
    _mem_pool = std::make_unique<MemPool>();
    static std::once_flag once_flag;
    std::call_once(once_flag, [] {
        REGISTER_GAUGE_STARROCKS_METRIC(tablet_writer_count, [&]() { return _s_tablet_writer_count.load(); });
    });
}

TabletsChannel::~TabletsChannel() {
    _s_tablet_writer_count -= _tablet_writers.size();
    _s_tablet_writer_count -= _vectorized_tablet_writers.size();
    delete _row_desc;
    delete _schema;
    _mem_pool.reset();
}

Status TabletsChannel::open(const PTabletWriterOpenRequest& params) {
    std::lock_guard<std::mutex> l(_global_lock);
    if (_state == kOpened) {
        // Normal case, already open by other sender
        return Status::OK();
    }
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = new OlapTableSchemaParam();
    RETURN_IF_ERROR(_schema->init(params.schema()));
    _tuple_desc = _schema->tuple_desc();
    _row_desc = new RowDescriptor(_tuple_desc, false);

    _num_remaining_senders = params.num_senders();
    _next_seqs.resize(_num_remaining_senders, 0);
    _closed_senders.Reset(_num_remaining_senders);

    _is_vectorized = params.is_vectorized();
    RETURN_IF_ERROR(_open_all_writers(params));

    _state = kOpened;
    return Status::OK();
}

Status TabletsChannel::add_batch(const PTabletWriterAddBatchRequest& params) {
    DCHECK(_is_vectorized == false);
    DCHECK(params.tablet_ids_size() == params.row_batch().num_rows());
    std::lock_guard<std::mutex> l(_global_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    auto next_seq = _next_seqs[params.sender_id()];
    // check packet
    if (params.packet_seq() < next_seq) {
        LOG(INFO) << "packet has already recept before, expect_seq=" << next_seq
                  << ", recept_seq=" << params.packet_seq();
        return Status::OK();
    } else if (params.packet_seq() > next_seq) {
        LOG(WARNING) << "lost data packet, expect_seq=" << next_seq << ", recept_seq=" << params.packet_seq();
        return Status::InternalError("lost data packet");
    }

    RowBatch row_batch(*_row_desc, params.row_batch());
    Status status = row_batch.init(params.row_batch());
    if (!status.ok()) {
        return Status::InternalError("batch init failed for tablet to append data");
    }

    // iterator all data
    for (int i = 0; i < params.tablet_ids_size(); ++i) {
        auto tablet_id = params.tablet_ids(i);
        auto it = _tablet_writers.find(tablet_id);
        if (it == std::end(_tablet_writers)) {
            return Status::InternalError(strings::Substitute("unknown tablet to append data, tablet=$0", tablet_id));
        }
        auto st = it->second->write(row_batch.get_row(i)->get_tuple(0));
        if (st != OLAP_SUCCESS) {
            const std::string& err_msg = strings::Substitute(
                    "tablet writer write failed, tablet_id=$0, txn_id=$1, err=$2", it->first, _txn_id, st);
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
    }
    _next_seqs[params.sender_id()]++;
    return Status::OK();
}

Status TabletsChannel::add_chunk(const PTabletWriterAddChunkRequest& params) {
    DCHECK(_is_vectorized == true);
    {
        std::lock_guard<std::mutex> l(_global_lock);
        if (_state == kFinished) {
            return _close_status;
        }

        auto next_seq = _next_seqs[params.sender_id()];
        // check packet
        if (params.packet_seq() < next_seq) {
            LOG(INFO) << "packet has already recept before, expect_seq=" << next_seq
                      << ", recept_seq=" << params.packet_seq();
            return Status::OK();
        } else if (params.packet_seq() > next_seq) {
            LOG(WARNING) << "lost data packet, expect_seq=" << next_seq << ", recept_seq=" << params.packet_seq();
            return Status::InternalError("lost data packet");
        }
    }

    auto& pchunk = params.chunk();
    {
        std::lock_guard<std::mutex> l(_global_lock);
        if (_chunk_meta.types.empty()) {
            RETURN_IF_ERROR(_build_chunk_meta(pchunk));
        }
    }

    vectorized::Chunk chunk;
    RETURN_IF_ERROR(chunk.deserialize((const uint8_t*)pchunk.data().data(), pchunk.data().size(), _chunk_meta));
    DCHECK_EQ(params.tablet_ids_size(), chunk.num_rows());

    size_t channel_size = _tablet_id_to_sorted_indexes.size();
    std::vector<uint32_t> row_indexes(chunk.num_rows());
    std::vector<uint32_t> channel_row_idx_start_points(channel_size + 1);
    {
        // compute row indexes for each channel
        channel_row_idx_start_points.assign(channel_size + 1, 0);
        for (uint32_t i = 0; i < params.tablet_ids_size(); ++i) {
            uint32_t channel_index = _tablet_id_to_sorted_indexes[params.tablet_ids(i)];
            channel_row_idx_start_points[channel_index]++;
        }

        // NOTE: we make the last item equal with number of rows of this chunk
        for (int i = 1; i <= channel_size; ++i) {
            channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
        }

        for (int i = params.tablet_ids_size() - 1; i >= 0; --i) {
            uint32_t channel_index = _tablet_id_to_sorted_indexes[params.tablet_ids(i)];
            row_indexes[channel_row_idx_start_points[channel_index] - 1] = i;
            channel_row_idx_start_points[channel_index]--;
        }
    }

    for (int i = 0; i < channel_size; ++i) {
        size_t from = channel_row_idx_start_points[i];
        size_t size = channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            // no data for this channel continue;
            continue;
        }
        auto tablet_id = params.tablet_ids(row_indexes[from]);
        auto it = _vectorized_tablet_writers.find(tablet_id);
        if (it == std::end(_vectorized_tablet_writers)) {
            return Status::InternalError(strings::Substitute("unknown tablet to append data, tablet=$0", tablet_id));
        }
        {
            std::lock_guard<std::mutex> l(_tablet_locks[tablet_id & k_shard_size]);
            auto st = it->second->write(&chunk, row_indexes.data(), from, size);
            if (!st.ok()) {
                return st;
            }
        }
    }

    {
        std::lock_guard<std::mutex> l(_global_lock);
        _next_seqs[params.sender_id()]++;
    }
    return Status::OK();
}

Status TabletsChannel::_build_chunk_meta(const ChunkPB& pb_chunk) {
    if (UNLIKELY(pb_chunk.is_nulls().empty() || pb_chunk.slot_id_map().empty())) {
        return Status::InternalError("pb_chunk meta could not be empty");
    }

    _chunk_meta.slot_id_to_index.init(pb_chunk.slot_id_map().size());
    for (int i = 0; i < pb_chunk.slot_id_map().size(); i += 2) {
        _chunk_meta.slot_id_to_index.insert(pb_chunk.slot_id_map()[i], pb_chunk.slot_id_map()[i + 1]);
    }

    _chunk_meta.is_nulls.resize(pb_chunk.is_nulls().size());
    for (int i = 0; i < pb_chunk.is_nulls().size(); ++i) {
        _chunk_meta.is_nulls[i] = pb_chunk.is_nulls()[i];
    }
    _chunk_meta.is_consts.resize(pb_chunk.is_nulls().size(), false);

    size_t column_index = 0;
    _chunk_meta.types.resize(pb_chunk.is_nulls().size());
    for (auto tuple_desc : _row_desc->tuple_descriptors()) {
        const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();
        for (const auto& kv : _chunk_meta.slot_id_to_index) {
            for (auto slot : slots) {
                if (kv.first == slot->id()) {
                    _chunk_meta.types[kv.second] = slot->type();
                    ++column_index;
                    break;
                }
            }
        }
    }

    if (UNLIKELY(column_index != _chunk_meta.is_nulls.size())) {
        return Status::InternalError("build chunk meta error");
    }
    return Status::OK();
}

Status TabletsChannel::close(int sender_id, bool* finished,
                             const google::protobuf::RepeatedField<int64_t>& partition_ids,
                             google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    {
        std::lock_guard<std::mutex> l(_global_lock);
        if (_state == kFinished) {
            return _close_status;
        }
        if (_closed_senders.Get(sender_id)) {
            // Double close from one sender, just return OK
            *finished = (_num_remaining_senders == 0);
            return _close_status;
        }
        for (auto pid : partition_ids) {
            _partition_ids.emplace(pid);
        }
        _closed_senders.Set(sender_id, true);
        _num_remaining_senders--;
        *finished = (_num_remaining_senders == 0);
        if (*finished) {
            _state = kFinished;
            if (!_is_vectorized) {
                // All senders are closed
                // 1. close all delta writers
                std::vector<std::shared_ptr<DeltaWriter>> need_wait_writers;
                for (auto& it : _tablet_writers) {
                    if (_partition_ids.count(it.second->partition_id()) > 0) {
                        auto st = it.second->close();
                        if (st != OLAP_SUCCESS) {
                            LOG(WARNING) << "Fail to close tablet writer, tablet_id=" << it.first
                                         << " transaction_id=" << _txn_id << " err=" << st;
                            // just skip this tablet(writer) and continue to close others
                            continue;
                        }
                        need_wait_writers.push_back(it.second);
                    } else {
                        auto st = it.second->cancel();
                        if (st != OLAP_SUCCESS) {
                            LOG(WARNING) << "Fail to cancel tablet writer, tablet_id=" << it.first
                                         << " transaction_id=" << _txn_id;
                            // just skip this tablet(writer) and continue to close others
                            continue;
                        }
                    }
                }

                // 2. wait delta writers and build the tablet vector
                for (auto writer : need_wait_writers) {
                    // close may return failed, but no need to handle it here.
                    // tablet_vec will only contains success tablet, and then let FE judge it.
                    writer->close_wait(tablet_vec);
                }
            }
        }
    }

    if (*finished & _is_vectorized) {
        // All senders are closed
        // 1. close all delta writers
        std::unordered_map<int64_t, std::shared_ptr<vectorized::DeltaWriter>> need_wait_writers;
        for (auto& it : _vectorized_tablet_writers) {
            if (_partition_ids.count(it.second->partition_id()) > 0) {
                std::lock_guard<std::mutex> l(_tablet_locks[it.first & k_shard_size]);
                auto st = it.second->close();
                if (!st.ok()) {
                    LOG(WARNING) << "Fail to close tablet writer, tablet_id=" << it.first
                                 << " transaction_id=" << _txn_id << " err=" << st.to_string();
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
                need_wait_writers.emplace(it.first, it.second);
            } else {
                std::lock_guard<std::mutex> l(_tablet_locks[it.first & k_shard_size]);
                auto st = it.second->cancel();
                if (!st.ok()) {
                    LOG(WARNING) << "Fail to cancel tablet writer, tablet_id=" << it.first
                                 << " transaction_id=" << _txn_id;
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
            }
        }

        // 2. wait delta writers and build the tablet vector
        for (auto& it : need_wait_writers) {
            std::lock_guard<std::mutex> l(_tablet_locks[it.first & k_shard_size]);
            // close may return failed, but no need to handle it here.
            // tablet_vec will only contains success tablet, and then let FE judge it.
            it.second->close_wait(tablet_vec);
        }
    }

    return Status::OK();
}

Status TabletsChannel::reduce_mem_usage_async(const std::set<int64_t>& flush_tablet_ids, int64_t* tablet_id,
                                              int64_t* tablet_mem_consumption) {
    std::shared_ptr<vectorized::DeltaWriter> vectorized_writer;
    int64_t max_consume = 0L;

    {
        std::lock_guard<std::mutex> l(_global_lock);
        if (_state == kFinished) {
            // TabletsChannel is closed without LoadChannel's lock,
            // therefore it's possible for reduce_mem_usage_async() to be called right after close().
            return _close_status;
        }

        if (_is_vectorized) {
            // find tablet writer with largest mem consumption
            for (auto& it : _vectorized_tablet_writers) {
                if (it.second->mem_consumption() > max_consume &&
                    flush_tablet_ids.find(it.first) == flush_tablet_ids.end()) {
                    max_consume = it.second->mem_consumption();
                    vectorized_writer = it.second;
                    *tablet_id = it.first;
                }
            }
        } else {
            std::shared_ptr<DeltaWriter> writer;
            // find tablet writer with largest mem consumption
            for (auto& it : _tablet_writers) {
                if (it.second->mem_consumption() > max_consume &&
                    flush_tablet_ids.find(it.first) == flush_tablet_ids.end()) {
                    max_consume = it.second->mem_consumption();
                    writer = it.second;
                    *tablet_id = it.first;
                }
            }

            if (writer.get() == nullptr || max_consume == 0) {
                // barely not happend, just return OK
                return Status::OK();
            }
            VLOG(3) << "pick the delta writer to flush, with mem consumption: " << max_consume
                    << ", channel key: " << _key;
            *tablet_mem_consumption = max_consume;
            return writer->flush_memtable_async();
        }
    }
    if (_is_vectorized) {
        if (vectorized_writer.get() == nullptr || max_consume == 0) {
            // barely not happend, just return OK
            return Status::OK();
        }
        VLOG(3) << "pick the delta writer to flush, with mem consumption: " << max_consume << ", channel key: " << _key;
        *tablet_mem_consumption = max_consume;
        std::lock_guard<std::mutex> l(_tablet_locks[*tablet_id & k_shard_size]);
        return vectorized_writer->flush_memtable_async();
    }
    return Status::OK();
}

Status TabletsChannel::wait_mem_usage_reduced(int64_t tablet_id) {
    std::shared_ptr<vectorized::DeltaWriter> vectorized_writer;
    {
        std::lock_guard<std::mutex> l(_global_lock);
        if (_is_vectorized) {
            auto it = _vectorized_tablet_writers.find(tablet_id);
            if (it == _vectorized_tablet_writers.end()) {
                std::stringstream ss;
                ss << "tablet writer is not found. tablet id: " << tablet_id;
                return Status::InternalError(ss.str());
            }
            vectorized_writer = it->second;
        } else {
            auto it = _tablet_writers.find(tablet_id);
            if (it == _tablet_writers.end()) {
                std::stringstream ss;
                ss << "tablet writer is not found. tablet id: " << tablet_id;
                return Status::InternalError(ss.str());
            }
            auto writer = it->second;
            return writer->wait_memtable_flushed();
        }
    }

    if (_is_vectorized) {
        std::lock_guard<std::mutex> l(_tablet_locks[tablet_id & k_shard_size]);
        return vectorized_writer->wait_memtable_flushed();
    }
    return Status::OK();
}

Status TabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& params) {
    std::vector<SlotDescriptor*>* index_slots = nullptr;
    int32_t schema_hash = 0;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            index_slots = &index->slots;
            schema_hash = index->schema_hash;
            break;
        }
    }
    if (index_slots == nullptr) {
        std::stringstream ss;
        ss << "unknown index id, key=" << _key;
        return Status::InternalError(ss.str());
    }
    if (_is_vectorized) {
        // init global dict info if need
        for (auto& slot : params.schema().slot_descs()) {
            vectorized::GlobalDictMap global_dict;
            if (slot.global_dict_words_size()) {
                for (size_t i = 0; i < slot.global_dict_words_size(); i++) {
                    const std::string& dict_word = slot.global_dict_words(i);
                    auto* data = _mem_pool->allocate(dict_word.size());
                    memcpy(data, dict_word.data(), dict_word.size());
                    Slice slice(data, dict_word.size());
                    global_dict.emplace(slice, i);
                }
                _global_dicts.insert(std::make_pair(slot.col_name(), std::move(global_dict)));
            }
        }

        std::vector<int64_t> tablet_ids;
        tablet_ids.reserve(params.tablets_size());
        for (auto& tablet : params.tablets()) {
            vectorized::WriteRequest request;
            request.tablet_id = tablet.tablet_id();
            request.schema_hash = schema_hash;
            request.write_type = vectorized::WriteType::LOAD;
            request.txn_id = _txn_id;
            request.partition_id = tablet.partition_id();
            request.load_id = params.id();
            request.tuple_desc = _tuple_desc;
            request.slots = index_slots;
            request.global_dicts = &_global_dicts;

            std::shared_ptr<vectorized::DeltaWriter> writer;
            auto st = vectorized::DeltaWriter::open(&request, _mem_tracker.get(), &writer);
            if (!st.ok()) {
                std::stringstream ss;
                ss << "open delta writer failed, tablet_id=" << tablet.tablet_id() << ", txn_id=" << _txn_id
                   << ", partition_id=" << tablet.partition_id() << ", err=" << st.to_string();
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
            _vectorized_tablet_writers.emplace(tablet.tablet_id(), writer);
            tablet_ids.emplace_back(tablet.tablet_id());
        }
        _s_tablet_writer_count += _vectorized_tablet_writers.size();
        DCHECK_EQ(_vectorized_tablet_writers.size(), params.tablets_size());
        // In order to get sorted index for each tablet
        std::sort(tablet_ids.begin(), tablet_ids.end());
        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            _tablet_id_to_sorted_indexes.emplace(tablet_ids[i], i);
        }
    } else {
        for (auto& tablet : params.tablets()) {
            WriteRequest request;
            request.tablet_id = tablet.tablet_id();
            request.schema_hash = schema_hash;
            request.write_type = WriteType::LOAD;
            request.txn_id = _txn_id;
            request.partition_id = tablet.partition_id();
            request.load_id = params.id();
            request.tuple_desc = _tuple_desc;
            request.slots = index_slots;

            std::shared_ptr<DeltaWriter> writer;
            auto st = DeltaWriter::open(&request, _mem_tracker.get(), &writer);
            if (st != OLAP_SUCCESS) {
                std::stringstream ss;
                ss << "open delta writer failed, tablet_id=" << tablet.tablet_id() << ", txn_id=" << _txn_id
                   << ", partition_id=" << tablet.partition_id() << ", err=" << st;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
            _tablet_writers.emplace(tablet.tablet_id(), writer);
        }
        _s_tablet_writer_count += _tablet_writers.size();
        DCHECK_EQ(_tablet_writers.size(), params.tablets_size());
    }
    return Status::OK();
}

Status TabletsChannel::cancel() {
    {
        std::lock_guard<std::mutex> l(_global_lock);
        if (_state == kFinished) {
            return _close_status;
        }
        for (auto& it : _tablet_writers) {
            it.second->cancel();
        }
    }

    for (auto& it : _vectorized_tablet_writers) {
        std::lock_guard<std::mutex> l(_tablet_locks[it.first & k_shard_size]);
        it.second->cancel();
    }

    std::lock_guard<std::mutex> l(_global_lock);
    _state = kFinished;
    return Status::OK();
}

std::string TabletsChannelKey::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key) {
    os << "(id=" << key.id << ",index_id=" << key.index_id << ")";
    return os;
}

} // namespace starrocks
