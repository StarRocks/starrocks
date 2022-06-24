// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/tablets_channel.h

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
#pragma once

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
DIAGNOSTIC_POP

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "common/statusor.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/ref_counted.h"
#include "runtime/descriptors.h"
#include "runtime/global_dicts.h"
#include "runtime/mem_tracker.h"
#include "serde/protobuf_serde.h"
#include "storage/vectorized/async_delta_writer.h"
#include "util/countdown_latch.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class OlapTableSchemaParam;
class LoadChannel;

struct TabletsChannelKey {
    UniqueId id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t index_id_) : id(pid), index_id(index_id_) {}

    ~TabletsChannelKey() noexcept = default;

    bool operator==(const TabletsChannelKey& rhs) const noexcept { return index_id == rhs.index_id && id == rhs.id; }

    std::string to_string() const;
};

inline std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key);

// Write channel for a particular (load, index).
class TabletsChannel : public RefCountedThreadSafe<TabletsChannel> {
    using AsyncDeltaWriter = vectorized::AsyncDeltaWriter;
    using AsyncDeltaWriterCallback = vectorized::AsyncDeltaWriterCallback;
    using AsyncDeltaWriterRequest = vectorized::AsyncDeltaWriterRequest;
    using CommittedRowsetInfo = vectorized::CommittedRowsetInfo;

public:
    TabletsChannel(LoadChannel* load_channel, const TabletsChannelKey& key, MemTracker* mem_tracker);

    TabletsChannel(const TabletsChannel&) = delete;
    TabletsChannel(TabletsChannel&&) = delete;
    void operator=(const TabletsChannel&) = delete;
    void operator=(TabletsChannel&&) = delete;

    const TabletsChannelKey& key() const { return _key; }

    // NOTE: This method is not retryable.
    Status open(const PTabletWriterOpenRequest& params);

    void add_chunk(brpc::Controller* cntl, const PTabletWriterAddChunkRequest& request,
                   PTabletWriterAddBatchResult* response, google::protobuf::Closure* done);

    void cancel();

    MemTracker* mem_tracker() { return _mem_tracker; }

private:
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    friend class RefCountedThreadSafe<TabletsChannel>;
    ~TabletsChannel();

    static std::atomic<uint64_t> _s_tablet_writer_count;

    struct Sender {
        bthread::Mutex lock;
        int64_t next_seq = 0;
        bool closed = false;
    };

    class WriteContext : public RefCountedThreadSafe<WriteContext> {
    public:
        explicit WriteContext(PTabletWriterAddBatchResult* response) : _response(response), _latch(nullptr) {}

        WriteContext(const WriteContext&) = delete;
        void operator=(const WriteContext&) = delete;
        WriteContext(WriteContext&&) = delete;
        void operator=(WriteContext&&) = delete;

        void update_status(const Status& status) {
            if (status.ok() || _response == nullptr) {
                return;
            }
            std::lock_guard l(_response_lock);
            if (_response->status().status_code() == TStatusCode::OK) {
                status.to_protobuf(_response->mutable_status());
            }
        }

        void add_committed_tablet_info(PTabletInfo* tablet_info) {
            DCHECK(_response != nullptr);
            std::lock_guard l(_response_lock);
            _response->add_tablet_vec()->Swap(tablet_info);
        }

        void set_count_down_latch(BThreadCountDownLatch* latch) { _latch = latch; }

        void clear_response() { _response = nullptr; }

    private:
        friend class TabletsChannel;
        friend class RefCountedThreadSafe<WriteContext>;
        ~WriteContext() {
            if (_latch) _latch->count_down();
        }

        mutable bthread::Mutex _response_lock;
        PTabletWriterAddBatchResult* _response;
        BThreadCountDownLatch* _latch;

        vectorized::Chunk _chunk;
        std::unique_ptr<uint32_t[]> _row_indexes;
        std::unique_ptr<uint32_t[]> _channel_row_idx_start_points;
    };

    class WriteCallback : public AsyncDeltaWriterCallback {
    public:
        explicit WriteCallback(WriteContext* context) : _context(context) { _context->AddRef(); }

        ~WriteCallback() override { _context->Release(); }

        void run(const Status& st, const CommittedRowsetInfo* info) override;

        WriteCallback(const WriteCallback&) = delete;
        void operator=(const WriteCallback&) = delete;
        WriteCallback(WriteCallback&&) = delete;
        void operator=(WriteCallback&&) = delete;

    private:
        WriteContext* _context;
    };

    Status _open_all_writers(const PTabletWriterOpenRequest& params);

    Status _build_chunk_meta(const ChunkPB& pb_chunk);

    StatusOr<scoped_refptr<WriteContext>> _create_write_context(const PTabletWriterAddChunkRequest& request,
                                                                PTabletWriterAddBatchResult* response,
                                                                google::protobuf::Closure* done);

    int _close_sender(Sender* sender, const int64_t* partitions, size_t partitions_size);

    LoadChannel* _load_channel;

    TabletsChannelKey _key;

    MemTracker* _mem_tracker;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    OlapTableSchemaParam* _schema = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    RowDescriptor* _row_desc = nullptr;

    // next sequence we expect
    std::atomic<int> _num_remaining_senders;
    std::vector<Sender> _senders;

    mutable bthread::Mutex _partitions_ids_lock;
    std::unordered_set<int64_t> _partition_ids;

    mutable bthread::Mutex _chunk_meta_lock;
    serde::ProtobufChunkMeta _chunk_meta;
    std::atomic<bool> _has_chunk_meta;

    std::unordered_map<int64_t, uint32_t> _tablet_id_to_sorted_indexes;
    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, std::unique_ptr<AsyncDeltaWriter>> _delta_writers;

    vectorized::GlobalDictByNameMaps _global_dicts;
    std::unique_ptr<MemPool> _mem_pool;
};

} // namespace starrocks
