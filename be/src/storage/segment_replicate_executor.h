// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "common/status.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "storage/olap_define.h"
#include "util/reusable_closure.h"
#include "util/spinlock.h"
#include "util/threadpool.h"

namespace starrocks {

class DataDir;
class ExecEnv;
class SegmentPB;
class PTabletInfo;
class FileSystem;

namespace vectorized {
class DeltaWriterOptions;
}

using DeltaWriterOptions = starrocks::vectorized::DeltaWriterOptions;

class ReplicateChannel {
public:
    ReplicateChannel(const DeltaWriterOptions* opt, std::string host, int32_t port, int64_t node_id);
    ~ReplicateChannel();

    Status sync_segment(SegmentPB* segment, butil::IOBuf& data, bool eos,
                        std::vector<std::unique_ptr<PTabletInfo>>* replicate_tablet_infos,
                        std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos);

    Status async_segment(SegmentPB* segment, butil::IOBuf& data, bool eos,
                         std::vector<std::unique_ptr<PTabletInfo>>* replicate_tablet_infos,
                         std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos);

    void cancel();

    int64_t node_id() { return _node_id; }

    std::string debug_string();

private:
    Status _init();
    void _send_request(SegmentPB* segment, butil::IOBuf& data, bool eos);
    Status _wait_response(std::vector<std::unique_ptr<PTabletInfo>>* replicate_tablet_infos,
                          std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos);

    const DeltaWriterOptions* _opt;
    const std::string _host;
    const int32_t _port;
    const int64_t _node_id;

    ReusableClosure<PTabletWriterAddSegmentResult>* _closure = nullptr;
    doris::PBackendService_Stub* _stub = nullptr;

    bool _inited = false;
    Status _st = Status::OK();
};

class ReplicateToken {
public:
    ReplicateToken(std::unique_ptr<ThreadPoolToken> sync_pool_token, const DeltaWriterOptions* opt);
    ~ReplicateToken() = default;

    Status submit(std::unique_ptr<SegmentPB> segment, bool eos);

    // when error has happpens, so we cancel this token
    // and remove all tasks in the queue.
    void cancel();

    // wait all tasks in token to be completed.
    Status wait();

    Status status() const {
        std::lock_guard l(_status_lock);
        return _status;
    }

    void set_status(const Status& status) {
        if (status.ok()) return;
        std::lock_guard l(_status_lock);
        if (_status.ok()) _status = status;
    }

    std::string debug_string();

    const std::vector<std::unique_ptr<PTabletInfo>>* replicated_tablet_infos() const {
        return &_replicated_tablet_infos;
    }

    const std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos() const { return &_failed_tablet_infos; }

private:
    friend class SegmentReplicateTask;

    void _sync_segment(std::unique_ptr<SegmentPB> segment, bool eos);

    std::unique_ptr<ThreadPoolToken> _replicate_token;

    mutable SpinLock _status_lock;
    // Records the current flush status of the tablet.
    Status _status;

    const DeltaWriterOptions* _opt;

    std::vector<std::unique_ptr<ReplicateChannel>> _replicate_channels;

    std::vector<std::unique_ptr<PTabletInfo>> _replicated_tablet_infos;
    std::vector<std::unique_ptr<PTabletInfo>> _failed_tablet_infos;

    std::unique_ptr<FileSystem> _fs;

    std::set<int64_t> _failed_node_id;

    int64_t _max_fail_replica_num;
};

class SegmentReplicateExecutor {
public:
    SegmentReplicateExecutor() = default;
    ~SegmentReplicateExecutor() = default;

    // init should be called after storage engine is opened,
    // because it needs path hash of each data dir.
    Status init(const std::vector<DataDir*>& data_dirs);

    // NOTE: we use SERIAL mode here to ensure all segment from one tablet are synced in order.
    std::unique_ptr<ReplicateToken> create_replicate_token(
            const DeltaWriterOptions* opt,
            ThreadPool::ExecutionMode execution_mode = ThreadPool::ExecutionMode::SERIAL);

private:
    std::unique_ptr<ThreadPool> _replicate_pool;
};

} // namespace starrocks
