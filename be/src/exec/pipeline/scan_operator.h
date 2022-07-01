// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/spinlock.h"

namespace starrocks {

class PriorityThreadPool;
class ScanNode;

namespace pipeline {

class ScanOperator : public SourceOperator {
public:
    ScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node);

    ~ScanOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool pending_finish() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    void set_io_threads(PriorityThreadPool* io_threads) { _io_threads = io_threads; }

    void set_workgroup(workgroup::WorkGroupPtr wg) { _workgroup = std::move(wg); }

    // interface for different scan node
    virtual Status do_prepare(RuntimeState* state) = 0;
    virtual void do_close(RuntimeState* state) = 0;
    virtual ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) = 0;

    int64_t get_last_scan_rows_num() { return _last_scan_rows_num.exchange(0); }
    int64_t get_last_scan_bytes() { return _last_scan_bytes.exchange(0); }

<<<<<<< HEAD:be/src/exec/pipeline/scan_operator.h
=======
    // It takes effect, only when it is positive.
    virtual size_t max_scan_concurrency() const { return 0; }

protected:
    static constexpr int MAX_IO_TASKS_PER_OP = 4;
    const size_t _buffer_size = config::pipeline_io_buffer_size;

    // Shared scan
    virtual void attach_chunk_source(int32_t source_index) = 0;
    virtual void detach_chunk_source(int32_t source_index) {}
    virtual bool has_shared_chunk_source() const = 0;
    virtual bool has_buffer_output() const = 0;
    virtual bool has_available_buffer() const = 0;
    virtual ChunkPtr get_chunk_from_buffer() = 0;

>>>>>>> 14983c7e1 ([Refactor] refactor and fix the scan counters (#8088)):be/src/exec/pipeline/scan/scan_operator.h
private:
    // This method is only invoked when current morsel is reached eof
    // and all cached chunk of this morsel has benn read out
    Status _pickup_morsel(RuntimeState* state, int chunk_source_index);
    Status _trigger_next_scan(RuntimeState* state, int chunk_source_index);
    Status _try_to_trigger_next_scan(RuntimeState* state);
<<<<<<< HEAD:be/src/exec/pipeline/scan_operator.h
=======
    void _finish_chunk_source_task(RuntimeState* state, int chunk_source_index, int64_t cpu_time_ns, int64_t scan_rows,
                                   int64_t scan_bytes);
>>>>>>> 14983c7e1 ([Refactor] refactor and fix the scan counters (#8088)):be/src/exec/pipeline/scan/scan_operator.h
    void _merge_chunk_source_profiles();

protected:
    ScanNode* _scan_node = nullptr;
    // ScanOperator may do parallel scan, so each _chunk_sources[i] needs to hold
    // a profile indenpendently, to be more specificly, _chunk_sources[i] will go through
    // many ChunkSourcePtr in the entire life time, all these ChunkSources of _chunk_sources[i]
    // should share one profile because these ChunkSources are serial in timeline.
    // And all these parallel profiles will be merged to ScanOperator's profile at the end.
    std::vector<std::shared_ptr<RuntimeProfile>> _chunk_source_profiles;

private:
    inline void set_scan_status(const Status& status) {
        std::lock_guard<SpinLock> l(_scan_status_mutex);
        if (_scan_status.ok()) {
            _scan_status = status;
        }
    }

    inline Status get_scan_status() const {
        std::lock_guard<SpinLock> l(_scan_status_mutex);
        return _scan_status;
    }

    static constexpr int MAX_IO_TASKS_PER_OP = 4;

    const size_t _buffer_size = config::pipeline_io_buffer_size;

    bool _is_finished = false;

    int32_t _io_task_retry_cnt = 0;
    PriorityThreadPool* _io_threads = nullptr;
    std::atomic<int> _num_running_io_tasks = 0;
    std::vector<std::atomic<bool>> _is_io_task_running;
    std::vector<ChunkSourcePtr> _chunk_sources;
    mutable SpinLock _scan_status_mutex;
    Status _scan_status;
    // we should hold a weak ptr because query context may be released before running io task
    std::weak_ptr<QueryContext> _query_ctx;

    workgroup::WorkGroupPtr _workgroup = nullptr;
    std::atomic_int64_t _last_scan_rows_num = 0;
    std::atomic_int64_t _last_scan_bytes = 0;
};

class ScanOperatorFactory : public SourceOperatorFactory {
public:
    ScanOperatorFactory(int32_t id, ScanNode* scan_node);

    ~ScanOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    // interface for different scan node
    virtual Status do_prepare(RuntimeState* state) = 0;
    virtual void do_close(RuntimeState* state) = 0;
    virtual OperatorPtr do_create(int32_t dop, int32_t driver_sequence) = 0;

protected:
    ScanNode* _scan_node;
};

} // namespace pipeline
} // namespace starrocks
