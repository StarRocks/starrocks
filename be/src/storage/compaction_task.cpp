// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "storage/compaction_task.h"

#include "runtime/current_thread.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_scheduler.h"
#include "storage/storage_engine.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

void CompactionTask::run() {
    LOG(INFO) << "start compaction. task_id:" << _task_info.task_id << ", tablet:" << _task_info.tablet_id
              << ", algorithm:" << CompactionUtils::compaction_algorithm_to_string(_task_info.algorithm)
              << ", compaction_type:" << _task_info.compaction_type
              << ", compaction_score:" << _task_info.compaction_score
              << ", output_version:" << _task_info.output_version << ", input rowsets size:" << _input_rowsets.size();
    _task_info.start_time = UnixMillis();
    scoped_refptr<Trace> trace(new Trace);
    SCOPED_CLEANUP({
        uint64_t time_s = _watch.elapsed_time() / 1e9;
        if ((compaction_type() == CUMULATIVE_COMPACTION && time_s > config::cumulative_compaction_trace_threshold) ||
            (compaction_type() == BASE_COMPACTION && time_s > config::base_compaction_trace_threshold)) {
            LOG(INFO) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    });
    ADOPT_TRACE(trace.get());
    TRACE("[Compaction] start to perform compaction. task_id:$0, tablet:$1, algorithm::$2, compaction_type:$3, "
          "compaction_score:$4",
          _task_info.task_id, _task_info.tablet_id,
          CompactionUtils::compaction_algorithm_to_string(_task_info.algorithm), _task_info.compaction_type,
          _task_info.compaction_score);
    std::stringstream ss;
    ss << "output version:" << _task_info.output_version << ", input versions size:" << _input_rowsets.size()
       << ", input versions:";

    for (int i = 0; i < 5 && i < _input_rowsets.size(); ++i) {
        ss << _input_rowsets[i]->version() << ";";
    }
    if (_input_rowsets.size() > 5) {
        ss << ".." << (*_input_rowsets.rbegin())->version();
    }
    TRACE("[Compaction] $0", ss.str());
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());
    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _task_info.input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _task_info.input_rows_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", _task_info.input_segments_num);
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);

    bool is_finished = false;
    DeferOp op([&] {
        TRACE("[Compaction] do compaction callback.");
        if (!is_finished) {
            set_compaction_task_state(COMPACTION_FAILED);
        }
        // reset compaction before judge need_compaction again
        // because if there is a compaction task for one compaction type in a tablet,
        // it will not be able to run another one for that type
        _tablet->reset_compaction(compaction_type());
        _task_info.end_time = UnixMillis();
        StorageEngine::instance()->compaction_manager()->unregister_task(this);
        // compaction context has been updated when commit
        // so do not update context here
        StorageEngine::instance()->compaction_manager()->update_tablet_async(_tablet, false, true);
        // must be put after unregister_task
        _scheduler->notify();
        TRACE("[Compaction] $0", _task_info.to_string());
    });
    if (should_stop()) {
        LOG(INFO) << "compaction task" << _task_info.task_id << " is stopped.";
        return;
    }

    set_compaction_task_state(COMPACTION_RUNNING);
    bool registered = StorageEngine::instance()->compaction_manager()->register_task(this);
    if (!registered) {
        LOG(WARNING) << "register compaction task failed. task_id:" << _task_info.task_id
                     << ", tablet:" << _task_info.tablet_id;
        return;
    }
    TRACE("[Compaction] compaction registered");

    _try_lock();
    if (!_compaction_lock.owns_lock()) {
        return;
    }
    TRACE("[Compaction] got compaction lock");

    Status status = run_impl();
    if (status.ok()) {
        _success_callback();
    } else {
        _failure_callback();
    }
    _watch.stop();
    _task_info.end_time = UnixMillis();
    // get elapsed_time in us
    _task_info.elapsed_time = _watch.elapsed_time() / 1000;
    is_finished = true;
    LOG(INFO) << "compaction finish. status:" << status.to_string() << ", task info:" << _task_info.to_string();
}

bool CompactionTask::should_stop() const {
    return StorageEngine::instance()->bg_worker_stopped() || !config::enable_compaction || BackgroudTask::should_stop();
}

void CompactionTask::_success_callback() {
    set_compaction_task_state(COMPACTION_SUCCESS);
    // for compatible, update compaction time
    if (_task_info.compaction_type == CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(UnixMillis());
    } else {
        _tablet->set_last_base_compaction_success_time(UnixMillis());
    }

    // for compatible
    if (_task_info.compaction_type == CUMULATIVE_COMPACTION) {
        StarRocksMetrics::instance()->cumulative_compaction_deltas_total.increment(_input_rowsets.size());
        StarRocksMetrics::instance()->cumulative_compaction_bytes_total.increment(_task_info.input_rowsets_size);
    } else {
        StarRocksMetrics::instance()->base_compaction_deltas_total.increment(_input_rowsets.size());
        StarRocksMetrics::instance()->base_compaction_bytes_total.increment(_task_info.input_rowsets_size);
    }

    // preload the rowset
    // warm-up this rowset
    auto st = _output_rowset->load();
    if (!st.ok()) {
        // only log load failure
        LOG(WARNING) << "ignore load rowset error tablet:" << _tablet->tablet_id()
                     << ", rowset:" << _output_rowset->rowset_id() << ", status:" << st;
    }
}

void CompactionTask::_failure_callback() {
    set_compaction_task_state(COMPACTION_FAILED);
    if (_task_info.compaction_type == CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_failure_time(UnixMillis());
    } else {
        _tablet->set_last_base_compaction_failure_time(UnixMillis());
    }
    LOG(WARNING) << "compaction task:" << _task_info.task_id << ", tablet:" << _task_info.tablet_id << " failed.";
}

} // namespace starrocks
