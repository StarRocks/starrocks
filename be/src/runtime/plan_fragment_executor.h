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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/plan_fragment_executor.h

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

#include <condition_variable>
#include <functional>
#include <memory>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/mem_tracker.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/stream_load_executor.h"

namespace starrocks {

class ExecNode;
class RowDescriptor;
class DataSink;
class DataStreamMgr;
class RuntimeProfile;
class RuntimeState;
class TPlanFragment;
class TPlanFragmentExecParams;
class TPlanExecParams;

// PlanFragmentExecutor handles all aspects of the execution of a single plan fragment,
// including setup and tear-down, both in the success and error case.
// Tear-down frees all memory allocated for this plan fragment and closes all data
// streams; it happens automatically in the d'tor.
//
// The executor makes an aggregated profile for the entire fragment available,
// which includes profile information for the plan itself as well as the output
// sink, if any.
// The ReportStatusCallback passed into the c'tor is invoked periodically to report the
// execution status. The frequency of those reports is controlled by the flag
// status_report_interval; setting that flag to 0 disables periodic reporting altogether
// Regardless of the value of that flag, if a report callback is specified, it is
// invoked at least once at the end of execution with an overall status and profile
// (and 'done' indicator). The only exception is when execution is cancelled, in which
// case the callback is *not* invoked (the coordinator already knows that execution
// stopped, because it initiated the cancellation).
//
// Aside from Cancel(), which may be called asynchronously, this class is not
// thread-safe.
class PlanFragmentExecutor {
public:
    // Callback to report execution status of plan fragment.
    // 'profile' is the cumulative profile, 'done' indicates whether the execution
    // is done or still continuing.
    // Note: this does not take a const RuntimeProfile&, because it might need to call
    // functions like PrettyPrint() or to_thrift(), neither of which is const
    // because they take locks.
    typedef std::function<void(const Status& status, RuntimeProfile* profile, RuntimeProfile* load_channel_profile,
                               bool done)>
            report_status_callback;

    // if report_status_cb is not empty, is used to report the accumulated profile
    // information periodically during execution open().
    PlanFragmentExecutor(ExecEnv* exec_env, report_status_callback report_status_cb);

    // Closes the underlying plan fragment and frees up all resources allocated in open()
    // It is an error to delete a PlanFragmentExecutor with a report callback before open()
    // indicated that execution is finished.
    ~PlanFragmentExecutor();

    // prepare for execution. Call this prior to open().
    // This call won't block.
    // runtime_state() and row_desc() will not be valid until prepare() is called.
    // If request.query_options.mem_limit > 0, it is used as an approximate limit on the
    // number of bytes this query can consume at runtime.
    // The query will be aborted (MEM_LIMIT_EXCEEDED) if it goes over that limit.
    Status prepare(const TExecPlanFragmentParams& request);

    // Start execution.
    // If this fragment has a sink, open() will send all rows produced
    // by the fragment to that sink. Therefore, open() may block until
    // all rows are produced
    // This also starts the status-reporting thread, if the interval flag
    // is > 0 and a callback was specified in the c'tor.
    // If this fragment has a sink, report_status_cb will have been called for the final
    // time when open() returns, and the status-reporting thread will have been stopped.
    Status open();

    // Closes the underlying plan fragment and frees up all resources allocated in open()
    void close();

    // Initiate cancellation. Must not be called until after prepare() returned.
    void cancel();

    // call these only after prepare()
    RuntimeState* runtime_state() { return _runtime_state; }

    void set_runtime_state(RuntimeState* runtime_state) { _runtime_state = runtime_state; }

    const RowDescriptor& row_desc();

    // Profile information for plan and output sink.
    RuntimeProfile* profile();

    const Status& status() const { return _status; }

    DataSink* get_sink() { return _sink.get(); }

    void report_profile_once();

    void set_is_report_on_cancel(bool val) { _is_report_on_cancel = val; }

    bool is_done() { return _done; }

    Status status() {
        std::lock_guard<std::mutex> l(_status_lock);
        return _status;
    }

private:
    Status _prepare_stream_load_pipe(const TExecPlanFragmentParams& request);

    ExecEnv* _exec_env;        // not owned
    ExecNode* _plan = nullptr; // lives in _runtime_state->obj_pool()
    TUniqueId _query_id;
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;

    // profile reporting-related
    report_status_callback _report_status_cb;

    // true if _plan->_get_next_internal_vectorized() indicated that it's done
    bool _done;

    // true if prepare() returned OK
    bool _prepared;

    // true if close() has been called
    bool _closed;

    bool enable_profile;

    // If load_profile_collect_second is set and time cost of load is less than the value,
    // then profile will not be reported to FE even though enable_profile=true
    int32_t load_profile_collect_second = -1;

    // If this is set to false, and 'enable_profile' is false as well,
    // This executor will not report status to FE on being cancelled.
    bool _is_report_on_cancel;

    // Overall execution status. Either ok() or set to the first error status that
    // was encountered.
    Status _status;

    // Protects _status
    // lock ordering:
    // 1. _report_thread_lock
    // 2. _status_lock
    std::mutex _status_lock;

    std::mutex _cancel_lock;

    // note that RuntimeState should be constructed before and destructed after `_sink' and `_row_batch',
    // therefore we declare it before `_sink' and `_row_batch'
    RuntimeState* _runtime_state = nullptr;
    // Output sink for rows sent to this fragment. May not be set, in which case rows are
    // returned via get_next's row batch
    // Created in prepare (if required), owned by this object.
    std::unique_ptr<DataSink> _sink;

    ChunkPtr _chunk;

    // Number of rows returned by this fragment
    RuntimeProfile::Counter* _rows_produced_counter = nullptr;

    // It is shared with BufferControlBlock and will be called in two different
    // threads. But their calls are all at different time, there is no problem of
    // multithreaded access.
    std::shared_ptr<QueryStatistics> _query_statistics;
    bool _collect_query_statistics_with_every_batch;

    // If this is a runtime filter merge node for some query.
    bool _is_runtime_filter_merge_node;

    std::vector<StreamLoadContext*> _stream_load_contexts;
    bool _channel_stream_load = false;

    ObjectPool* obj_pool() { return _runtime_state->obj_pool(); }

    // Invoked the report callback if there is a report callback and the current
    // status isn't CANCELLED. Sets 'done' to true in the callback invocation if
    // done == true or we have an error status.
    void send_report(bool done);

    // If _status.ok(), sets _status to status.
    // If we're transitioning to an error status, stops report thread and
    // sends a final report.
    void update_status(const Status& status);

    Status _open_internal_vectorized();

    Status _get_next_internal_vectorized(ChunkPtr* chunk);

    const DescriptorTbl& desc_tbl() { return _runtime_state->desc_tbl(); }

    void collect_query_statistics();
};

} // namespace starrocks
