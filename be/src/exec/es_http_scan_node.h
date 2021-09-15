// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/es_http_scan_node.h

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

#ifndef BE_EXEC_ES_HTTP_SCAN_NODE_H
#define BE_EXEC_ES_HTTP_SCAN_NODE_H

#include <atomic>
#include <condition_variable>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "exec/convert_scan_node.h"
#include "exec/es_http_scanner.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {

class RuntimeState;
class PartRangeKey;
class PartitionInfo;
class EsHttpScanCounter;
class EsPredicate;

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

class EsHttpScanNode : public ConvertScanNode {
public:
    EsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~EsHttpScanNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    virtual Status close(RuntimeState* state) override;
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    // Update process status to one failed status,
    // NOTE: Must hold the mutex of this scan node
    bool update_status(const Status& new_status) {
        if (_process_status.ok()) {
            _process_status = new_status;
            return true;
        }
        return false;
    }

    // Create scanners to do scan job
    Status start_scanners();

    // Collect all scanners 's status
    Status collect_scanners_status();

    // One scanner worker, This scanner will hanle 'length' ranges start from start_idx
    void scanner_worker(int start_idx, int length, std::promise<Status>& p_status);

    // Scan one range
    Status scanner_scan(std::unique_ptr<EsHttpScanner> scanner, const std::vector<ExprContext*>& conjunct_ctxs,
                        EsScanCounter* counter);

    Status build_conjuncts_list();

    TupleId _tuple_id;
    RuntimeState* _runtime_state;
    TupleDescriptor* _tuple_desc;

    int _num_running_scanners;
    std::atomic<bool> _scan_finished;
    bool _eos;
    int _max_buffered_batches;
    RuntimeProfile::Counter* _wait_scanner_timer;

    Status _process_status;

    std::vector<std::thread> _scanner_threads;
    std::vector<std::promise<Status>> _scanners_status;
    std::map<std::string, std::string> _properties;
    std::map<std::string, std::string> _docvalue_context;
    std::map<std::string, std::string> _fields_context;
    std::vector<TScanRangeParams> _scan_ranges;
    std::vector<std::string> _column_names;

    std::mutex _batch_queue_lock;
    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;
    std::deque<std::shared_ptr<RowBatch>> _batch_queue;
    std::vector<EsPredicate*> _predicates;

    std::vector<int> _predicate_to_conjunct;
};

} // namespace starrocks

#endif
