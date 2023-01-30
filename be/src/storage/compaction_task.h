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

#pragma once

#include <mutex>
#include <sstream>
#include <vector>

#include "storage/background_task.h"
#include "storage/compaction_utils.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"
#include "util/runtime_profile.h"
#include "util/time.h"

namespace starrocks {

enum CompactionTaskState { COMPACTION_INIT, COMPACTION_RUNNING, COMPACTION_FAILED, COMPACTION_SUCCESS };

static const char* compaction_state_to_string(CompactionTaskState state) {
    switch (state) {
    case COMPACTION_INIT:
        return "COMPACTION_INIT";
    case COMPACTION_RUNNING:
        return "COMPACTION_RUNNING";
    case COMPACTION_FAILED:
        return "COMPACTION_FAILED";
    case COMPACTION_SUCCESS:
        return "COMPACTION_SUCCESS";
    default:
        return "UNKNOWN_STATE";
    }
}

struct CompactionTaskInfo {
    CompactionTaskInfo(CompactionAlgorithm algo) : algorithm(algo) {}
    CompactionAlgorithm algorithm;
    CompactionTaskState state{COMPACTION_INIT};
    uint64_t task_id{0};
    Version output_version;
    uint64_t elapsed_time{0};
    int64_t tablet_id{0};
    double compaction_score{0};
    int64_t start_time{0};
    int64_t end_time{0};
    size_t input_rows_num{0};
    uint32_t input_rowsets_num{0};
    size_t input_rowsets_size{0};
    size_t input_segments_num{0};
    uint32_t segment_iterator_num{0};
    uint32_t output_segments_num{0};
    uint32_t output_rowset_size{0};
    size_t merged_rows{0};
    size_t filtered_rows{0};
    size_t output_num_rows{0};
    CompactionType compaction_type{CompactionType::INVALID_COMPACTION};

    // for vertical compaction
    size_t column_group_size{0};
    size_t total_output_num_rows{0};
    size_t total_merged_rows{0};
    size_t total_del_filtered_rows{0};

    // return [0-100] to indicate progress
    int get_progress() const {
        if (input_rows_num == 0) {
            return 100;
        }
        if (algorithm == HORIZONTAL_COMPACTION) {
            return (output_num_rows + merged_rows + filtered_rows) * 100 / input_rows_num;
        } else {
            if (column_group_size == 0) {
                return 0;
            }
            return (total_output_num_rows + total_merged_rows + total_del_filtered_rows) * 100 /
                   (input_rows_num * column_group_size);
        }
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << "[CompactionTaskInfo]";
        ss << " task_id:" << task_id;
        ss << ", tablet_id:" << tablet_id;
        ss << ", compaction score:" << compaction_score;
        ss << ", algorithm:" << CompactionUtils::compaction_algorithm_to_string(algorithm);
        ss << ", state:" << compaction_state_to_string(state);
        ss << ", compaction_type:" << starrocks::to_string(compaction_type);
        ss << ", output_version:" << output_version;
        ss << ", start_time:" << ToStringFromUnixMillis(start_time);
        ss << ", end_time:" << ToStringFromUnixMillis(end_time);
        ss << ", elapsed_time:" << elapsed_time << " us";
        ss << ", input_rowsets_size:" << input_rowsets_size;
        ss << ", input_segments_num:" << input_segments_num;
        ss << ", input_rowsets_num:" << input_rowsets_num;
        ss << ", input_rows_num:" << input_rows_num;
        ss << ", output_num_rows:" << output_num_rows;
        ss << ", merged_rows:" << merged_rows;
        ss << ", filtered_rows:" << filtered_rows;
        ss << ", output_segments_num:" << output_segments_num;
        ss << ", output_rowset_size:" << output_rowset_size;
        ss << ", column_group_size:" << column_group_size;
        ss << ", total_output_num_rows:" << total_output_num_rows;
        ss << ", total_merged_rows:" << total_merged_rows;
        ss << ", total_del_filtered_rows:" << total_del_filtered_rows;
        ss << ", progress:" << get_progress();
        return ss.str();
    }
};

class CompactionTask : public BackgroundTask {
public:
    CompactionTask(CompactionAlgorithm algorithm) : _task_info(algorithm), _runtime_profile("compaction") {
        _watch.start();
    }
    ~CompactionTask() override;

    void run() override;

    bool should_stop() const override;

    void set_input_rowsets(std::vector<RowsetSharedPtr>&& input_rowsets) {
        _input_rowsets = input_rowsets;
        _task_info.input_rowsets_num = _input_rowsets.size();
    }

    void set_compaction_task_state(CompactionTaskState state) { _task_info.state = state; }

    CompactionTaskState compaction_task_state() { return _task_info.state; }

    bool is_compaction_finished() const {
        return _task_info.state == COMPACTION_FAILED || _task_info.state == COMPACTION_SUCCESS;
    }

    const std::vector<RowsetSharedPtr>& input_rowsets() const { return _input_rowsets; }

    void set_output_version(const Version& version) { _task_info.output_version = version; }

    const Version& output_version() const { return _task_info.output_version; }

    void set_input_rows_num(size_t input_rows_num) { _task_info.input_rows_num = input_rows_num; }

    size_t input_rows_num() const { return _task_info.input_rows_num; }

    void set_input_rowsets_size(size_t input_rowsets_size) { _task_info.input_rowsets_size = input_rowsets_size; }

    size_t input_rowsets_size() const { return _task_info.input_rowsets_size; }

    void set_compaction_type(CompactionType type) { _task_info.compaction_type = type; }

    CompactionType compaction_type() const { return _task_info.compaction_type; }

    void set_tablet(const TabletSharedPtr& tablet) {
        _tablet = tablet;
        _task_info.tablet_id = _tablet->tablet_id();
    }

    TabletSharedPtr& tablet() { return _tablet; }

    void set_task_id(uint64_t task_id) { _task_info.task_id = task_id; }

    uint64_t task_id() { return _task_info.task_id; }

    void set_compaction_score(double compaction_score) { _task_info.compaction_score = compaction_score; }

    double compaction_score() { return _task_info.compaction_score; }

    void set_segment_iterator_num(size_t segment_iterator_num) {
        _task_info.segment_iterator_num = segment_iterator_num;
    }

    size_t segment_iterator_num() { return _task_info.segment_iterator_num; }

    void set_input_segments_num(size_t input_segments_num) { _task_info.input_segments_num = input_segments_num; }

    void set_start_time(int64_t start_time) { _task_info.start_time = start_time; }

    void set_end_time(int64_t end_time) { _task_info.end_time = end_time; }

    void set_output_segments_num(uint32_t output_segments_num) { _task_info.output_segments_num = output_segments_num; }

    void set_output_rowset_size(uint32_t output_rowset_size) { _task_info.output_rowset_size = output_rowset_size; }

    void set_merged_rows(size_t merged_rows) { _task_info.merged_rows = merged_rows; }

    void set_filtered_rows(size_t filtered_rows) { _task_info.filtered_rows = filtered_rows; }

    void set_output_num_rows(size_t output_num_rows) { _task_info.output_num_rows = output_num_rows; }

    void set_mem_tracker(MemTracker* mem_tracker) { _mem_tracker = mem_tracker; }

    std::string get_task_info() {
        _task_info.elapsed_time = _watch.elapsed_time() / 1000;
        return _task_info.to_string();
    }

protected:
    virtual Status run_impl() = 0;

    void _try_lock() {
        if (_task_info.compaction_type == CUMULATIVE_COMPACTION) {
            _compaction_lock = std::unique_lock(_tablet->get_cumulative_lock(), std::try_to_lock);
        } else {
            _compaction_lock = std::unique_lock(_tablet->get_base_lock(), std::try_to_lock);
        }
    }

    Status _validate_compaction(const Statistics& stats) {
        // check row number
        DCHECK(_output_rowset) << "_output_rowset is null";
        VLOG(1) << "validate compaction, _input_rows_num:" << _task_info.input_rows_num
                << ", output rowset rows:" << _output_rowset->num_rows() << ", merged_rows:" << stats.merged_rows
                << ", filtered_rows:" << stats.filtered_rows;
        if (_task_info.input_rows_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
            LOG(WARNING) << "row_num does not match between cumulative input and output! "
                         << "input_row_num=" << _task_info.input_rows_num << ", merged_row_num=" << stats.merged_rows
                         << ", filtered_row_num=" << stats.filtered_rows
                         << ", output_row_num=" << _output_rowset->num_rows();

            return Status::InternalError("compaction check lines error.");
        }
        return Status::OK();
    }

    void _commit_compaction() {
        std::unique_lock wrlock(_tablet->get_header_lock());
        std::stringstream input_stream_info;
        for (int i = 0; i < 5 && i < _input_rowsets.size(); ++i) {
            input_stream_info << _input_rowsets[i]->version() << ";";
        }
        if (_input_rowsets.size() > 5) {
            input_stream_info << ".." << (*_input_rowsets.rbegin())->version();
        }
        _tablet->modify_rowsets({_output_rowset}, _input_rowsets);
        _tablet->save_meta();
        Rowset::close_rowsets(_input_rowsets);
        VLOG(1) << "commit compaction. output version:" << _task_info.output_version
                << ", output rowset version:" << _output_rowset->version()
                << ", input rowsets:" << input_stream_info.str() << ", input rowsets size:" << _input_rowsets.size()
                << ", max_version:" << _tablet->max_continuous_version();
    }

    void _success_callback();

    void _failure_callback(const Status& st);

protected:
    CompactionTaskInfo _task_info;
    RuntimeProfile _runtime_profile;
    std::vector<RowsetSharedPtr> _input_rowsets;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _output_rowset;
    std::unique_lock<std::mutex> _compaction_lock;
    MonotonicStopWatch _watch;
    MemTracker* _mem_tracker{nullptr};
};

} // namespace starrocks
