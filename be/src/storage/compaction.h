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

#include <vector>

#include "storage/compaction_utils.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta.h"
#include "storage/utils.h"
#include "util/runtime_profile.h"
#include "util/semaphore.hpp"

namespace starrocks {

class DataDir;

// This class is a base class for compaction.
// The entrance of this class is compact()
// Any compaction should go through four procedures.
//  1. pick rowsets satisfied to compact
//  2. do compaction
//  3. modify rowsets
//  4. gc unused rowstes
class Compaction {
public:
    explicit Compaction(MemTracker* mem_tracker, TabletSharedPtr tablet);
    virtual ~Compaction();

    Compaction(const Compaction&) = delete;
    Compaction& operator=(const Compaction&) = delete;

    virtual Status compact() = 0;

    static Status init(int concurrency);

protected:
    virtual Status pick_rowsets_to_compact() = 0;
    virtual std::string compaction_name() const = 0;
    virtual ReaderType compaction_type() const = 0;

    Status do_compaction();
    Status do_compaction_impl();

    Status modify_rowsets();

    Status check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    Status check_correctness(const Statistics& stats);

    // semaphore used to limit the concurrency of running compaction tasks
    static Semaphore _concurrency_sem;

    MemTracker* _mem_tracker = nullptr;
    TabletSharedPtr _tablet;
    // used for vertical compaction
    // the first group is key columns
    std::vector<std::vector<uint32_t>> _column_groups;

    std::vector<RowsetSharedPtr> _input_rowsets;
    int64_t _input_rowsets_size;
    int64_t _input_row_num;

    RowsetSharedPtr _output_rowset;
    std::unique_ptr<RowsetWriter> _output_rs_writer;

    enum CompactionState { INITED = 0, SUCCESS = 1 };
    CompactionState _state;

    Version _output_version;

    RuntimeProfile _runtime_profile;
    // for flat json used
    std::vector<std::unique_ptr<ColumnAccessPath>> _column_access_paths;

private:
    // merge rows from vectorized reader and write into `_output_rs_writer`.
    // return Status::OK() and set statistics into `*stats_output`.
    // return others on error
    Status _merge_rowsets_horizontally(size_t segment_iterator_num, Statistics* stats_output,
                                       const TabletSchemaCSPtr& tablet_schema);
    Status _merge_rowsets_vertically(size_t segment_iterator_num, Statistics* stats_output,
                                     const TabletSchemaCSPtr& tablet_schema);
};

} // namespace starrocks
