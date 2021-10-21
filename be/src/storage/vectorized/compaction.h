// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <vector>

#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_id_generator.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta.h"
#include "storage/utils.h"
#include "util/semaphore.hpp"

namespace starrocks::vectorized {

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
    struct Statistics {
        // number of rows written to the destination rowset after merge
        int64_t output_rows = 0;
        int64_t merged_rows = 0;
        int64_t filtered_rows = 0;
    };

    explicit Compaction(MemTracker* mem_tracker, TabletSharedPtr tablet);
    virtual ~Compaction();

    Compaction(const Compaction&) = delete;
    Compaction& operator=(const Compaction&) = delete;

    virtual Status compact() = 0;

    static Status init(int concurreny);

protected:
    virtual Status pick_rowsets_to_compact() = 0;
    virtual std::string compaction_name() const = 0;
    virtual ReaderType compaction_type() const = 0;

    Status do_compaction();
    Status do_compaction_impl();

    // merge rows from vectorized reader and write into `_output_rs_writer`.
    // return Status::OK() and set statistics into `*stats_output`.
    // return others on error
    Status merge_rowsets(int64_t mem_limit, Statistics* stats_output);

    void modify_rowsets();

    Status construct_output_rowset_writer();

    Status check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    Status check_correctness(const Statistics& stats);

    // semaphore used to limit the concurrency of running compaction tasks
    static Semaphore _concurrency_sem;

protected:
    MemTracker* _mem_tracker = nullptr;
    TabletSharedPtr _tablet;

    std::vector<RowsetSharedPtr> _input_rowsets;
    int64_t _input_rowsets_size;
    int64_t _input_row_num;

    RowsetSharedPtr _output_rowset;
    std::unique_ptr<RowsetWriter> _output_rs_writer;

    enum CompactionState { INITED = 0, SUCCESS = 1 };
    CompactionState _state;

    Version _output_version;
    VersionHash _output_version_hash;

    RuntimeProfile _runtime_profile;
};

} // namespace starrocks::vectorized
