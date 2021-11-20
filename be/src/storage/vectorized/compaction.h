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

enum CompactionAlgorithm {
    // compaction by all columns together.
    HORIZONTAL = 0,
    // compaction by column group, for tablet with many columns.
    VERTICAL = 1
};

std::string compaction_algorithm_to_string(CompactionAlgorithm algorithm);

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

    // choose compaction algorithm according to tablet schema, max columns per group and segment iterator num.
    // 1. if the number of columns in the schema is less than or equal to max_columns_per_group, use HORIZONTAL.
    // 2. if source_num is less than or equal to 1, or is more than MAX_SOURCES, use HORIZONTAL.
    static CompactionAlgorithm choose_compaction_algorithm(size_t num_columns, int64_t max_columns_per_group,
                                                           size_t source_num);

    static uint32_t get_segment_max_rows(int64_t max_segment_file_size, int64_t input_row_num, int64_t input_size);

    static void get_column_groups(size_t num_columns, size_t num_key_columns, int64_t max_columns_per_group,
                                  std::vector<std::vector<uint32_t>>* column_groups);

protected:
    virtual Status pick_rowsets_to_compact() = 0;
    virtual std::string compaction_name() const = 0;
    virtual ReaderType compaction_type() const = 0;

    Status do_compaction();
    Status do_compaction_impl();

    // merge rows from vectorized reader and write into `_output_rs_writer`.
    // return Status::OK() and set statistics into `*stats_output`.
    // return others on error
    Status merge_rowsets_horizontal(int64_t mem_limit, Statistics* stats_output);
    Status merge_rowsets_vertical(Statistics* stats_output);

    Status modify_rowsets();

    Status construct_output_rowset_writer(uint32_t max_rows_per_segment, CompactionAlgorithm algorithm);

    Status check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets);
    Status check_correctness(const Statistics& stats);

    // semaphore used to limit the concurrency of running compaction tasks
    static Semaphore _concurrency_sem;

private:
    StatusOr<size_t> _get_segment_iterator_num();

protected:
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
};

} // namespace starrocks::vectorized
