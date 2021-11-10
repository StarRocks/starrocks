// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/row_batch.h

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

#ifndef STARROCKS_BE_RUNTIME_ROW_BATCH_H
#define STARROCKS_BE_RUNTIME_ROW_BATCH_H

#include <cstring>
#include <vector>

#include "codegen/starrocks_ir.h"
#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch_interface.hpp"

namespace starrocks {

class TRowBatch;
class Tuple;
class TupleRow;
class TupleDescriptor;
class PRowBatch;

// A RowBatch encapsulates a batch of rows, each composed of a number of tuples.
// The maximum number of rows is fixed at the time of construction, and the caller
// can add rows up to that capacity.
// The row batch reference a few different sources of memory.
//   1. TupleRow ptrs - this is always owned and managed by the row batch.
//   2. Tuple memory - this is allocated (or transferred to) the row batches tuple pool.
//   3. Auxillary tuple memory (e.g. string data) - this can either be stored externally
//      (don't copy strings) or from the tuple pool (strings are copied).  If external,
//      the data is in an io buffer that may not be attached to this row batch.  The
//      creator of that row batch has to make sure that the io buffer is not recycled
//      until all batches that reference the memory have been consumed.
// In order to minimize memory allocations, RowBatches and TRowBatches that have been
// serialized and sent over the wire should be reused (this prevents _compression_scratch
// from being needlessly reallocated).
//
// Row batches and memory usage: We attempt to stream row batches through the plan
// tree without copying the data. This means that row batches are often not-compact
// and reference memory outside of the row batch. This results in most row batches
// having a very small memory footprint and in some row batches having a very large
// one (it contains all the memory that other row batches are referencing). An example
// is IoBuffers which are only attached to one row batch. Only when the row batch reaches
// a blocking operator or the root of the fragment is the row batch memory freed.
// This means that in some cases (e.g. very selective queries), we still need to
// pass the row batch through the exec nodes (even if they have no rows) to trigger
// memory deletion. at_capacity() encapsulates the check that we are not accumulating
// excessive memory.
//
// A row batch is considered at capacity if all the rows are full or it has accumulated
// auxiliary memory up to a soft cap. (See _at_capacity_mem_usage comment).
// TODO: stick _tuple_ptrs into a pool?
class RowBatch : public RowBatchInterface {
public:
    // Create RowBatch for a maximum of 'capacity' rows of tuples specified
    // by 'row_desc'.
    RowBatch(const RowDescriptor& row_desc, int capacity);
    Status init();

    // Populate a row batch from input_batch by copying input_batch's
    // tuple_data into the row batch's mempool and converting all offsets
    // in the data back into pointers.
    // TODO: figure out how to transfer the data from input_batch to this RowBatch
    // (so that we don't need to make yet another copy)
    RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch);

    RowBatch(const RowDescriptor& row_desc, const PRowBatch& input_batch);
    Status init(const PRowBatch& input_batch);

    // Releases all resources accumulated at this row batch.  This includes
    //  - tuple_ptrs
    //  - tuple mem pool data
    //  - buffer handles from the io mgr
    ~RowBatch() override;

    // used to c
    void clear();

    static const int INVALID_ROW_INDEX = -1;

    // Add n rows of tuple pointers after the last committed row and return its index.
    // The rows are uninitialized and each tuple of the row must be set.
    // Returns INVALID_ROW_INDEX if the row batch cannot fit n rows.
    // Two consecutive add_row() calls without a commit_last_row() between them
    // have the same effect as a single call.
    int add_rows(int n) {
        if (_num_rows + n > _capacity) {
            return INVALID_ROW_INDEX;
        }

        _has_in_flight_row = true;
        return _num_rows;
    }

    int add_row() { return add_rows(1); }

    void commit_rows(int n) {
        DCHECK_GE(n, 0);
        DCHECK_LE(_num_rows + n, _capacity);
        _num_rows += n;
        _has_in_flight_row = false;
    }

    void commit_last_row() { commit_rows(1); }

    // Set function can be used to reduce the number of rows in the batch.  This is only
    // used in the limit case where more rows were added than necessary.
    void set_num_rows(int num_rows) {
        DCHECK_LE(num_rows, _num_rows);
        _num_rows = num_rows;
    }

    // Returns true if row_batch has reached capacity.
    bool is_full() { return _num_rows == _capacity; }

    // The total size of all data represented in this row batch (tuples and referenced
    // string data).
    int total_byte_size();

    TupleRow* get_row(int row_idx) const {
        DCHECK(_tuple_ptrs != nullptr);
        DCHECK_GE(row_idx, 0);
        //DCHECK_LT(row_idx, _num_rows + (_has_in_flight_row ? 1 : 0));
        return reinterpret_cast<TupleRow*>(_tuple_ptrs + row_idx * _num_tuples_per_row);
    }

    /// An iterator for going through a row batch, starting at 'row_idx'.
    /// If 'limit' is specified, it will iterate up to row number 'row_idx + limit'
    /// or the last row, whichever comes first. Otherwise, it will iterate till the last
    /// row in the batch. This is more efficient than using GetRow() as it avoids loading
    /// the row batch state and doing multiplication on each loop with GetRow().
    class Iterator {
    public:
        Iterator(RowBatch* parent, int row_idx, int limit = -1)
                : _num_tuples_per_row(parent->_num_tuples_per_row),
                  _row(parent->_tuple_ptrs + _num_tuples_per_row * row_idx),
                  _row_batch_end(parent->_tuple_ptrs +
                                 _num_tuples_per_row * (limit == -1
                                                                ? parent->_num_rows
                                                                : std::min<int>(row_idx + limit, parent->_num_rows))),
                  _parent(parent) {
            DCHECK_GE(row_idx, 0);
            DCHECK_GT(_num_tuples_per_row, 0);
            /// We allow empty row batches with _num_rows == capacity_ == 0.
            /// That's why we cannot call GetRow() above to initialize '_row'.
            DCHECK_LE(row_idx, parent->_capacity);
        }

        /// Return the current row pointed to by the row pointer.
        TupleRow* IR_ALWAYS_INLINE get() { return reinterpret_cast<TupleRow*>(_row); }

        /// Increment the row pointer and return the next row.
        TupleRow* IR_ALWAYS_INLINE next() {
            _row += _num_tuples_per_row;
            DCHECK_LE((_row - _parent->_tuple_ptrs) / _num_tuples_per_row, _parent->_capacity);
            return get();
        }

        /// Returns the row batch which this iterator is iterating through.
        RowBatch* parent() { return _parent; }

    private:
        /// Number of tuples per row.
        const int _num_tuples_per_row;

        /// Pointer to the current row.
        Tuple** _row;

        /// Pointer to the row after the last row for read iterators.
        Tuple** const _row_batch_end;

        /// The row batch being iterated on.
        RowBatch* const _parent;
    };

    MemPool* tuple_data_pool() { return _tuple_data_pool.get(); }

    // Resets the row batch, returning all resources it has accumulated.
    void reset();

    void copy_row(TupleRow* src, TupleRow* dest) { memcpy(dest, src, _num_tuples_per_row * sizeof(Tuple*)); }

    // Create a serialized version of this row batch in output_batch, attaching all of the
    // data it references to output_batch.tuple_data. output_batch.tuple_data will be
    // snappy-compressed unless the compressed data is larger than the uncompressed
    // data. Use output_batch.is_compressed to determine whether tuple_data is compressed.
    // If an in-flight row is present in this row batch, it is ignored.
    // This function does not reset().
    // Returns the uncompressed serialized size (this will be the true size of output_batch
    // if tuple_data is actually uncompressed).
    int serialize(TRowBatch* output_batch);
    int serialize(PRowBatch* output_batch);

    // Utility function: returns total size of batch.
    static int get_batch_size(const TRowBatch& batch);
    static int get_batch_size(const PRowBatch& batch);

    int num_rows() const { return _num_rows; }
    int capacity() const { return _capacity; }

    const RowDescriptor& row_desc() const { return _row_desc; }

    std::string to_string();

private:
    bool _has_in_flight_row; // if true, last row hasn't been committed yet
    int _num_rows;           // # of committed rows
    int _capacity;           // maximum # of rows

    int _num_tuples_per_row;
    RowDescriptor _row_desc;

    // Array of pointers with _capacity * _num_tuples_per_row elements.
    // The memory ownership depends on whether legacy joins and aggs are enabled.
    //
    // Memory is malloc'd and owned by RowBatch:
    // If enable_partitioned_hash_join=true and enable_partitioned_aggregation=true
    // then the memory is owned by this RowBatch and is freed upon its destruction.
    // This mode is more performant especially with SubplanNodes in the ExecNode tree
    // because the tuple pointers are not transferred and do not have to be re-created
    // in every Reset().
    //
    // Memory is allocated from MemPool:
    // Otherwise, the memory is allocated from _tuple_data_pool. As a result, the
    // pointer memory is transferred just like tuple data, and must be re-created
    // in Reset(). This mode is required for the legacy join and agg which rely on
    // the tuple pointers being allocated from the _tuple_data_pool, so they can
    // acquire ownership of the tuple pointers.
    Tuple** _tuple_ptrs;
    int _tuple_ptrs_size;

    // If true, this batch is considered at capacity. This is explicitly set by streaming
    // components that return rows via row batches.
    bool _need_to_return;

    // holding (some of the) data referenced by rows
    std::unique_ptr<MemPool> _tuple_data_pool;

    // String to write compressed tuple data to in serialize().
    // This is a string so we can swap() with the string in the TRowBatch we're serializing
    // to (we don't compress directly into the TRowBatch in case the compressed data is
    // longer than the uncompressed data). Swapping avoids copying data to the TRowBatch and
    // avoids excess memory allocations: since we reuse RowBatchs and TRowBatchs, and
    // assuming all row batches are roughly the same size, all strings will eventually be
    // allocated to the right size.
    std::string _compression_scratch;

    bool _cleared = false;
};

} // namespace starrocks

#endif
