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

#include "storage/rowset/fill_subfield_iterator.h"

namespace starrocks {

Status FillSubfieldIterator::next_batch(size_t* n, Column* dst) {
    return _column_iter->next_batch(n, dst, _predicate_path);
}

Status FillSubfieldIterator::next_batch(const SparseRange<>& range, Column* dst) {
    return _column_iter->next_batch(range, dst, _predicate_path);
}

Status FillSubfieldIterator::seek_to_first() {
    return _column_iter->seek_to_first();
}

Status FillSubfieldIterator::seek_to_ordinal(ordinal_t ord) {
    return _column_iter->seek_to_ordinal(ord);
}

ordinal_t FillSubfieldIterator::get_current_ordinal() const {
    return _column_iter->get_current_ordinal();
}

Status FillSubfieldIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    return _column_iter->fetch_subfield_by_rowid(rowids, size, values);
}

Status FillSubfieldIterator::fetch_values_by_rowid_for_predicate_evaluate(const Column& rowids, Column* values) {
    // Two callers reach this iterator by rowid and want opposite things of it.
    // _finish_late_materialization passes a column that already holds its rows and only needs the
    // subfields nobody has read yet, which is what fetch_values_by_rowid delegates to
    // fetch_subfield_by_rowid for. Predicate evaluation instead empties the column first and needs the
    // values themselves: routing it through fetch_subfield_by_rowid leaves an already-materialized
    // leaf field untouched -- a scalar iterator inherits a fetch_subfield_by_rowid that does nothing --
    // and the caller rejects the short column with "col size not equal to ordinal col size".
    return _column_iter->fetch_values_by_rowid(rowids, values);
}

ordinal_t FillSubfieldIterator::num_rows() const {
    return _column_iter->num_rows();
}

} // namespace starrocks