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

} // namespace starrocks