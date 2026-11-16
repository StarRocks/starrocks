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

#include "exprs/table_function/json_each.h"

#include <algorithm>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exprs/table_function/table_function.h"
#include "runtime/runtime_state.h"
#include "velocypack/vpack.h"

namespace starrocks {

// Emits a bounded slice of the expansion per call: at most chunk_size (key, value) rows, cutting
// inside a row when one JSON value alone has more members than that.
//
// This used to declare the whole input chunk processed on its first line and materialize every
// member of every row in one go, with no intermediate memory-limit check. A JsonValue is a
// std::string of vpack bytes, so a K-member object becomes K key strings plus K value sub-trees each
// copied out on its own; for wide, shallow objects the per-member overhead dominates and the whole
// chunk's expansion lands in a single unchecked allocation.
//
// The cursor is the pair (processed_rows(), get_offset()) - the row being expanded and the index of
// the next member to emit from it - and the invariants tying it to TableFunctionOperator are the ones
// spelled out in multi_unnest.h. Slicing a row across calls is cheap because vpack::Slice addresses
// its members positionally: keyAt()/valueAt()/at() read the index table at the tail of the object, so
// resuming at member k costs no walk and needs no iterator cached across calls. (The compact
// encodings 0x13/0x14 have no index table and degrade to a walk, but they are only produced when a
// Builder is configured for them, and JsonValue builds with vpack::Options::Defaults.)
std::pair<Columns, UInt32Column::Ptr> JsonEach::process(RuntimeState* runtime_state, TableFunctionState* state) const {
    // At least one row per call, so the operator cannot spin forever on a result that carries no
    // bracket at all should chunk_size ever be configured as 0.
    const uint32_t chunk_size = std::max<uint32_t>(1, runtime_state->chunk_size());
    // Set once by the operator in prepare() and constant for the life of the state.
    const bool required = state->is_required();

    size_t num_input_rows = 0;
    JsonColumn* json_column = nullptr;
    if (!state->get_columns().empty()) {
        auto& arg0 = state->get_columns()[0];
        num_input_rows = arg0->size();
        json_column = down_cast<JsonColumn*>(ColumnHelper::get_data_column(arg0->as_mutable_raw_ptr()));
    }

    BinaryColumn::MutablePtr key_column_ptr = BinaryColumn::create();
    JsonColumn::MutablePtr value_column_ptr = JsonColumn::create();
    if (required) {
        key_column_ptr->reserve(chunk_size);
        value_column_ptr->reserve(chunk_size);
    }

    auto offset_column = UInt32Column::create();
    offset_column->reserve(chunk_size + 1);

    size_t cur_row = state->processed_rows();
    auto move_to_next_row = [&]() {
        cur_row++;
        state->set_processed_rows(cur_row);
        state->set_offset(0);
    };

    uint32_t emitted = 0;
    while (emitted < chunk_size && cur_row < num_input_rows) {
        // One bracket per input row touched by this batch: bracket k belongs to input row
        // (processed_rows() on entry) + k, which is how the operator picks the outer-column row.
        offset_column->append(emitted);

        const JsonValue* json = json_column->get_object(cur_row);
        DCHECK(!!json);
        const vpack::Slice json_slice = json->to_vslice();
        const bool is_object = json_slice.isObject();
        const bool is_array = !is_object && json_slice.isArray();
        if (!is_object && !is_array) {
            // Scalars, and the placeholder value a NULL row carries in the data column, expand to
            // nothing - a zero-length bracket, exactly as before.
            move_to_next_row();
            continue;
        }

        // length() covers both shapes; the Object-only objectLength() is private in this vpack build.
        const uint64_t members = json_slice.length();
        const auto cursor = static_cast<uint64_t>(state->get_offset());
        DCHECK_LE(cursor, members);
        const uint64_t slice_len = std::min<uint64_t>(members - cursor, chunk_size - emitted);

        // When the expanded values are not required the operator reads nothing but the bracket counts
        // (they drive outer-column replication), so no key has to be copied and no value sub-tree
        // extracted: how many rows this value expands to is its member count, which is O(1).
        if (required) {
            for (uint64_t k = cursor; k < cursor + slice_len; ++k) {
                if (is_object) {
                    const std::string_view key_str = json_slice.keyAt(k).stringView();
                    key_column_ptr->append(Slice(key_str.data(), key_str.size()));
                    value_column_ptr->append(JsonValue(json_slice.valueAt(k)));
                } else {
                    // The key of an array member is its index in the whole array, so it is the cursor
                    // that numbers them, not a counter local to this batch.
                    const std::string key = std::to_string(k);
                    key_column_ptr->append(Slice(key));
                    value_column_ptr->append(JsonValue(json_slice.at(k)));
                }
            }
        }

        emitted += static_cast<uint32_t>(slice_len);
        if (cursor + slice_len >= members) {
            // Closed here rather than by the next call finding the row exhausted, so a value whose
            // member count is an exact multiple of chunk_size does not cost an extra call and an extra
            // zero-length bracket.
            move_to_next_row();
        } else {
            // The row is only partially emitted, so processed_rows() stays put and this same row
            // becomes the first bracket of the next call.
            state->set_offset(static_cast<int64_t>(cursor + slice_len));
        }
    }
    offset_column->append(emitted);

    Columns result;
    result.reserve(2);
    result.emplace_back(std::move(key_column_ptr));
    result.emplace_back(std::move(value_column_ptr));
    return std::make_pair(std::move(result), std::move(offset_column));
}

} // namespace starrocks
