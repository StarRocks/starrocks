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

#include "column/schema.h"
#include "storage/base/short_key_index.h"
#include "storage_primitive/key_coder.h"
#include "storage_primitive/primary_key_encoder.h"
#include "types/datum.h"

namespace starrocks {

// SeekTuple represent the values of key columns, including NULL.
// The column id of columns specified in |_schema| must be continuously and started from zero.
class SeekTuple {
public:
    // default is infinite.
    SeekTuple() = default;

    SeekTuple(Schema schema, std::vector<Datum> values) : _schema(std::move(schema)), _values(std::move(values)) {
#ifndef NDEBUG
        if (_schema.sort_key_idxes().empty()) {
            // Ensure the key columns are continuous and started from zero.
            // But the value columns may not be continuous in delta column.
            for (size_t i = 0; i < _schema.num_key_fields(); i++) {
                CHECK_EQ(ColumnId(i), _schema.field(i)->id());
            }
        }
#endif
    }

    bool empty() const { return _values.empty(); }

    const Schema& schema() const { return _schema; }

    size_t columns() const { return _values.size(); }

    // Return the value of i-th column.
    const Datum& get(int i) const { return _values[i]; }

    // Encode the first Min(|num_short_keys|, |columns|) values for short key index lookup.
    // if |num_short_keys| is greater than |columns|, one additional char |padding| will be
    // appended at the tail.
    std::string short_key_encode(size_t num_short_keys, uint8_t padding) const;

    std::string short_key_encode(size_t num_short_keys, std::vector<uint32_t> short_key_idxes, uint8_t padding) const;

    // Encode the full, untruncated, order-preserving sort key for the first Min(|num_cols|, |columns|)
    // values, indexing |_values[0..num_cols-1]| directly. Used for query search keys.
    // Unlike short_key_encode, every column (including CHAR/VARCHAR/VARBINARY) is encoded at full length
    // via escaping rather than fixed-width truncation. CHAR values are escaped as-is (no NUL-truncation):
    // a query literal with an embedded NUL or over-width value stays correctly ordered against the
    // NUL-truncated stored form.
    // If |num_cols| is greater than |columns|, one additional char |padding| will be appended at the tail.
    std::string full_sort_key_encode(size_t num_cols, uint8_t padding) const;

    // Same as above, but indexes |_values[sort_key_idxes[i]]| for the i-th requested column. Used to
    // encode a physical (writer) row tuple, whose column order may differ from the sort key order.
    // CHAR values are NUL-truncated to their visible prefix before escaping, matching the canonical
    // stored/page-read form.
    std::string full_sort_key_encode(const std::vector<uint32_t>& sort_key_idxes, uint8_t padding) const;

    void convert_to(SeekTuple* new_tuple, const std::vector<LogicalType>& new_types) const;

    std::string debug_string() const;

private:
    // Shared helper for both full_sort_key_encode overloads. |num_requested| is the number of sort key
    // columns to encode; |column_id_at(i)| maps the i-th requested column to an index into |_values|.
    // |canonicalize_char| selects the CHAR NUL-truncation behavior: true for the physical overload
    // (writer / stored-row re-encode), false for the compact overload (query search key).
    template <class IndexAccessor>
    std::string _full_sort_key_encode(size_t num_requested, IndexAccessor&& column_id_at, uint8_t padding,
                                      bool canonicalize_char) const;

    Schema _schema;
    std::vector<Datum> _values;
};

inline std::string SeekTuple::short_key_encode(size_t num_short_keys, uint8_t padding) const {
    std::string output;
    size_t n = std::min(num_short_keys, _values.size());
    for (auto cid = 0; cid < n; cid++) {
        if (_values[cid].is_null()) {
            output.push_back(KEY_NULL_FIRST_MARKER);
        } else {
            output.push_back(KEY_NORMAL_MARKER);
            const auto& field = *_schema.field(cid);
            if (field.short_key_length() > 0) {
                const KeyCoder* coder = get_key_coder(field.type()->type());
                coder->encode_ascending(_values[cid], field.short_key_length(), &output);
            }
        }
    }
    if (_values.size() < num_short_keys) {
        output.push_back(padding);
    }
    return output;
}

inline std::string SeekTuple::short_key_encode(size_t num_short_keys, std::vector<uint32_t> sort_key_idxes,
                                               uint8_t padding) const {
    std::string output;
    DCHECK(num_short_keys <= sort_key_idxes.size())
            << "num short key: " << num_short_keys << " vs " << sort_key_idxes.size();
    size_t n = std::min(num_short_keys, _values.size());
    size_t val_num = _values.size();
    for (auto i = 0; i < n; i++) {
        if (sort_key_idxes[i] >= val_num || _values[sort_key_idxes[i]].is_null()) {
            output.push_back(KEY_NULL_FIRST_MARKER);
        } else {
            output.push_back(KEY_NORMAL_MARKER);
            const auto& field = *_schema.field(sort_key_idxes[i]);
            if (field.short_key_length() > 0) {
                const KeyCoder* coder = get_key_coder(field.type()->type());
                coder->encode_ascending(_values[sort_key_idxes[i]], field.short_key_length(), &output);
            }
        }
    }
    if (_values.size() < num_short_keys) {
        output.push_back(padding);
    }
    return output;
}

template <class IndexAccessor>
std::string SeekTuple::_full_sort_key_encode(size_t num_requested, IndexAccessor&& column_id_at, uint8_t padding,
                                             bool canonicalize_char) const {
    std::string output;
    size_t n = std::min(num_requested, _values.size());
    for (size_t i = 0; i < n; i++) {
        size_t cid = column_id_at(i);
        if (cid >= _values.size() || _values[cid].is_null()) {
            output.push_back(KEY_NULL_FIRST_MARKER);
            continue;
        }
        output.push_back(KEY_NORMAL_MARKER);
        const bool is_last = (i + 1 == num_requested);
        const auto& field = *_schema.field(cid);
        const LogicalType lt = field.type()->type();
        if (lt == TYPE_VARCHAR || lt == TYPE_VARBINARY) {
            // Variable-length: escape 0x00 -> 0x00 0x01 and terminate with 0x00 0x00 unless this is the
            // last requested column. Embedded NULs are significant, so the raw slice is never truncated.
            encoding_utils::encode_slice(_values[cid].get_slice(), &output, is_last);
        } else if (lt == TYPE_CHAR) {
            // CHAR is variable-length escaped, but NUL-truncation is asymmetric by overload: the physical
            // overload truncates to the visible prefix first, matching the canonical stored/page-read
            // form and stripping writer-side padding; the compact (query) overload escapes the raw slice
            // so an embedded-NUL or over-width query key stays correctly ordered against the truncated
            // stored entry.
            Slice s = _values[cid].get_slice();
            if (canonicalize_char) {
                size_t vis = 0;
                while (vis < s.size && s.data[vis] != '\0') {
                    vis++;
                }
                s = Slice(s.data, vis);
            }
            encoding_utils::encode_slice(s, &output, is_last);
        } else {
            const KeyCoder* coder = get_key_coder(lt);
            // The caller (SegmentWriter) must only enable the full sort key index when every
            // sort column is encodable (see full_sort_key_codec.h: is_full_sort_key_encodable).
            // Types with no registered coder (e.g. FLOAT/DOUBLE/JSON/complex) must never reach
            // here; fail loud in debug rather than null-deref in release.
            DCHECK(coder != nullptr) << "no key coder registered for type " << lt;
            coder->full_encode_ascending(_values[cid], &output);
        }
    }
    if (_values.size() < num_requested) {
        output.push_back(padding);
    }
    return output;
}

inline std::string SeekTuple::full_sort_key_encode(size_t num_cols, uint8_t padding) const {
    return _full_sort_key_encode(
            num_cols, [](size_t i) { return i; }, padding, /*canonicalize_char=*/false);
}

inline std::string SeekTuple::full_sort_key_encode(const std::vector<uint32_t>& sort_key_idxes, uint8_t padding) const {
    return _full_sort_key_encode(
            sort_key_idxes.size(), [&sort_key_idxes](size_t i) { return sort_key_idxes[i]; }, padding,
            /*canonicalize_char=*/true);
}

} // namespace starrocks
