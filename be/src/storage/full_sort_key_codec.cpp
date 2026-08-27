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

#include "storage/full_sort_key_codec.h"

#include <algorithm>
#include <cstring>
#include <string>

#include "common/config_rowset_fwd.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_pool.h"
#include "storage/base/short_key_index.h"
#include "storage/non_retryable_load_errors.h"
#include "storage_primitive/key_coder.h"
#include "types/datum.h"
#include "types/storage_type_traits.h"

namespace starrocks {

namespace {

// Reverses encoding_utils::encode_slice (be/src/storage_primitive/primary_key_encoder.cpp): un-escape
// 0x00 0x01 -> 0x00 and stop at the 0x00 0x00 terminator, unless |is_last| (in which case the remainder
// of |src| is the raw, un-terminated value). Mirrors the local free function `decode_slice` defined at
// primary_key_encoder.cpp:174-213, which isn't declared in that module's header and so isn't linkable
// from here.
Status unescape_slice_column(Slice* src, bool is_last, std::string* dest) {
    if (is_last) {
        dest->append(src->data, src->size);
        src->remove_prefix(src->size);
        return Status::OK();
    }
    auto* separator = static_cast<uint8_t*>(memmem(src->data, src->size, "\0\0", 2));
    if (separator == nullptr) {
        return Status::InvalidArgument("full sort key: separator not found in encoded string column");
    }
    auto* data = reinterpret_cast<uint8_t*>(src->data);
    size_t len = separator - data;
    dest->reserve(len);
    for (size_t i = 0; i < len; i++) {
        if (i >= 1 && data[i - 1] == '\0' && data[i] == '\1') {
            continue;
        }
        dest->push_back(static_cast<char>(data[i]));
    }
    src->remove_prefix(len + 2);
    return Status::OK();
}

// Decode a fixed-size, non-string key column: KeyCoder::decode_ascending fills a local |CppType| buffer
// (the same on-disk representation used by KeyCoderTraits<LT>::full_encode_ascending_datum), which is
// then written into |out| via Datum::set. This works uniformly for every fixed key-eligible type
// (including the DATE/DATETIME/legacy-DECIMAL/DECIMALV2 "wrapper" types) because Datum stores exactly
// this on-disk CppType internally, with no separate logical representation to reconstruct.
template <LogicalType LT>
Status decode_fixed_datum(Slice* s, MemPool* pool, Datum* out) {
    using CppType = StorageCppType<LT>;
    CppType raw{};
    RETURN_IF_ERROR(
            get_key_coder(LT)->decode_ascending(s, StorageCppTypeSize<LT>, reinterpret_cast<uint8_t*>(&raw), pool));
    out->set<CppType>(raw);
    return Status::OK();
}

Status decode_fixed_column(LogicalType lt, Slice* s, MemPool* pool, Datum* out) {
    switch (lt) {
#define M(TYPE) \
    case TYPE:  \
        return decode_fixed_datum<TYPE>(s, pool, out);
        M(TYPE_BOOLEAN)
        M(TYPE_TINYINT)
        M(TYPE_SMALLINT)
        M(TYPE_INT)
        M(TYPE_UNSIGNED_INT)
        M(TYPE_BIGINT)
        M(TYPE_UNSIGNED_BIGINT)
        M(TYPE_LARGEINT)
        M(TYPE_INT256)
        M(TYPE_DATE_V1)
        M(TYPE_DATE)
        M(TYPE_DATETIME_V1)
        M(TYPE_DATETIME)
        M(TYPE_DECIMAL)
        M(TYPE_DECIMALV2)
        M(TYPE_DECIMAL32)
        M(TYPE_DECIMAL64)
        M(TYPE_DECIMAL128)
        M(TYPE_DECIMAL256)
#undef M
    default:
        return Status::NotSupported("full sort key: unsupported key column type " + type_to_string(lt));
    }
}

} // namespace

Status decode_full_sort_key(const Slice& encoded, const Schema& schema, const std::vector<uint32_t>& sort_key_idxes,
                            VariantTuple* out) {
    out->clear();
    out->reserve(sort_key_idxes.size());

    Slice s = encoded;
    MemPool pool;
    size_t n = sort_key_idxes.size();
    for (size_t i = 0; i < n; i++) {
        uint32_t cid = sort_key_idxes[i];
        if (s.size == 0 || cid >= schema.num_fields()) {
            return Status::InvalidArgument("full sort key: malformed encoded buffer or column index");
        }
        auto marker = static_cast<uint8_t>(s.data[0]);
        s.remove_prefix(1);
        const FieldPtr& field = schema.field(cid);

        if (marker == KEY_NULL_FIRST_MARKER) {
            Datum null_datum;
            null_datum.set_null();
            out->emplace(field->type(), null_datum);
            continue;
        }
        if (marker != KEY_NORMAL_MARKER) {
            return Status::InvalidArgument("full sort key: bad marker byte in encoded buffer");
        }

        const bool is_last = (i + 1 == n);
        const LogicalType lt = field->type()->type();
        Datum value;
        if (lt == TYPE_VARCHAR || lt == TYPE_VARBINARY || lt == TYPE_CHAR) {
            std::string tmp;
            RETURN_IF_ERROR(unescape_slice_column(&s, is_last, &tmp));
            auto* buf = reinterpret_cast<char*>(pool.allocate(tmp.size()));
            if (!tmp.empty()) {
                RETURN_IF_UNLIKELY_NULL(buf, Status::MemoryAllocFailed("alloc mem for full sort key decoder failed"));
                memcpy(buf, tmp.data(), tmp.size());
            }
            value.set_slice(Slice(buf, tmp.size()));
        } else {
            RETURN_IF_ERROR(decode_fixed_column(lt, &s, &pool, &value));
        }
        out->emplace(field->type(), value);
    }
    return Status::OK();
}

namespace {

// Per-column encoded size for one row, mirroring SeekTuple::_full_sort_key_encode:
//   null                     -> 1 (marker only)
//   VARCHAR/VARBINARY last   -> 1 + len            (encode_slice appends the slice directly)
//   VARCHAR/VARBINARY other  -> 1 + len + NULs + 2 (0x00 -> 0x00 0x01, plus a 0x00 0x00 terminator)
//   CHAR                     -> as above, over the NUL-truncated visible prefix, which has no NULs
//   fixed                    -> 1 + coder->full_encode_size()
struct SortKeyColumnPlan {
    const Column* column = nullptr;
    LogicalType type = TYPE_UNKNOWN;
    size_t fixed_size = 0; // whole 1 + width contribution, for fixed-width columns
    bool is_variable = false;
    bool is_last = false;
    bool nullable = false;
};

size_t variable_column_size(const SortKeyColumnPlan& plan, size_t row) {
    Slice s = plan.column->get(row).get_slice();
    if (plan.type == TYPE_CHAR) {
        // The physical encoder truncates CHAR at the first NUL, stripping writer-side padding.
        size_t visible = 0;
        while (visible < s.size && s.data[visible] != '\0') {
            visible++;
        }
        s = Slice(s.data, visible);
    }
    if (plan.is_last) {
        return 1 + s.size;
    }
    size_t nuls = std::count(s.data, s.data + s.size, '\0');
    return 1 + s.size + nuls + 2;
}

size_t row_encoded_size(const std::vector<SortKeyColumnPlan>& plans, size_t row) {
    size_t total = 0;
    for (const auto& plan : plans) {
        if (plan.nullable && plan.column->is_null(row)) {
            total += 1;
        } else if (plan.is_variable) {
            total += variable_column_size(plan, row);
        } else {
            total += plan.fixed_size;
        }
    }
    return total;
}

// If every requested column is fixed-width and non-nullable, every row encodes to the same size, so
// the whole range can be decided without visiting rows. A nullable column breaks this: a NULL
// contributes only its marker byte, so sizes vary by row.
bool constant_row_size(const std::vector<SortKeyColumnPlan>& plans, size_t* size) {
    size_t total = 0;
    for (const auto& plan : plans) {
        if (plan.is_variable || plan.nullable) {
            return false;
        }
        total += plan.fixed_size;
    }
    *size = total;
    return true;
}

// Returns false if no plan can be built -- a bad column index or a type with no registered coder.
// Callers treat that as "nothing to measure", consistent with is_full_sort_key_encodable().
bool build_plans(const Schema& schema, const std::vector<ColumnId>& sort_key_idxes, const Chunk& chunk,
                 std::vector<SortKeyColumnPlan>* out) {
    size_t n = sort_key_idxes.size();
    out->reserve(n);
    for (size_t i = 0; i < n; ++i) {
        ColumnId cid = sort_key_idxes[i];
        if (cid >= schema.num_fields() || cid >= chunk.num_columns()) {
            return false;
        }
        SortKeyColumnPlan plan;
        plan.column = chunk.get_column_by_index(cid).get();
        plan.type = schema.field(cid)->type()->type();
        plan.is_last = (i + 1 == n);
        plan.nullable = plan.column->is_nullable();
        if (plan.type == TYPE_VARCHAR || plan.type == TYPE_VARBINARY || plan.type == TYPE_CHAR) {
            plan.is_variable = true;
        } else {
            const KeyCoder* coder = get_key_coder(plan.type);
            if (coder == nullptr) {
                return false;
            }
            plan.fixed_size = 1 + coder->full_encode_size();
        }
        out->emplace_back(plan);
    }
    return true;
}

} // namespace

bool full_sort_key_exceed_limit(const Schema& schema, const std::vector<ColumnId>& sort_key_idxes, const Chunk& chunk,
                                size_t offset, size_t len, size_t limit) {
    std::vector<SortKeyColumnPlan> plans;
    if (sort_key_idxes.empty() || len == 0 || !build_plans(schema, sort_key_idxes, chunk, &plans)) {
        return false;
    }
    size_t constant_size = 0;
    if (constant_row_size(plans, &constant_size)) {
        return constant_size > limit;
    }
    for (size_t r = offset; r < offset + len; ++r) {
        if (row_encoded_size(plans, r) > limit) {
            return true;
        }
    }
    return false;
}

namespace {

Status sort_key_size_exceeded(int32_t limit) {
    return Status::Cancelled(
            strings::Substitute("$0 limit: $1 (BE config sort_key_limit_size)", kSortKeySizeExceedError, limit));
}

// Gate shared by check_sort_key_size. Returns false when there is nothing to check;
// otherwise sets |limit| to the positive configured limit.
bool sort_key_check_enabled(const Schema& schema, const std::vector<ColumnId>& sort_key_idxes, int32_t* limit) {
    if (sort_key_idxes.empty()) {
        return false;
    }
    // Read the mutable config once, so the comparison and the message cannot disagree and a negative
    // value cannot be widened into a huge size_t.
    *limit = config::sort_key_limit_size;
    if (*limit <= 0) {
        return false;
    }
    return is_full_sort_key_encodable(schema, sort_key_idxes);
}

} // namespace

Status check_sort_key_size(const Schema& schema, const std::vector<ColumnId>& sort_key_idxes, const Chunk& chunk,
                           size_t offset, size_t len) {
    int32_t limit = 0;
    if (!sort_key_check_enabled(schema, sort_key_idxes, &limit)) {
        return Status::OK();
    }
    if (full_sort_key_exceed_limit(schema, sort_key_idxes, chunk, offset, len, static_cast<size_t>(limit))) {
        return sort_key_size_exceeded(limit);
    }
    return Status::OK();
}

bool is_full_sort_key_encodable(const Schema& schema, const std::vector<uint32_t>& sort_key_idxes) {
    for (uint32_t cid : sort_key_idxes) {
        if (cid >= schema.num_fields()) {
            return false;
        }
        const LogicalType lt = schema.field(cid)->type()->type();
        if (lt == TYPE_VARCHAR || lt == TYPE_VARBINARY || lt == TYPE_CHAR) {
            continue;
        }
        if (get_key_coder(lt) == nullptr) {
            return false;
        }
    }
    return true;
}

} // namespace starrocks
