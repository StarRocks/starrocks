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

#include <cstring>
#include <string>

#include "runtime/mem_pool.h"
#include "storage/base/short_key_index.h"
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
