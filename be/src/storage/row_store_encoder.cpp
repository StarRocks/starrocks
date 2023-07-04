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

#include "storage/row_store_encoder.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "gutil/endian.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"
#include "types/date_value.hpp"

using std::string;
using std::vector;

namespace starrocks {

template <class UT>
UT _to_bigendian(UT v);

template <>
uint8_t _to_bigendian(uint8_t v) {
    return v;
}
template <>
uint16_t _to_bigendian(uint16_t v) {
    return BigEndian::FromHost16(v);
}
template <>
uint32_t _to_bigendian(uint32_t v) {
    return BigEndian::FromHost32(v);
}
template <>
uint64_t _to_bigendian(uint64_t v) {
    return BigEndian::FromHost64(v);
}
template <>
uint128_t _to_bigendian(uint128_t v) {
    return BigEndian::FromHost128(v);
}

template <class T>
void encode_integral(const T& v, string* dest) {
    if constexpr (std::is_signed<T>::value) {
        typedef typename std::make_unsigned<T>::type UT;
        UT uv = v;
        uv ^= static_cast<UT>(1) << (sizeof(UT) * 8 - 1);
        uv = _to_bigendian(uv);
        dest->append(reinterpret_cast<const char*>(&uv), sizeof(uv));
    } else {
        T nv = _to_bigendian(v);
        dest->append(reinterpret_cast<const char*>(&nv), sizeof(nv));
    }
}

template <class T>
void decode_integral(Slice* src, T* v) {
    if constexpr (std::is_signed<T>::value) {
        typedef typename std::make_unsigned<T>::type UT;
        UT uv = *(UT*)(src->data);
        uv = _to_bigendian(uv);
        uv ^= static_cast<UT>(1) << (sizeof(UT) * 8 - 1);
        *v = uv;
    } else {
        T nv = *(T*)(src->data);
        *v = _to_bigendian(nv);
    }
    src->remove_prefix(sizeof(T));
}

template <int LEN>
static bool SSEEncodeChunk(const uint8_t** srcp, uint8_t** dstp) {
#if defined(__aarch64__) || !defined(__SSE4_2__)
    return false;
#else
    __m128i data;
    if (LEN == 16) {
        // Load 16 bytes (unaligned) into the XMM register.
        data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(*srcp));
    } else if (LEN == 8) {
        // Load 8 bytes (unaligned) into the XMM register
        data = reinterpret_cast<__m128i>(_mm_load_sd(reinterpret_cast<const double*>(*srcp)));
    }
    // Compare each byte of the input with '\0'. This results in a vector
    // where each byte is either \x00 or \xFF, depending on whether the
    // input had a '\x00' in the corresponding position.
    __m128i zeros = reinterpret_cast<__m128i>(_mm_setzero_pd());
    __m128i zero_bytes = _mm_cmpeq_epi8(data, zeros);

    // Check whether the resulting vector is all-zero.
    bool all_zeros;
    if (LEN == 16) {
        all_zeros = _mm_testz_si128(zero_bytes, zero_bytes);
    } else { // LEN == 8
        all_zeros = _mm_cvtsi128_si64(zero_bytes) == 0;
    }

    // If it's all zero, we can just store the entire chunk.
    if (PREDICT_FALSE(!all_zeros)) {
        return false;
    }

    if (LEN == 16) {
        _mm_storeu_si128(reinterpret_cast<__m128i*>(*dstp), data);
    } else {
        _mm_storel_epi64(reinterpret_cast<__m128i*>(*dstp), data); // movq m64, xmm
    }
    *dstp += LEN;
    *srcp += LEN;
    return true;
#endif //__aarch64__
}

// Non-SSE loop which encodes 'len' bytes from 'srcp' into 'dst'.
static inline void EncodeChunkLoop(const uint8_t** srcp, uint8_t** dstp, int len) {
    while (len--) {
        if (PREDICT_FALSE(**srcp == '\0')) {
            *(*dstp)++ = 0;
            *(*dstp)++ = 1;
        } else {
            *(*dstp)++ = **srcp;
        }
        (*srcp)++;
    }
}

inline void encode_slice(const Slice& s, string* dst, bool is_last) {
    if (is_last) {
        dst->append(s.data, s.size);
    } else {
        // If we're a middle component of a composite key, we need to add a \x00
        // at the end in order to separate this component from the next one. However,
        // if we just did that, we'd have issues where a key that actually has
        // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
        // encode \x00 as \x00\x01.
        size_t old_size = dst->size();
        dst->resize(old_size + s.size * 2 + 2);

        const uint8_t* srcp = (const uint8_t*)s.data;
        uint8_t* dstp = reinterpret_cast<uint8_t*>(&(*dst)[old_size]);
        size_t len = s.size;
        size_t rem = len;

        while (rem >= 16) {
            if (!SSEEncodeChunk<16>(&srcp, &dstp)) {
                goto slow_path;
            }
            rem -= 16;
        }
        while (rem >= 8) {
            if (!SSEEncodeChunk<8>(&srcp, &dstp)) {
                goto slow_path;
            }
            rem -= 8;
        }
        // Roll back to operate in 8 bytes at a time.
        if (len > 8 && rem > 0) {
            dstp -= 8 - rem;
            srcp -= 8 - rem;
            if (!SSEEncodeChunk<8>(&srcp, &dstp)) {
                // TODO: optimize for the case where the input slice has '\0'
                // bytes. (e.g. move the pointer to the first zero byte.)
                dstp += 8 - rem;
                srcp += 8 - rem;
                goto slow_path;
            }
            rem = 0;
            goto done;
        }

    slow_path:
        EncodeChunkLoop(&srcp, &dstp, rem);

    done:
        *dstp++ = 0;
        *dstp++ = 0;
        dst->resize(dstp - reinterpret_cast<uint8_t*>(&(*dst)[0]));
    }
}

inline Status decode_slice(Slice* src, string* dest, bool is_last) {
    if (is_last) {
        dest->append(src->data, src->size);
    } else {
        uint8_t* separator = static_cast<uint8_t*>(memmem(src->data, src->size, "\0\0", 2));
        DCHECK(separator) << "bad encoded primary key, separator not found";
        if (PREDICT_FALSE(separator == nullptr)) {
            LOG(WARNING) << "bad encoded primary key, separator not found";
            return Status::InvalidArgument("bad encoded primary key, separator not found");
        }
        uint8_t* data = (uint8_t*)src->data;
        int len = separator - data;
        for (int i = 0; i < len; i++) {
            if (i >= 1 && data[i - 1] == '\0' && data[i] == '\1') {
                continue;
            }
            dest->push_back((char)data[i]);
        }
        src->remove_prefix(len + 2);
    }
    return Status::OK();
}

typedef void (*EncodeOp)(const void*, int, string*);

static void prepare_ops_datas_selective(const Schema& schema, const Chunk& chunk, vector<EncodeOp>* pops, int from_col,
                                        int to_col, vector<const void*>* pdatas) {
    CHECK(from_col <= to_col && from_col >= 0 && to_col < schema.num_fields()) << "invalid input col index";
    auto& ops = *pops;
    auto& datas = *pdatas;
    ops.resize(to_col + 1, nullptr);
    datas.resize(to_col + 1, nullptr);
    for (int j = from_col; j <= to_col; j++) {
        datas[j] = chunk.get_column_by_index(j)->raw_data();
        switch (schema.field(j)->type()->type()) {
        case TYPE_BOOLEAN:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const uint8_t*)data)[idx], buff);
            };
            break;
        case TYPE_TINYINT:
            ops[j] = [](const void* data, int idx, string* buff) { encode_integral(((const int8_t*)data)[idx], buff); };
            break;
        case TYPE_SMALLINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int16_t*)data)[idx], buff);
            };
            break;
        case TYPE_INT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int32_t*)data)[idx], buff);
            };
            break;
        case TYPE_BIGINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int64_t*)data)[idx], buff);
            };
            break;
        case TYPE_LARGEINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int128_t*)data)[idx], buff);
            };
            break;
        case TYPE_VARCHAR:
            if (j == to_col) {
                ops[j] = [](const void* data, int idx, string* buff) {
                    encode_slice(((const Slice*)data)[idx], buff, true);
                };
            } else {
                ops[j] = [](const void* data, int idx, string* buff) {
                    encode_slice(((const Slice*)data)[idx], buff, false);
                };
            }
            break;
        case TYPE_DATE:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int32_t*)data)[idx], buff);
            };
            break;
        case TYPE_DATETIME:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int64_t*)data)[idx], buff);
            };
            break;
        default:
            CHECK(false) << "type not supported for primary key encoding "
                         << logical_type_to_string(schema.field(j)->type()->type());
        }
    }
}

void RowStoreEncoder::chunk_to_keys(const Schema& schema, const Chunk& chunk, size_t offset, size_t len,
                                    std::vector<std::string>& keys) {
    vector<EncodeOp> ops;
    vector<const void*> datas;
    prepare_ops_datas_selective(schema, chunk, &ops, 0, schema.num_key_fields() - 1, &datas);
    keys.resize(len);
    for (size_t i = 0; i < len; i++) {
        for (int j = 0; j < schema.num_key_fields(); j++) {
            ops[j](datas[j], offset + i, &keys[i]);
        }
    }
}

void RowStoreEncoder::chunk_to_values(const Schema& schema, const Chunk& chunk, size_t offset, size_t len,
                                      std::vector<std::string>& values) {
    vector<EncodeOp> ops;
    vector<const void*> datas;
    prepare_ops_datas_selective(schema, chunk, &ops, schema.num_key_fields(), schema.num_fields() - 1, &datas);
    values.resize(len);
    for (size_t i = 0; i < len; i++) {
        for (int j = schema.num_key_fields(); j < schema.num_fields(); j++) {
            ops[j](datas[j], offset + i, &values[i]);
        }
    }
}

void RowStoreEncoder::encode_chunk_to_full_row_column(const Schema& schema, const Chunk& chunk,
                                                      BinaryColumn* dest_column) {
    vector<EncodeOp> ops;
    vector<const void*> datas;
    prepare_ops_datas_selective(schema, chunk, &ops, schema.num_key_fields(), schema.num_fields() - 1, &datas);
    size_t len = chunk.num_rows();
    dest_column->reserve(dest_column->size() + len);
    string buff;
    for (size_t i = 0; i < len; i++) {
        buff.clear();
        for (int j = schema.num_key_fields(); j < schema.num_fields(); j++) {
            ops[j](datas[j], i, &buff);
        }
        dest_column->append(buff);
    }
}

static void prepare_ops_datas_selective_for_columns(const Schema& schema, int from_col, int ncol,
                                                    const vector<Column*>& columns, vector<EncodeOp>* pops,
                                                    vector<const void*>* pdatas) {
    auto& ops = *pops;
    auto& datas = *pdatas;
    ops.resize(ncol, nullptr);
    datas.resize(ncol, nullptr);
    for (int j = 0; j < ncol; j++) {
        datas[j] = columns[j]->raw_data();
        switch (schema.field(from_col + j)->type()->type()) {
        case TYPE_BOOLEAN:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const uint8_t*)data)[idx], buff);
            };
            break;
        case TYPE_TINYINT:
            ops[j] = [](const void* data, int idx, string* buff) { encode_integral(((const int8_t*)data)[idx], buff); };
            break;
        case TYPE_SMALLINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int16_t*)data)[idx], buff);
            };
            break;
        case TYPE_INT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int32_t*)data)[idx], buff);
            };
            break;
        case TYPE_BIGINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int64_t*)data)[idx], buff);
            };
            break;
        case TYPE_LARGEINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int128_t*)data)[idx], buff);
            };
            break;
        case TYPE_VARCHAR:
            if (j == ncol - 1) {
                ops[j] = [](const void* data, int idx, string* buff) {
                    encode_slice(((const Slice*)data)[idx], buff, true);
                };
            } else {
                ops[j] = [](const void* data, int idx, string* buff) {
                    encode_slice(((const Slice*)data)[idx], buff, false);
                };
            }
            break;
        case TYPE_DATE:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int32_t*)data)[idx], buff);
            };
            break;
        case TYPE_DATETIME:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int64_t*)data)[idx], buff);
            };
            break;
        default:
            CHECK(false) << "type not supported for primary key encoding "
                         << logical_type_to_string(schema.field(j)->type()->type());
        }
    }
}

void RowStoreEncoder::encode_columns_to_full_row_column(const Schema& schema, const vector<Column*>& columns,
                                                        BinaryColumn& dest) {
    vector<EncodeOp> ops;
    vector<const void*> datas;
    prepare_ops_datas_selective_for_columns(schema, schema.num_key_fields(), columns.size(), columns, &ops, &datas);
    size_t len = columns[0]->size();
    dest.reserve(dest.size() + len);
    string buff;
    for (size_t i = 0; i < len; i++) {
        buff.clear();
        for (int j = 0; j < columns.size(); j++) {
            ops[j](datas[j], i, &buff);
        }
        dest.append(buff);
    }
}

void RowStoreEncoder::extract_columns_from_full_row_column(const Schema& schema, const BinaryColumn& full_row_column,
                                                           const std::vector<uint32_t>& read_column_ids,
                                                           vector<std::unique_ptr<Column>>& dest) {
    for (size_t i = 0; i < full_row_column.size(); i++) {
        Slice s = full_row_column.get_slice(i);
        uint32_t cur_read_idx = 0;
        for (uint j = schema.num_key_fields(); j <= read_column_ids.back(); j++) {
            Column* dest_column = nullptr;
            if (read_column_ids[cur_read_idx] == j) {
                dest_column = dest[cur_read_idx].get();
                cur_read_idx++;
            }
            switch (schema.field(j)->type()->type()) {
            case TYPE_BOOLEAN: {
                uint8_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_TINYINT: {
                int8_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_SMALLINT: {
                int16_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_INT: {
                int32_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_BIGINT: {
                int64_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_LARGEINT: {
                int128_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_VARCHAR: {
                string v;
                decode_slice(&s, &v, (j == schema.num_fields() - 2) || (j == schema.num_key_fields() - 1));
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_DATE: {
                DateValue v;
                decode_integral(&s, &v._julian);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_DATETIME: {
                TimestampValue v;
                decode_integral(&s, &v._timestamp);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            default:
                CHECK(false) << "type not supported for primary key encoding";
            }
        }
    }
}

Status RowStoreEncoder::kvs_to_chunk(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                                     const Schema& schema, Chunk* dest) {
    CHECK(keys.size() == values.size()) << "key size should equal to value size";
    for (int i = 0; i < keys.size(); i++) {
        Slice s = Slice(keys[i]);
        for (int j = 0; j < schema.num_fields(); j++) {
            if (j == schema.num_key_fields()) {
                s = Slice(values[i]);
            }
            //auto& column = *(dest->get_column_by_index(j));
            switch (schema.field(j)->type()->type()) {
            case TYPE_BOOLEAN: {
                //auto& tc = down_cast<UInt8Column&>(column);
                uint8_t v;
                decode_integral(&s, &v);
                //tc.append((int8_t)v);
                dest->get_column_by_index(j).get()->append_datum(Datum(v));
            } break;
            case TYPE_TINYINT: {
                //auto& tc = down_cast<Int8Column&>(column);
                int8_t v;
                decode_integral(&s, &v);
                //tc.append(v);
                dest->get_column_by_index(j).get()->append_datum(Datum(v));
            } break;
            case TYPE_SMALLINT: {
                //auto& tc = down_cast<Int16Column&>(column);
                int16_t v;
                decode_integral(&s, &v);
                //tc.append(v);
                dest->get_column_by_index(j).get()->append_datum(Datum(v));
            } break;
            case TYPE_INT: {
                //auto& tc = down_cast<Int32Column&>(column);
                int32_t v;
                decode_integral(&s, &v);
                dest->get_column_by_index(j).get()->append_datum(Datum(v));
            } break;
            case TYPE_BIGINT: {
                //auto& tc = down_cast<Int64Column&>(column);
                int64_t v;
                decode_integral(&s, &v);
                //tc.append(v);
                dest->get_column_by_index(j).get()->append_datum(Datum(v));
            } break;
            case TYPE_LARGEINT: {
                //auto& tc = down_cast<Int128Column&>(column);
                int128_t v;
                decode_integral(&s, &v);
                //tc.append(v);
                dest->get_column_by_index(j).get()->append_datum(Datum(v));
            } break;
            case TYPE_VARCHAR: {
                //auto& tc = down_cast<BinaryColumn&>(column);
                string v;
                RETURN_IF_ERROR(
                        decode_slice(&s, &v, (j == schema.num_fields() - 1) || (j == schema.num_key_fields() - 1)));
                //tc.append(v);
                dest->get_column_by_index(j).get()->append_datum(Datum(Slice(v)));
            } break;
            case TYPE_DATE: {
                //auto& tc = down_cast<DateColumn&>(column);
                DateValue v;
                decode_integral(&s, &v._julian);
                //tc.append(v);
                dest->get_column_by_index(j).get()->append_datum(Datum(v));
            } break;
            case TYPE_DATETIME: {
                //auto& tc = down_cast<TimestampColumn&>(column);
                TimestampValue v;
                decode_integral(&s, &v._timestamp);
                //tc.append(v);
                dest->get_column_by_index(j).get()->append_datum(Datum(v));
            } break;
            default:
                CHECK(false) << "type not supported for primary key encoding";
            }
        }
    }
    return Status::OK();
}

void RowStoreEncoder::combine_key_with_ver(std::string& key, const int8_t op, const int64_t version) {
    uint32_t key_len = key.length();
    encode_integral(encode_version(op, version), &key);
    encode_integral(key_len, &key);
}

Status RowStoreEncoder::split_key_with_ver(const std::string& ckey, std::string& key, int8_t& op, int64_t& version) {
    Slice len_slice = Slice(ckey.data() + ckey.length() - 4, 4);
    Slice ver_slice = Slice(ckey.data() + ckey.length() - 12, 8);
    uint32_t len = 0;
    int64_t ver = 0;
    decode_integral(&len_slice, &len);
    decode_integral(&ver_slice, &ver);
    CHECK(ckey.length() == len + 12) << "split_key_with_ver error";
    key = ckey.substr(0, len);
    decode_version(ver, op, version);
    return Status::OK();
}

std::unique_ptr<Schema> create_binary_schema() {
    Fields fields;
    string name = "col0";
    auto fd = new Field(0, name, TYPE_VARCHAR, false);
    fd->set_is_key(true);
    fd->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    fields.emplace_back(fd);
    return std::unique_ptr<Schema>(new Schema(std::move(fields), KeysType::PRIMARY_KEYS, {}));
}

void RowStoreEncoder::pk_column_to_keys(Schema& pkey_schema, Column* column, std::vector<std::string>& keys) {
    std::unique_ptr<Schema> create_schema;
    Schema* select_schema;
    if (pkey_schema.num_key_fields() == 1) {
        select_schema = &pkey_schema;
    } else {
        create_schema = std::move(create_binary_schema());
        select_schema = create_schema.get();
    }
    auto chunk_ptr = ChunkHelper::new_chunk(*select_schema, 4096);
    chunk_ptr->get_column_by_index(0)->append(*column);
    RowStoreEncoder::chunk_to_keys(*select_schema, *chunk_ptr.get(), 0, chunk_ptr.get()->num_rows(), keys);
}

} // namespace starrocks