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

#include <array>
#include <initializer_list>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/datum_convert.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/type_traits.h"
#include "storage/types.h"

namespace starrocks {

using Datums = std::vector<Datum>;

/// DatumBuilder
template <LogicalType Type>
struct DatumBuilder {
    template <typename T>
    Datums operator()(const std::vector<T>& data) const {
        Datums datums;
        datums.resize(data.size());
        for (size_t i = 0; i < data.size(); i++) {
            datums[i].set(static_cast<typename TypeTraits<Type>::CppType>(data[i]));
        }
        return datums;
    }

    template <typename T>
    Datums operator()(const std::initializer_list<T>& data) const {
        return operator()(std::vector<T>(data));
    }
};

static std::vector<const char*> to_cstring(const std::vector<std::string>& v) {
    std::vector<const char*> res;
    for (const auto& s : v) {
        res.emplace_back(s.c_str());
    }
    return res;
}

template <>
struct DatumBuilder<TYPE_DECIMALV2> {
    Datums operator()(const std::vector<std::string>& data) const { return operator()(to_cstring(data)); }

    Datums operator()(const std::vector<const char*>& data) const {
        Datums datums;
        datums.resize(data.size());
        for (size_t i = 0; i < data.size(); i++) {
            Datum& d = datums[i];
            auto type_info = get_type_info(TYPE_DECIMALV2);
            CHECK(datum_from_string(type_info.get(), &d, data[i], nullptr).ok());
        }
        return datums;
    }

    template <typename T>
    Datums operator()(const std::initializer_list<T>& data) const {
        return operator()(std::vector<T>(data));
    }
};

template <>
struct DatumBuilder<TYPE_DATE> {
    Datums operator()(const std::vector<std::string>& data) const { return operator()(to_cstring(data)); }

    Datums operator()(const std::vector<const char*>& data) const {
        Datums datums;
        datums.resize(data.size());
        for (size_t i = 0; i < data.size(); i++) {
            Datum& d = datums[i];
            auto type_info = get_type_info(TYPE_DATE);
            CHECK(datum_from_string(type_info.get(), &d, data[i], nullptr).ok());
        }
        return datums;
    }

    template <typename T>
    Datums operator()(const std::initializer_list<T>& data) const {
        return operator()(std::vector<T>(data));
    }
};

template <>
struct DatumBuilder<TYPE_DATETIME> {
    Datums operator()(const std::vector<std::string>& data) const { return operator()(to_cstring(data)); }

    Datums operator()(const std::vector<const char*>& data) const {
        Datums datums;
        datums.resize(data.size());
        for (size_t i = 0; i < data.size(); i++) {
            Datum& d = datums[i];
            auto type_info = get_type_info(TYPE_DATETIME);
            CHECK(datum_from_string(type_info.get(), &d, data[i], nullptr).ok());
        }
        return datums;
    }

    template <typename T>
    Datums operator()(const std::initializer_list<T>& data) const {
        return operator()(std::vector<T>(data));
    }
};

[[maybe_unused]] static DatumBuilder<TYPE_BOOLEAN> COL_BOOLEAN;   // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_CHAR> COL_CHAR;         // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_VARCHAR> COL_VARCHAR;   // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_TINYINT> COL_TINYINT;   // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_SMALLINT> COL_SMALLINT; // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_INT> COL_INT;           // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_BIGINT> COL_BIGINT;     // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_LARGEINT> COL_LARGEINT; // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_FLOAT> COL_FLOAT;       // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_DOUBLE> COL_DOUBLE;     // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_DECIMALV2> COL_DECIMAL; // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_DATE> COL_DATE;         // NOLINT
[[maybe_unused]] static DatumBuilder<TYPE_DATETIME> COL_DATETIME; // NOLINT

/// VectorChunkIterator
class VectorChunkIterator final : public ChunkIterator {
public:
    VectorChunkIterator(Schema schema, Datums c1) : ChunkIterator(std::move(schema)), _columns{std::move(c1)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2), std::move(c3)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2), std::move(c3), std::move(c4)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4, Datums c5)
            : ChunkIterator(std::move(schema)),
              _columns{std::move(c1), std::move(c2), std::move(c3), std::move(c4), std::move(c5)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4, Datums c5, Datums c6)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2), std::move(c3),
                                                         std::move(c4), std::move(c5), std::move(c6)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4, Datums c5, Datums c6, Datums c7)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2), std::move(c3), std::move(c4),
                                                         std::move(c5), std::move(c6), std::move(c7)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4, Datums c5, Datums c6, Datums c7,
                        Datums c8)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2), std::move(c3), std::move(c4),
                                                         std::move(c5), std::move(c6), std::move(c7), std::move(c8)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4, Datums c5, Datums c6, Datums c7,
                        Datums c8, Datums c9)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2), std::move(c3),
                                                         std::move(c4), std::move(c5), std::move(c6),
                                                         std::move(c7), std::move(c8), std::move(c9)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4, Datums c5, Datums c6, Datums c7,
                        Datums c8, Datums c9, Datums c10)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2), std::move(c3), std::move(c4),
                                                         std::move(c5), std::move(c6), std::move(c7), std::move(c8),
                                                         std::move(c9), std::move(c10)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4, Datums c5, Datums c6, Datums c7,
                        Datums c8, Datums c9, Datums c10, Datums c11)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1), std::move(c2),  std::move(c3), std::move(c4),
                                                         std::move(c5), std::move(c6),  std::move(c7), std::move(c8),
                                                         std::move(c9), std::move(c10), std::move(c11)} {}

    VectorChunkIterator(Schema schema, Datums c1, Datums c2, Datums c3, Datums c4, Datums c5, Datums c6, Datums c7,
                        Datums c8, Datums c9, Datums c10, Datums c11, Datums c12)
            : ChunkIterator(std::move(schema)), _columns{std::move(c1),  std::move(c2),  std::move(c3),
                                                         std::move(c4),  std::move(c5),  std::move(c6),
                                                         std::move(c7),  std::move(c8),  std::move(c9),
                                                         std::move(c10), std::move(c11), std::move(c12)} {}

    void chunk_size(size_t n) { _chunk_size = n; }
    size_t chunk_size() const { return _chunk_size; }

    void next_row(size_t row) { _next_row = row; }
    size_t next_row() const { return _next_row; }

    Status do_get_next(Chunk* chunk) override {
        size_t nr = std::min(_chunk_size, _columns[0].size() - _next_row);
        for (size_t i = 0; i < nr; i++) {
            next_row(chunk);
        }
        return nr > 0 ? Status::OK() : Status::EndOfFile("eof");
    }

    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override { return do_get_next(chunk); }

    void close() override {}

private:
    void next_row(Chunk* chunk) {
        for (size_t i = 0; i < _schema.num_fields(); i++) {
            const Datum& v = _columns[i][_next_row];
            chunk->get_column_by_index(i)->append_datum(v);
        }
        _next_row++;
    }

    size_t _chunk_size = 1024;
    size_t _next_row = 0;
    std::vector<Datums> _columns;
};

} // namespace starrocks
