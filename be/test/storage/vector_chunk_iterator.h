// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <array>
#include <initializer_list>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/datum_convert.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/types.h"

namespace starrocks::vectorized {

using Datums = std::vector<Datum>;

/// DatumBuilder
template <FieldType Type>
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
struct DatumBuilder<OLAP_FIELD_TYPE_DECIMAL_V2> {
    Datums operator()(const std::vector<std::string>& data) const { return operator()(to_cstring(data)); }

    Datums operator()(const std::vector<const char*>& data) const {
        Datums datums;
        datums.resize(data.size());
        for (size_t i = 0; i < data.size(); i++) {
            Datum& d = datums[i];
            auto type_info = get_type_info(OLAP_FIELD_TYPE_DECIMAL_V2);
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
struct DatumBuilder<OLAP_FIELD_TYPE_DATE_V2> {
    Datums operator()(const std::vector<std::string>& data) const { return operator()(to_cstring(data)); }

    Datums operator()(const std::vector<const char*>& data) const {
        Datums datums;
        datums.resize(data.size());
        for (size_t i = 0; i < data.size(); i++) {
            Datum& d = datums[i];
            auto type_info = get_type_info(OLAP_FIELD_TYPE_DATE_V2);
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
struct DatumBuilder<OLAP_FIELD_TYPE_TIMESTAMP> {
    Datums operator()(const std::vector<std::string>& data) const { return operator()(to_cstring(data)); }

    Datums operator()(const std::vector<const char*>& data) const {
        Datums datums;
        datums.resize(data.size());
        for (size_t i = 0; i < data.size(); i++) {
            Datum& d = datums[i];
            auto type_info = get_type_info(OLAP_FIELD_TYPE_TIMESTAMP);
            CHECK(datum_from_string(type_info.get(), &d, data[i], nullptr).ok());
        }
        return datums;
    }

    template <typename T>
    Datums operator()(const std::initializer_list<T>& data) const {
        return operator()(std::vector<T>(data));
    }
};

[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_BOOL> COL_BOOLEAN;       // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_CHAR> COL_CHAR;          // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_VARCHAR> COL_VARCHAR;    // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_TINYINT> COL_TINYINT;    // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_SMALLINT> COL_SMALLINT;  // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_INT> COL_INT;            // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_BIGINT> COL_BIGINT;      // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_LARGEINT> COL_LARGEINT;  // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_FLOAT> COL_FLOAT;        // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_DOUBLE> COL_DOUBLE;      // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_DECIMAL_V2> COL_DECIMAL; // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_DATE_V2> COL_DATE;       // NOLINT
[[maybe_unused]] static DatumBuilder<OLAP_FIELD_TYPE_TIMESTAMP> COL_DATETIME; // NOLINT

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

} // namespace starrocks::vectorized
