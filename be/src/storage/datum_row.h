// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/row.h"
#include "util/int96.h"

namespace starrocks {

class DatumRow final : public Row {

public:

    explicit DatumRow(size_t num_columns): _datums(num_columns) {
    }

    size_t size() const override {
        return _datums.size();
    }

    const vectorized::Datum& get_datum(size_t ordinal) {
        return safe_get_datum(ordinal);
    }

    int8_t get_int8(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_int8();
    }

    uint8_t get_uint8(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_uint8();
    }

    int16_t get_int16(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_int16();
    }

    uint16_t get_uint16(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_uint16();
    }

    uint24_t get_uint24(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_uint24();
    }

    int32_t get_int32(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_int32();
    }

    uint32_t get_uint32(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_uint32();
    }

    int64_t get_int64(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_int64();
    }

    uint64_t get_uint64(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_uint64();
    }

    int96_t get_int96(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_int96();
    }

    const int128_t& get_int128(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_int128();
    }

    float get_float(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_float();
    }

    double get_double(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_double();
    }

    vectorized::TimestampValue get_timestamp(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_timestamp();
    }

    vectorized::DateValue get_date(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_date();
    }

    const Slice& get_slice(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_slice();
    }

    const decimal12_t& get_decimal12(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_decimal12();
    }

    const DecimalV2Value& get_decimal(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_decimal();
    }

    const DatumArray& get_array(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_array();
    }

    const HyperLogLog* get_hyperloglog(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_hyperloglog();
    }

    const BitmapValue* get_bitmap(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_bitmap();
    }

    const PercentileValue* get_percentile(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_percentile();
    }

    const JsonValue* get_json(size_t ordinal) const override {
        return safe_get_datum(ordinal).get_json();
    }

    void set_datum(size_t ordinal, const vectorized::Datum& datum) {
        safe_set_datum(ordinal, datum);
    }

    void set_int8(size_t ordinal, int8_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_uint8(size_t ordinal, uint8_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_int16(size_t ordinal, int16_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_uint16(size_t ordinal, uint16_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_uint24(size_t ordinal, uint24_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_int32(size_t ordinal, int32_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_uint32(size_t ordinal, uint32_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_int64(size_t ordinal, int64_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_uint64(size_t ordinal, uint64_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_int96(size_t ordinal, int96_t v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_int128(size_t ordinal, const int128_t& v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_float(size_t ordinal, float v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_double(size_t ordinal, double v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_timestamp(size_t ordinal, vectorized::TimestampValue v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_date(size_t ordinal, vectorized::DateValue v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_slice(size_t ordinal, const Slice& v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_decimal12(size_t ordinal, const decimal12_t& v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_decimal(size_t ordinal, const DecimalV2Value& v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_array(size_t ordinal, const DatumArray& v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_hyperloglog(size_t ordinal, HyperLogLog* v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_bitmap(size_t ordinal, BitmapValue* v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_percentile(size_t ordinal, PercentileValue* v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

    void set_json(size_t ordinal, JsonValue* v) override {
        safe_set_datum(ordinal, vectorized::Datum(v));
    }

private:

    const vectorized::Datum& safe_get_datum(size_t ordinal) const {
        // TODO check ordinal is valid
        return _datums.at(ordinal);
    }

    void safe_set_datum(size_t ordinal, const vectorized::Datum& datum) {
        // TODO check ordinal is valid
        _datums[ordinal] = datum;
    }

    std::vector<vectorized::Datum> _datums;
};

} // namespace starrocks