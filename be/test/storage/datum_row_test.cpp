// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/datum_row.h"

#include <gtest/gtest.h>

namespace starrocks {

class DatumRowTest : public testing::Test {
public:

    void SetUp() override {
    }

    void TearDown() override {
    }
};

static int96_t create_int96(uint64_t lo, uint32_t hi) {
    int96_t int96;
    int96.lo = lo;
    int96.hi = hi;
    return int96;
}

TEST_F(DatumRowTest, test_get_and_set_type) {
    DatumRow datum_row(24);
    ASSERT_EQ(24, datum_row.size());

    datum_row.set_int8(0, (int8_t) -9);
    ASSERT_EQ((int8_t) -9, datum_row.get_int8(0));

    datum_row.set_uint8(1, (uint8_t) 129);
    ASSERT_EQ((uint8_t) 129, datum_row.get_uint8(1));

    datum_row.set_int16(2, (int16_t) -30000);
    ASSERT_EQ((int16_t) -30000, datum_row.get_int16(2));

    datum_row.set_uint16(3, (uint16_t) 60000);
    ASSERT_EQ((uint16_t) 60000, datum_row.get_uint16(3));

    datum_row.set_uint24(4, uint24_t(100000));
    ASSERT_EQ(uint24_t(100000), datum_row.get_uint24(4));

    datum_row.set_int32(5, (int32_t) -(1 << 26));
    ASSERT_EQ((int32_t) -(1 << 26), datum_row.get_int32(5));

    datum_row.set_uint32(6, (uint32_t) (1 << 31));
    ASSERT_EQ((uint32_t) (1 << 31), datum_row.get_uint32(6));

    datum_row.set_int64(7, (int64_t) -(1L << 40));
    ASSERT_EQ((int64_t) -(1L << 40), datum_row.get_int64(7));

    datum_row.set_uint64(8, (uint64_t) (1L << 63));
    ASSERT_EQ((uint64_t) (1L << 63), datum_row.get_uint64(8));

    int96_t int96 = create_int96((uint64_t) (1L << 63), (uint32_t) (1 << 31));
    datum_row.set_int96(9, int96);
    ASSERT_EQ(int96, datum_row.get_int96(9));

    datum_row.set_float(10, 3.4f);
    ASSERT_EQ(3.4f, datum_row.get_float(10));

    datum_row.set_double(11, 232.432);
    ASSERT_EQ(232.432, datum_row.get_double(11));

    vectorized::TimestampValue timestamp_value =
            vectorized::TimestampValue::create(2022, 9, 21, 10, 5, 3);
    datum_row.set_timestamp(12, timestamp_value);
    ASSERT_EQ(timestamp_value, datum_row.get_timestamp(12));

    vectorized::DateValue date_value =vectorized::DateValue::create(2022, 9, 21);
    datum_row.set_date(13, date_value);
    ASSERT_EQ(date_value, datum_row.get_date(13));

    datum_row.set_slice(14, Slice("abcd"));
    ASSERT_EQ(Slice("abcd"), datum_row.get_slice(14));

    datum_row.set_decimal12(15, decimal12_t(25, 85));
    ASSERT_EQ(decimal12_t(25, 85), datum_row.get_decimal12(15));

    datum_row.set_decimal(16, DecimalV2Value("120.8374"));
    ASSERT_EQ(DecimalV2Value("120.8374"
                             ""), datum_row.get_decimal(16));
}

TEST_F(DatumRowTest, test_get_and_set_datum) {
    DatumRow datum_row(2);
    ASSERT_EQ(2, datum_row.size());

    datum_row.set_datum(0, vectorized::Datum(Slice("8nfasdf")));
    ASSERT_EQ(Slice("8nfasdf"), datum_row.get_datum(0).get_slice());

    datum_row.set_datum(1, vectorized::Datum(0.384f));
    ASSERT_EQ(0.384f, datum_row.get_datum(1).get_float());
}

} // namespace starrocks