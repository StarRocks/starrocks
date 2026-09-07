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

#include <gtest/gtest.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "gen_cpp/parquet_types.h"

namespace starrocks::parquet {
namespace {
template <typename T>
T round_trip(const T& input) {
    auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TCompactProtocol protocol(buffer);
    input.write(&protocol);
    T output;
    output.read(&protocol);
    return output;
}
} // namespace

TEST(GeoAnnotationWireTest, StandardOrdinalsAndUnknownAlgorithms) {
    // Write the external field IDs independently of generated Thrift writers.
    for (int16_t kind : {17, 18}) {
        for (int32_t algorithm : {0, 1, 2, 3, 4, 127}) {
            auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
            apache::thrift::protocol::TCompactProtocol protocol(buffer);
            using namespace apache::thrift::protocol;
            protocol.writeStructBegin("LogicalType");
            protocol.writeFieldBegin("geo", T_STRUCT, kind);
            protocol.writeStructBegin("Geo");
            protocol.writeFieldBegin("crs", T_STRING, 1);
            protocol.writeString("EPSG:4326");
            protocol.writeFieldEnd();
            if (kind == 18) {
                protocol.writeFieldBegin("algorithm", T_I32, 2);
                protocol.writeI32(algorithm);
                protocol.writeFieldEnd();
            }
            protocol.writeFieldStop();
            protocol.writeStructEnd();
            protocol.writeFieldEnd();
            protocol.writeFieldStop();
            protocol.writeStructEnd();
            tparquet::LogicalType decoded;
            decoded.read(&protocol);
            if (kind == 17) {
                ASSERT_TRUE(decoded.__isset.GEOMETRY);
                EXPECT_EQ("EPSG:4326", decoded.GEOMETRY.crs);
            } else {
                ASSERT_TRUE(decoded.__isset.GEOGRAPHY);
                EXPECT_EQ("EPSG:4326", decoded.GEOGRAPHY.crs);
                EXPECT_EQ(algorithm, decoded.GEOGRAPHY.algorithm);
            }
            EXPECT_EQ(decoded, round_trip(decoded));
        }
    }
}

TEST(GeoAnnotationWireTest, OptionalDefaultsAndOrdinaryBinary) {
    tparquet::SchemaElement field;
    field.__set_name("shape");
    field.__set_type(tparquet::Type::BYTE_ARRAY);
    field.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    EXPECT_FALSE(round_trip(field).__isset.logicalType);

    tparquet::LogicalType logical;
    logical.__set_GEOGRAPHY(tparquet::GeographyType());
    field.__set_logicalType(logical);
    auto decoded = round_trip(field);
    EXPECT_TRUE(decoded.logicalType.__isset.GEOGRAPHY);
    EXPECT_FALSE(decoded.logicalType.GEOGRAPHY.__isset.crs);
    EXPECT_FALSE(decoded.logicalType.GEOGRAPHY.__isset.algorithm);
    // Preserve absence on the wire; the reader applies CRS84/SPHERICAL defaults.
    EXPECT_EQ(field, decoded);

    logical = tparquet::LogicalType();
    logical.__set_GEOMETRY(tparquet::GeometryType());
    field.__set_logicalType(logical);
    decoded = round_trip(field);
    EXPECT_TRUE(decoded.logicalType.__isset.GEOMETRY);
    EXPECT_FALSE(decoded.logicalType.GEOMETRY.__isset.crs);
    EXPECT_EQ(field, decoded);
}

TEST(GeoAnnotationWireTest, BoundingBoxRequiresXYButNotZM) {
    using namespace apache::thrift::protocol;
    // Independently encode the upstream fields, omitting each required coordinate in turn.
    for (int16_t omitted = 0; omitted <= 4; ++omitted) {
        auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        TCompactProtocol protocol(buffer);
        protocol.writeStructBegin("BoundingBox");
        for (int16_t id = 1; id <= 4; ++id) {
            if (id == omitted) {
                continue;
            }
            protocol.writeFieldBegin("coordinate", T_DOUBLE, id);
            protocol.writeDouble(id);
            protocol.writeFieldEnd();
        }
        protocol.writeFieldStop();
        protocol.writeStructEnd();
        tparquet::BoundingBox bbox;
        if (omitted != 0) {
            EXPECT_THROW(bbox.read(&protocol), TProtocolException);
        } else {
            ASSERT_NO_THROW(bbox.read(&protocol));
            EXPECT_EQ(1, bbox.xmin);
            EXPECT_EQ(2, bbox.xmax);
            EXPECT_EQ(3, bbox.ymin);
            EXPECT_EQ(4, bbox.ymax);
            EXPECT_FALSE(bbox.__isset.zmin);
            EXPECT_FALSE(bbox.__isset.zmax);
            EXPECT_FALSE(bbox.__isset.mmin);
            EXPECT_FALSE(bbox.__isset.mmax);
            EXPECT_EQ(bbox, round_trip(bbox));
        }
    }
}

TEST(GeoAnnotationWireTest, GeospatialStatisticsRemainMetadata) {
    tparquet::BoundingBox bbox;
    bbox.__set_xmin(-180);
    bbox.__set_xmax(180);
    bbox.__set_ymin(-90);
    bbox.__set_ymax(90);
    bbox.__set_zmin(-10);
    bbox.__set_zmax(10);
    bbox.__set_mmin(0);
    bbox.__set_mmax(100);
    tparquet::GeospatialStatistics stats;
    stats.__set_bbox(bbox);
    stats.__set_geospatial_types({1, 3, 7});
    tparquet::ColumnMetaData column;
    column.__set_type(tparquet::Type::BYTE_ARRAY);
    column.__set_geospatial_statistics(stats);
    EXPECT_EQ(column, round_trip(column));
    // Recognition does not define pruning or decode any WKB payload.
}
} // namespace starrocks::parquet
