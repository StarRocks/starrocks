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

#include "base/coding.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/meta_helper.h"
#include "formats/parquet/schema.h"
#include "fs/fs.h"
#include "io/string_input_stream.h"
#include "runtime/current_thread.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/predicate_tree/predicate_tree.h"

namespace starrocks::parquet {
namespace {
tparquet::SchemaElement geo_element(bool geography = true) {
    tparquet::SchemaElement element;
    element.__set_name("shape");
    element.__set_field_id(1);
    element.__set_type(tparquet::Type::BYTE_ARRAY);
    element.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::LogicalType logical;
    if (geography) {
        logical.__set_GEOGRAPHY(tparquet::GeographyType());
    } else {
        logical.__set_GEOMETRY(tparquet::GeometryType());
    }
    element.__set_logicalType(logical);
    return element;
}

std::vector<tparquet::SchemaElement> schema_of(tparquet::SchemaElement element) {
    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(1);
    return {root, std::move(element)};
}

TIcebergSchemaField lake_geo(TIcebergGeoKind::type kind = TIcebergGeoKind::GEOGRAPHY, std::string edge = "SPHERICAL") {
    TIcebergGeoMetadata geo;
    geo.__set_kind(kind);
    geo.__set_crs("OGC:CRS84");
    geo.__set_edge_algorithm(edge);
    TIcebergSchemaField field;
    field.__set_field_id(1);
    field.__set_name("shape");
    field.__set_geo_metadata(geo);
    return field;
}

template <typename T>
std::string compact_bytes(const T& value) {
    auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TCompactProtocol protocol(buffer);
    value.write(&protocol);
    return buffer->getBufferAsString();
}

// A complete two-column Parquet file: required id=42 and WKB POINT(0 0).
// Generated in memory to keep the fixture and its external annotations auditable.
std::string geo_file(bool geography, bool dictionary = false, bool annotated = true) {
    auto shape = geo_element(geography);
    if (!annotated) shape.__isset.logicalType = false;
    shape.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    tparquet::SchemaElement id;
    id.__set_name("id");
    id.__set_field_id(2);
    id.__set_type(tparquet::Type::INT32);
    id.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(2);
    std::string bytes = "PAR1";
    std::vector<tparquet::ColumnChunk> columns;
    for (const auto& field : {id, shape}) {
        const auto column_offset = bytes.size();
        std::string payload;
        if (field.name == "id") {
            put_fixed32_le(&payload, 42);
        } else {
            put_fixed32_le(&payload, 21);
            payload.push_back(1);        // Little endian ISO WKB, no embedded SRID.
            put_fixed32_le(&payload, 1); // POINT
            payload.append(16, '\0');
        }
        if (dictionary && field.name == "shape") {
            tparquet::DictionaryPageHeader dictionary_header;
            dictionary_header.__set_num_values(1);
            dictionary_header.__set_encoding(tparquet::Encoding::PLAIN);
            tparquet::PageHeader dictionary_page;
            dictionary_page.__set_type(tparquet::PageType::DICTIONARY_PAGE);
            dictionary_page.__set_dictionary_page_header(dictionary_header);
            dictionary_page.__set_uncompressed_page_size(payload.size());
            dictionary_page.__set_compressed_page_size(payload.size());
            bytes += compact_bytes(dictionary_page) + payload;
            payload.assign("\x01\x02\x00", 3); // One RLE dictionary index, value zero.
        }
        tparquet::DataPageHeader data;
        data.__set_num_values(1);
        data.__set_encoding(tparquet::Encoding::PLAIN);
        if (dictionary && field.name == "shape") data.__set_encoding(tparquet::Encoding::RLE_DICTIONARY);
        data.__set_definition_level_encoding(tparquet::Encoding::RLE);
        data.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        tparquet::PageHeader page;
        page.__set_type(tparquet::PageType::DATA_PAGE);
        page.__set_uncompressed_page_size(payload.size());
        page.__set_compressed_page_size(payload.size());
        page.__set_data_page_header(data);
        auto page_bytes = compact_bytes(page) + payload;
        tparquet::ColumnMetaData metadata;
        metadata.__set_type(field.type);
        metadata.__set_encodings({tparquet::Encoding::PLAIN, tparquet::Encoding::RLE});
        metadata.__set_path_in_schema({field.name});
        metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        metadata.__set_num_values(1);
        metadata.__set_total_uncompressed_size(page_bytes.size());
        metadata.__set_total_compressed_size(page_bytes.size());
        metadata.__set_data_page_offset(bytes.size());
        if (dictionary && field.name == "shape") {
            metadata.__set_dictionary_page_offset(column_offset);
            metadata.__set_encodings(
                    {tparquet::Encoding::PLAIN, tparquet::Encoding::RLE, tparquet::Encoding::RLE_DICTIONARY});
            metadata.__set_total_uncompressed_size(bytes.size() - column_offset + page_bytes.size());
            metadata.__set_total_compressed_size(bytes.size() - column_offset + page_bytes.size());
        }
        if (field.name == "shape" && annotated) {
            // Deliberately misleading bounds must not turn the unsupported geo projection into an empty result.
            tparquet::Statistics stats;
            stats.__set_min_value("z");
            stats.__set_max_value("z");
            metadata.__set_statistics(stats);
            tparquet::BoundingBox bbox;
            bbox.__set_xmin(170);
            bbox.__set_xmax(180);
            bbox.__set_ymin(80);
            bbox.__set_ymax(90);
            tparquet::GeospatialStatistics geo_stats;
            geo_stats.__set_bbox(bbox);
            metadata.__set_geospatial_statistics(geo_stats);
            // Poisoned bloom location: geo guards must run without reading it.
            metadata.__set_bloom_filter_offset(1000000);
            metadata.__set_bloom_filter_length(128);
        }
        tparquet::ColumnChunk column;
        column.__set_file_offset(column_offset);
        column.__set_meta_data(metadata);
        columns.push_back(column);
        bytes += page_bytes;
    }
    tparquet::RowGroup group;
    group.__set_columns(columns);
    group.__set_total_byte_size(bytes.size() - 4);
    group.__set_num_rows(1);
    tparquet::FileMetaData footer;
    footer.__set_version(2);
    footer.__set_schema({root, id, shape});
    footer.__set_num_rows(1);
    footer.__set_row_groups({group});
    auto metadata = compact_bytes(footer);
    bytes += metadata;
    put_fixed32_le(&bytes, metadata.size());
    bytes += "PAR1";
    return bytes;
}
} // namespace

TEST(GeoMetadataTest, WireRoundTripRetainsAlgorithmsAndStatistics) {
    for (int algorithm = 0; algorithm <= 4; ++algorithm) {
        tparquet::FileMetaData metadata;
        auto element = geo_element();
        element.logicalType.GEOGRAPHY.__set_algorithm(
                static_cast<tparquet::EdgeInterpolationAlgorithm::type>(algorithm));
        element.logicalType.GEOGRAPHY.__set_crs("EPSG:4326");
        metadata.__set_version(1);
        metadata.__set_schema(schema_of(element));
        metadata.__set_num_rows(0);
        tparquet::ColumnMetaData column;
        tparquet::GeospatialStatistics stats;
        stats.__set_geospatial_types({1, 3, 7});
        column.__set_geospatial_statistics(stats);
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(column);
        tparquet::RowGroup group;
        group.__set_columns({chunk});
        metadata.__set_row_groups({group});
        auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        apache::thrift::protocol::TCompactProtocol protocol(buffer);
        metadata.write(&protocol);
        tparquet::FileMetaData decoded;
        decoded.read(&protocol);
        EXPECT_EQ(metadata, decoded);
        SchemaDescriptor schema;
        ASSERT_TRUE(schema.from_thrift(decoded.schema, true).ok());
        const auto* field = schema.get_stored_column_by_field_idx(0);
        EXPECT_TRUE(field->contains_geo());
        EXPECT_EQ(algorithm, field->schema_element.logicalType.GEOGRAPHY.algorithm);
        EXPECT_EQ("EPSG:4326", field->schema_element.logicalType.GEOGRAPHY.crs);
    }
}

TEST(GeoMetadataTest, AnnotationValidation) {
    auto element = geo_element();
    for (auto physical : {tparquet::Type::INT32, tparquet::Type::FIXED_LEN_BYTE_ARRAY}) {
        element.__set_type(physical);
        SchemaDescriptor schema;
        EXPECT_FALSE(schema.from_thrift(schema_of(element), true).ok());
    }
    element = geo_element();
    element.logicalType.__set_GEOMETRY(tparquet::GeometryType());
    SchemaDescriptor conflicting;
    EXPECT_FALSE(conflicting.from_thrift(schema_of(element), true).ok());
    element = geo_element();
    element.logicalType.GEOGRAPHY.__set_crs("");
    SchemaDescriptor empty_crs;
    EXPECT_FALSE(empty_crs.from_thrift(schema_of(element), true).ok());
    element = geo_element();
    element.logicalType.__set_STRING(tparquet::StringType());
    SchemaDescriptor string_conflict;
    EXPECT_TRUE(string_conflict.from_thrift(schema_of(element), true).is_invalid_argument());
    element = geo_element();
    element.__set_converted_type(tparquet::ConvertedType::UTF8);
    SchemaDescriptor converted_conflict;
    EXPECT_TRUE(converted_conflict.from_thrift(schema_of(element), true).is_invalid_argument());
    element = geo_element();
    element.__isset.type = false;
    element.__set_num_children(1);
    SchemaDescriptor group_annotation;
    EXPECT_TRUE(group_annotation.from_thrift(schema_of(element), true).is_invalid_argument());
    element = geo_element();
    element.__isset.logicalType = false;
    SchemaDescriptor binary;
    EXPECT_TRUE(binary.from_thrift(schema_of(element), true).ok());
    EXPECT_FALSE(binary.get_stored_column_by_field_idx(0)->contains_geo());
}

TEST(GeoMetadataTest, ColumnReaderCannotBypassScanGuard) {
    ColumnReaderOptions options;
    auto binary = TypeDescriptor::create_varbinary_type(1024);
    auto element = geo_element();
    SchemaDescriptor annotated;
    ASSERT_TRUE(annotated.from_thrift(schema_of(element), true).ok());
    EXPECT_TRUE(ColumnReaderFactory::create(options, annotated.get_stored_column_by_field_idx(0), binary)
                        .status()
                        .is_not_supported());
    element.__isset.logicalType = false;
    SchemaDescriptor unannotated;
    ASSERT_TRUE(unannotated.from_thrift(schema_of(element), true).ok());
    auto lake = lake_geo();
    EXPECT_TRUE(ColumnReaderFactory::create(options, unannotated.get_stored_column_by_field_idx(0), binary, &lake)
                        .status()
                        .is_not_supported());
    TIcebergSchemaField container;
    container.__set_children({lake});
    ParquetField group;
    group.name = "nested";
    group.type = ColumnType::STRUCT;
    // Reject even when a pruned/UNKNOWN child would otherwise skip reader creation.
    EXPECT_TRUE(ColumnReaderFactory::create(options, &group, TypeDescriptor(TYPE_STRUCT), &container)
                        .status()
                        .is_not_supported());
}

TEST(GeoMetadataTest, IcebergConflictsAndUnannotatedFallback) {
    for (bool geography : {true, false}) {
        SchemaDescriptor schema;
        ASSERT_TRUE(schema.from_thrift(schema_of(geo_element(geography)), true).ok());
        const auto& field = *schema.get_stored_column_by_field_idx(0);
        auto lake = geography ? lake_geo() : lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR");
        EXPECT_TRUE(validate_geo_field(field, &lake, true).ok());
        auto opposite = geography ? lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR") : lake_geo();
        EXPECT_TRUE(validate_geo_field(field, &opposite, true).is_invalid_argument());
        lake.geo_metadata.__set_crs("EPSG:4326");
        EXPECT_FALSE(validate_geo_field(field, &lake, true).ok());
        lake = geography ? lake_geo() : lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR");
        lake.geo_metadata.__set_edge_algorithm("KARNEY");
        EXPECT_FALSE(validate_geo_field(field, &lake, true).ok());
        TIcebergSchemaField ordinary;
        ordinary.__set_iceberg_type("BINARY");
        EXPECT_FALSE(validate_geo_field(field, &ordinary, true).ok());
        // An older FE sends neither the source type nor geo metadata. This does
        // not prove a binary/geo conflict, and cannot enable geo projection.
        TIcebergSchemaField old_fe;
        EXPECT_TRUE(validate_geo_field(field, &old_fe, true).ok());
    }
    auto element = geo_element();
    element.__isset.logicalType = false;
    SchemaDescriptor unannotated;
    ASSERT_TRUE(unannotated.from_thrift(schema_of(element), true).ok());
    auto lake = lake_geo();
    EXPECT_TRUE(validate_geo_field(*unannotated.get_stored_column_by_field_idx(0), &lake, true).ok());
    EXPECT_FALSE(unannotated.get_stored_column_by_field_idx(0)->contains_geo());

    element.__set_type(tparquet::Type::INT32);
    SchemaDescriptor wrong_physical;
    ASSERT_TRUE(wrong_physical.from_thrift(schema_of(element), true).ok());
    EXPECT_FALSE(validate_geo_field(*wrong_physical.get_stored_column_by_field_idx(0), &lake, true).ok());
}

TEST(GeoMetadataTest, NestedGeoAndRenamedIcebergFields) {
    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(1);
    tparquet::SchemaElement group;
    group.__set_name("old_container_name");
    group.__set_num_children(1);
    group.__set_field_id(2);
    group.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    SchemaDescriptor schema;
    ASSERT_TRUE(schema.from_thrift({root, group, geo_element()}, true).ok());
    EXPECT_TRUE(schema.get_stored_column_by_field_idx(0)->contains_geo());
    auto child = lake_geo();
    child.__set_name("renamed_shape");
    TIcebergSchemaField container;
    container.__set_field_id(2);
    container.__set_name("container");
    container.__set_children({child});
    TIcebergSchema lake;
    lake.__set_fields({container});
    SlotDescriptor slot(2, "container", TypeDescriptor::create_varbinary_type(1024));
    EXPECT_TRUE(validate_geo_scan(schema, &lake, {}, true).ok());
    EXPECT_TRUE(validate_geo_scan(schema, &lake, {{0, &slot, true}}, true).is_not_supported());
    lake.fields[0].children[0].geo_metadata.__set_kind(TIcebergGeoKind::GEOMETRY);
    lake.fields[0].children[0].geo_metadata.__set_edge_algorithm("PLANAR");
    EXPECT_FALSE(validate_geo_scan(schema, &lake, {}, true).ok());
}

TEST(GeoMetadataTest, IcebergMetadataMustBeConsistentEvenWithoutParquetAnnotation) {
    auto element = geo_element();
    element.__isset.logicalType = false;
    SchemaDescriptor schema;
    ASSERT_TRUE(schema.from_thrift(schema_of(element), true).ok());
    const auto& field = *schema.get_stored_column_by_field_idx(0);
    auto lake = lake_geo();
    lake.__set_iceberg_type("GEOGRAPHY");
    EXPECT_TRUE(validate_geo_field(field, &lake, true).ok());
    lake.__isset.geo_metadata = false;
    EXPECT_TRUE(validate_geo_field(field, &lake, true).is_invalid_argument());
    lake = lake_geo();
    lake.__set_iceberg_type("BINARY");
    EXPECT_TRUE(validate_geo_field(field, &lake, true).is_invalid_argument());
    lake = lake_geo();
    lake.geo_metadata.__set_kind(static_cast<TIcebergGeoKind::type>(127));
    EXPECT_TRUE(validate_geo_field(field, &lake, true).is_invalid_argument());
    lake = lake_geo();
    lake.geo_metadata.__set_edge_algorithm("FUTURE_EDGE");
    EXPECT_TRUE(validate_geo_field(field, &lake, true).is_not_supported());
    lake = lake_geo();
    lake.geo_metadata.__isset.crs = false;
    EXPECT_TRUE(validate_geo_field(field, &lake, true).is_invalid_argument());
    lake = lake_geo();
    element.__set_converted_type(tparquet::ConvertedType::UTF8);
    SchemaDescriptor text_schema;
    ASSERT_TRUE(text_schema.from_thrift(schema_of(element), true).ok());
    EXPECT_TRUE(validate_geo_field(*text_schema.get_stored_column_by_field_idx(0), &lake, true).is_invalid_argument());
}

TEST(GeoMetadataTest, EveryGeographyAlgorithmMatchesWithoutEnablingCompute) {
    const std::vector<std::string> algorithms{"SPHERICAL", "VINCENTY", "THOMAS", "ANDOYER", "KARNEY"};
    for (int i = 0; i < algorithms.size(); ++i) {
        auto element = geo_element();
        element.logicalType.GEOGRAPHY.__set_algorithm(static_cast<tparquet::EdgeInterpolationAlgorithm::type>(i));
        SchemaDescriptor schema;
        ASSERT_TRUE(schema.from_thrift(schema_of(element), true).ok());
        auto lake = lake_geo(TIcebergGeoKind::GEOGRAPHY, algorithms[i]);
        EXPECT_TRUE(validate_geo_field(*schema.get_stored_column_by_field_idx(0), &lake, true).ok());
        lake.geo_metadata.__set_edge_algorithm(algorithms[(i + 1) % algorithms.size()]);
        EXPECT_TRUE(validate_geo_field(*schema.get_stored_column_by_field_idx(0), &lake, true).is_invalid_argument());
    }
}

TEST(GeoMetadataTest, MissingAndRenamedGeoColumnsCannotBecomeNulls) {
    tparquet::SchemaElement id;
    id.__set_name("id");
    id.__set_field_id(2);
    id.__set_type(tparquet::Type::INT32);
    id.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    SchemaDescriptor without_shape;
    ASSERT_TRUE(without_shape.from_thrift(schema_of(id), true).ok());
    auto lake_field = lake_geo();
    lake_field.__set_name("renamed_shape");
    TIcebergSchema lake;
    lake.__set_fields({lake_field});
    SlotDescriptor slot(1, "renamed_shape", TypeDescriptor::create_varbinary_type(1024));
    EXPECT_TRUE(validate_geo_scan(without_shape, &lake, {{0, &slot, true}}, true).is_not_supported());
    SlotDescriptor supported(2, "id", TypeDescriptor(TYPE_INT));
    EXPECT_TRUE(validate_geo_scan(without_shape, &lake, {{0, &supported, true}}, true).ok());
    auto element = geo_element();
    element.__isset.field_id = false;
    element.__set_name("RENAMED_SHAPE");
    SchemaDescriptor by_name;
    ASSERT_TRUE(by_name.from_thrift(schema_of(element), false).ok());
    EXPECT_TRUE(validate_geo_scan(by_name, &lake, {{0, &slot, true}}, false).is_not_supported());
    lake.fields[0].geo_metadata.__set_crs("EPSG:4326");
    EXPECT_TRUE(validate_geo_scan(by_name, &lake, {}, false).is_invalid_argument());
}

TEST(GeoMetadataTest, UnknownWireAlgorithmIsNotDefaulted) {
    auto element = geo_element();
    element.logicalType.GEOGRAPHY.__set_algorithm(static_cast<tparquet::EdgeInterpolationAlgorithm::type>(127));
    auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TCompactProtocol protocol(buffer);
    element.write(&protocol);
    tparquet::SchemaElement decoded;
    decoded.read(&protocol);
    ASSERT_EQ(127, decoded.logicalType.GEOGRAPHY.algorithm);
    SchemaDescriptor schema;
    ASSERT_TRUE(schema.from_thrift(schema_of(decoded), true).ok());
    EXPECT_FALSE(validate_geo_field(*schema.get_stored_column_by_field_idx(0), nullptr, true).ok());
}

TEST(GeoMetadataTest, StandardLogicalAnnotationFieldNumbers) {
    // Encode the external contract independently of generated tparquet writers.
    // LogicalType GEOMETRY=17, GEOGRAPHY=18; CRS=1 and geography algorithm=2.
    for (int16_t kind : {17, 18}) {
        auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        apache::thrift::protocol::TCompactProtocol protocol(buffer);
        using namespace apache::thrift::protocol;
        protocol.writeStructBegin("LogicalType");
        protocol.writeFieldBegin("geo", T_STRUCT, kind);
        protocol.writeStructBegin("Geo");
        protocol.writeFieldBegin("crs", T_STRING, 1);
        protocol.writeString("EPSG:3857");
        protocol.writeFieldEnd();
        if (kind == 18) {
            protocol.writeFieldBegin("algorithm", T_I32, 2);
            protocol.writeI32(4); // KARNEY
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
            EXPECT_EQ("EPSG:3857", decoded.GEOMETRY.crs);
        } else {
            ASSERT_TRUE(decoded.__isset.GEOGRAPHY);
            EXPECT_EQ("EPSG:3857", decoded.GEOGRAPHY.crs);
            EXPECT_EQ(tparquet::EdgeInterpolationAlgorithm::KARNEY, decoded.GEOGRAPHY.algorithm);
        }
    }
}

TEST(GeoMetadataTest, ProjectionRejectedBeforePruningOrMissingColumnSubstitution) {
    for (bool with_annotation : {true, false}) {
        auto element = geo_element();
        element.__isset.logicalType = with_annotation;
        SchemaDescriptor schema;
        ASSERT_TRUE(schema.from_thrift(schema_of(element), true).ok());
        TIcebergSchema lake;
        lake.__set_fields({lake_geo()});
        SlotDescriptor slot(1, "shape", TypeDescriptor::create_varbinary_type(1024));
        std::vector<FormatColumnInfo> columns{{0, &slot, true}};
        EXPECT_FALSE(validate_geo_scan(schema, &lake, columns, true).ok());
        // Unprojected valid geo metadata must not block supported-column scans.
        EXPECT_TRUE(validate_geo_scan(schema, &lake, {}, true).ok());
        if (with_annotation)
            EXPECT_FALSE(validate_geo_scan(schema, nullptr, columns, true).ok());
        else
            EXPECT_TRUE(validate_geo_scan(schema, nullptr, columns, true).ok());
    }
}

TEST(GeoMetadataTest, AnnotatedFileReadsSupportedColumnButRejectsGeo) {
    MemTracker tracker{-1, "geo_metadata_test"};
    CurrentThread::set_mem_tracker_source([] { return true; }, []() -> MemTracker* { return nullptr; });
    DeferOp reset_tracker_source([] { CurrentThread::set_mem_tracker_source(nullptr, nullptr); });
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&tracker);
    for (int variant = 0; variant < 4; ++variant) {
        auto bytes = geo_file(variant % 2 == 0, variant >= 2);
        const auto size = bytes.size();
        RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(bytes)), "geo-fixture.parquet");
        FormatScannerStats stats;
        FormatScanContext context;
        context.stats = &stats;
        context.timezone = "UTC";
        std::atomic<int32_t> lazy_coalesce_counter{0};
        context.lazy_column_coalesce_counter = &lazy_coalesce_counter;
        PredicateTree predicates;
        context.predicate_tree = &predicates;
        TSlotDescriptor output_slot;
        output_slot.__set_id(2);
        output_slot.__set_colName("id");
        output_slot.__set_slotType(TypeDescriptor(TYPE_INT).to_thrift());
        output_slot.__set_col_unique_id(-1);
        output_slot.__set_isMaterialized(true);
        output_slot.__set_isOutputColumn(true);
        output_slot.__set_isNullable(true);
        SlotDescriptor id(output_slot);
        context.materialized_columns = {{0, &id, true}};
        FileReader reader(1024, &file, size);
        auto status = reader.init(&context);
        ASSERT_TRUE(status.ok()) << status;
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(id.type(), true), id.id());
        status = reader.get_next(&chunk);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(1, chunk->num_rows());
        EXPECT_EQ(42, chunk->get_column_by_index(0)->get(0).get_int32());

        SlotDescriptor shape(1, "shape", TypeDescriptor::create_varbinary_type(1024));
        context.materialized_columns = {{0, &shape, true}};
        context.options.parquet_bloom_filter_enable = true;
        // An ordinary byte equality would prune against the deliberately wrong
        // min/max or dictionary. It must never be evaluated for native geo.
        std::unique_ptr<ColumnPredicate> predicate(
                new_column_eq_predicate(get_type_info(TYPE_VARCHAR), shape.id(), "not-in-dictionary"));
        PredicateAndNode root;
        root.add_child(PredicateColumnNode(predicate.get()));
        predicates = PredicateTree::create(std::move(root));
        FileReader unsupported(1024, &file, size);
        status = unsupported.init(&context);
        EXPECT_TRUE(status.is_not_supported()) << status;
        EXPECT_EQ(0, stats.bloom_filter_tried_counter);
        EXPECT_EQ(0, stats.statistics_tried_counter);
    }
}
TEST(GeoMetadataTest, UnannotatedWkbRemainsOrdinaryBinary) {
    MemTracker tracker{-1, "geo_binary_control"};
    CurrentThread::set_mem_tracker_source([] { return true; }, []() -> MemTracker* { return nullptr; });
    DeferOp reset_tracker_source([] { CurrentThread::set_mem_tracker_source(nullptr, nullptr); });
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&tracker);
    for (bool dictionary : {false, true}) {
        auto bytes = geo_file(true, dictionary, false);
        const auto size = bytes.size();
        RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(bytes)), "binary-control.parquet");
        FormatScannerStats stats;
        FormatScanContext context;
        context.stats = &stats;
        context.timezone = "UTC";
        std::atomic<int32_t> lazy_coalesce_counter{0};
        context.lazy_column_coalesce_counter = &lazy_coalesce_counter;
        PredicateTree predicates;
        context.predicate_tree = &predicates;
        TSlotDescriptor output_slot;
        output_slot.__set_id(1);
        output_slot.__set_colName("shape");
        output_slot.__set_slotType(TypeDescriptor::create_varbinary_type(1024).to_thrift());
        output_slot.__set_col_unique_id(-1);
        output_slot.__set_isMaterialized(true);
        output_slot.__set_isOutputColumn(true);
        output_slot.__set_isNullable(true);
        SlotDescriptor shape(output_slot);
        context.materialized_columns = {{0, &shape, true}};
        FileReader reader(1024, &file, size);
        auto status = reader.init(&context);
        ASSERT_TRUE(status.ok()) << status;
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(shape.type(), true), shape.id());
        status = reader.get_next(&chunk);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(1, chunk->num_rows());
        std::string expected(1, '\x01');
        put_fixed32_le(&expected, 1);
        expected.append(16, '\0');
        EXPECT_EQ(expected, chunk->get_column_by_index(0)->get(0).get_slice().to_string());
    }
}
} // namespace starrocks::parquet
