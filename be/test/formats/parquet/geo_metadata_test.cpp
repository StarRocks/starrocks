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
Status validate_scan(const std::vector<tparquet::SchemaElement>& elements, const TIcebergSchema* lake,
                     const std::vector<FormatColumnInfo>& columns, bool case_sensitive,
                     const std::vector<ColumnAccessPathPtr>* paths = nullptr) {
    tparquet::FileMetaData thrift;
    thrift.__set_schema(elements);
    FileMetaData metadata;
    RETURN_IF_ERROR(metadata.init(thrift, case_sensitive));
    std::vector<GroupReaderParam::Column> read_columns;
    std::unordered_set<std::string> names;
    if (lake != nullptr && metadata.schema().exist_filed_id()) {
        return LakeMetaHelper(&metadata, case_sensitive, lake)
                .prepare_read_columns(columns, paths, read_columns, names);
    }
    return ParquetMetaHelper(&metadata, case_sensitive).prepare_read_columns(columns, paths, read_columns, names);
}

Status validate_scan(const SchemaDescriptor& schema, const TIcebergSchema* lake,
                     const std::vector<FormatColumnInfo>& columns, bool case_sensitive,
                     const std::vector<ColumnAccessPathPtr>* paths = nullptr) {
    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(schema.get_fields_size());
    std::vector<tparquet::SchemaElement> elements{root};
    for (const auto& field : schema.get_parquet_fields()) elements.push_back(field.schema_element);
    return validate_scan(elements, lake, columns, case_sensitive, paths);
}

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
            // Deliberately misleading bounds must not hide an Iceberg/Parquet metadata conflict.
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
            // Invalid bloom location: metadata conflict must be reported before reading it.
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

TEST(GeoMetadataTest, WideOrdinarySchemaReadsOnlySelectedColumn) {
    tparquet::FileMetaData thrift;
    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(2048);
    thrift.schema.push_back(root);
    TIcebergSchema lake;
    for (int i = 0; i < 2048; ++i) {
        auto element = geo_element();
        element.__isset.logicalType = false;
        element.__set_name("column_" + std::to_string(i));
        element.__set_field_id(i + 1);
        thrift.schema.push_back(element);
        TIcebergSchemaField field;
        field.__set_name(element.name);
        field.__set_field_id(element.field_id);
        lake.fields.push_back(field);
    }
    FileMetaData metadata;
    ASSERT_TRUE(metadata.init(thrift, true).ok());
    SlotDescriptor slot(1, "column_1024", TypeDescriptor::create_varbinary_type(1024));
    LakeMetaHelper helper(&metadata, true, &lake);
    for (int file = 0; file < 100; ++file) {
        std::vector<GroupReaderParam::Column> read_columns;
        std::unordered_set<std::string> names;
        ASSERT_TRUE(helper.prepare_read_columns({{0, &slot, true}}, nullptr, read_columns, names).ok());
        ASSERT_EQ(1, read_columns.size());
        EXPECT_EQ(1024, read_columns[0].idx_in_parquet);
        EXPECT_EQ(std::unordered_set<std::string>{"column_1024"}, names);
    }
}

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
        EXPECT_TRUE(field->schema_element.logicalType.__isset.GEOGRAPHY);
        EXPECT_EQ(algorithm, field->schema_element.logicalType.GEOGRAPHY.algorithm);
        EXPECT_EQ("EPSG:4326", field->schema_element.logicalType.GEOGRAPHY.crs);
    }
}

TEST(GeoMetadataTest, AnnotationValidationOnlyForSelectedGeo) {
    const auto validate = [](const tparquet::SchemaElement& element) {
        SchemaDescriptor schema;
        auto status = schema.from_thrift(schema_of(element), true);
        if (!status.ok()) return status;
        return validate_geo_field(*schema.get_stored_column_by_field_idx(0), nullptr);
    };
    for (auto physical : {tparquet::Type::INT32, tparquet::Type::FIXED_LEN_BYTE_ARRAY}) {
        auto element = geo_element();
        element.__set_type(physical);
        EXPECT_TRUE(validate(element).is_invalid_argument());
    }
    auto element = geo_element();
    element.logicalType.__set_GEOMETRY(tparquet::GeometryType());
    EXPECT_TRUE(validate(element).is_invalid_argument());
    element = geo_element();
    element.logicalType.GEOGRAPHY.__set_crs("");
    EXPECT_TRUE(validate(element).is_invalid_argument());
    element = geo_element();
    element.__set_converted_type(tparquet::ConvertedType::UTF8);
    EXPECT_TRUE(validate(element).is_invalid_argument());

    // Metadata parsing does not run geo semantic checks on unselected fields.
    SchemaDescriptor schema;
    ASSERT_TRUE(schema.from_thrift(schema_of(element), true).ok());
    EXPECT_TRUE(validate_scan(schema, nullptr, {}, true).ok());
    SlotDescriptor slot(1, "shape", TypeDescriptor::create_varbinary_type(1024));
    EXPECT_TRUE(validate_scan(schema, nullptr, {{0, &slot, true}}, true).ok());
    TIcebergSchema lake;
    lake.__set_fields({lake_geo()});
    EXPECT_TRUE(validate_scan(schema, &lake, {{0, &slot, true}}, true).is_invalid_argument());
    // Plain Parquet matching does not introduce a new geo-specific type policy.
    element = geo_element();
    element.logicalType.__set_STRING(tparquet::StringType());
    SchemaDescriptor mixed;
    ASSERT_TRUE(mixed.from_thrift(schema_of(element), true).ok());
    EXPECT_TRUE(validate_scan(mixed, nullptr, {{0, &slot, true}}, true).ok());
}

TEST(GeoMetadataTest, IcebergConflictsAndUnannotatedFallback) {
    for (bool geography : {true, false}) {
        SchemaDescriptor schema;
        ASSERT_TRUE(schema.from_thrift(schema_of(geo_element(geography)), true).ok());
        const auto& field = *schema.get_stored_column_by_field_idx(0);
        auto lake = geography ? lake_geo() : lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR");
        EXPECT_TRUE(validate_geo_field(field, &lake).ok());
        auto opposite = geography ? lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR") : lake_geo();
        EXPECT_TRUE(validate_geo_field(field, &opposite).is_invalid_argument());
        lake.geo_metadata.__set_crs("EPSG:4326");
        EXPECT_FALSE(validate_geo_field(field, &lake).ok());
        if (geography) {
            lake = lake_geo(TIcebergGeoKind::GEOGRAPHY, "KARNEY");
            EXPECT_TRUE(validate_geo_field(field, &lake).is_invalid_argument());
        }
        // An older FE without geo metadata cannot prove a binary/geo conflict.
        TIcebergSchemaField old_fe;
        EXPECT_TRUE(validate_geo_field(field, &old_fe).ok());
    }
    auto element = geo_element();
    element.__isset.logicalType = false;
    SchemaDescriptor unannotated;
    ASSERT_TRUE(unannotated.from_thrift(schema_of(element), true).ok());
    auto lake = lake_geo();
    EXPECT_TRUE(validate_geo_field(*unannotated.get_stored_column_by_field_idx(0), &lake).ok());
    EXPECT_FALSE(unannotated.get_stored_column_by_field_idx(0)->schema_element.__isset.logicalType);

    element.__set_type(tparquet::Type::INT32);
    SchemaDescriptor wrong_physical;
    ASSERT_TRUE(wrong_physical.from_thrift(schema_of(element), true).ok());
    EXPECT_FALSE(validate_geo_field(*wrong_physical.get_stored_column_by_field_idx(0), &lake).ok());
}

TEST(GeoMetadataTest, GeoValidationDoesNotRecurseIntoContainers) {
    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(1);
    tparquet::SchemaElement group;
    group.__set_name("container");
    group.__set_num_children(1);
    group.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    SchemaDescriptor schema;
    ASSERT_TRUE(schema.from_thrift({root, group, geo_element()}, true).ok());
    const auto& container = *schema.get_stored_column_by_field_idx(0);
    // Scalar validation has no recursive container responsibility.
    EXPECT_TRUE(validate_geo_field(container, nullptr).ok());
    auto lake = lake_geo();
    EXPECT_TRUE(validate_geo_field(container.children[0], &lake).ok());
}

TEST(GeoMetadataTest, UnannotatedWkbUsesSourceGeoSemantics) {
    auto element = geo_element();
    element.__isset.logicalType = false;
    SchemaDescriptor schema;
    ASSERT_TRUE(schema.from_thrift(schema_of(element), true).ok());
    const auto& field = *schema.get_stored_column_by_field_idx(0);
    for (const auto& edge : {"SPHERICAL", "VINCENTY", "THOMAS", "ANDOYER", "KARNEY"}) {
        auto lake = lake_geo(TIcebergGeoKind::GEOGRAPHY, edge);
        lake.geo_metadata.__set_crs("EPSG:4326");
        EXPECT_TRUE(validate_geo_field(field, &lake).ok());
    }
    auto lake = lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR");
    lake.geo_metadata.__set_crs("EPSG:3857");
    EXPECT_TRUE(validate_geo_field(field, &lake).ok());
    // The trusted source descriptor still cannot agree with a text-annotated file.
    element.__set_converted_type(tparquet::ConvertedType::UTF8);
    SchemaDescriptor text_schema;
    ASSERT_TRUE(text_schema.from_thrift(schema_of(element), true).ok());
    EXPECT_TRUE(validate_geo_field(*text_schema.get_stored_column_by_field_idx(0), &lake).is_invalid_argument());
    element.__isset.converted_type = false;
    tparquet::LogicalType logical;
    logical.__set_STRING(tparquet::StringType());
    element.__set_logicalType(logical);
    SchemaDescriptor logical_text_schema;
    ASSERT_TRUE(logical_text_schema.from_thrift(schema_of(element), true).ok());
    EXPECT_TRUE(
            validate_geo_field(*logical_text_schema.get_stored_column_by_field_idx(0), &lake).is_invalid_argument());
}

TEST(GeoMetadataTest, EveryGeographyAlgorithmMatchesWithoutEnablingCompute) {
    const std::vector<std::string> algorithms{"SPHERICAL", "VINCENTY", "THOMAS", "ANDOYER", "KARNEY"};
    for (int i = 0; i < algorithms.size(); ++i) {
        auto element = geo_element();
        element.logicalType.GEOGRAPHY.__set_algorithm(static_cast<tparquet::EdgeInterpolationAlgorithm::type>(i));
        SchemaDescriptor schema;
        ASSERT_TRUE(schema.from_thrift(schema_of(element), true).ok());
        auto lake = lake_geo(TIcebergGeoKind::GEOGRAPHY, algorithms[i]);
        EXPECT_TRUE(validate_geo_field(*schema.get_stored_column_by_field_idx(0), &lake).ok());
        lake.geo_metadata.__set_edge_algorithm(algorithms[(i + 1) % algorithms.size()]);
        EXPECT_TRUE(validate_geo_field(*schema.get_stored_column_by_field_idx(0), &lake).is_invalid_argument());
    }
}

TEST(GeoMetadataTest, MissingAndNoIdColumnsKeepExistingMatching) {
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
    EXPECT_TRUE(validate_scan(without_shape, &lake, {{0, &slot, true}}, true).ok());
    SlotDescriptor supported(2, "id", TypeDescriptor(TYPE_INT));
    EXPECT_TRUE(validate_scan(without_shape, &lake, {{0, &supported, true}}, true).ok());
    auto element = geo_element();
    element.__isset.field_id = false;
    element.__set_name("RENAMED_SHAPE");
    SchemaDescriptor by_name;
    ASSERT_TRUE(by_name.from_thrift(schema_of(element), false).ok());
    EXPECT_TRUE(validate_scan(by_name, &lake, {{0, &slot, true}}, false).ok());
    lake.fields[0].geo_metadata.__set_crs("EPSG:4326");
    EXPECT_TRUE(validate_scan(by_name, &lake, {{0, &slot, true}}, false).ok());
    EXPECT_TRUE(validate_scan(by_name, &lake, {}, false).ok());
}

TEST(GeoMetadataTest, NestedGeoUsesSelectedFieldTraversal) {
    // Binary slots exercise the metadata contract without enabling native GEO in FE.
    const auto binary = TypeDescriptor::create_varbinary_type(1024);
    for (bool geography : {true, false}) {
        for (int container = 0; container < 3; ++container) {
            SCOPED_TRACE(::testing::Message() << "geography=" << geography << ", container=" << container);
            auto shape = geo_element(geography);
            auto id = shape;
            id.__set_name("id");
            id.__set_field_id(2);
            id.__set_type(tparquet::Type::INT32);
            id.__isset.logicalType = false;
            auto record = shape;
            record.__set_name("payload");
            record.__set_field_id(4);
            record.__set_num_children(2);
            record.__isset.type = false;
            record.__isset.logicalType = false;
            auto root = record;
            root.__set_name("root");
            root.__set_num_children(1);
            root.__isset.field_id = false;
            root.__isset.repetition_type = false;

            auto source_shape = geography ? lake_geo() : lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR");
            source_shape.__set_name("renamed_shape");
            TIcebergSchemaField source_id;
            source_id.__set_field_id(2);
            source_id.__set_name("id");
            TIcebergSchemaField source_record;
            source_record.__set_field_id(4);
            source_record.__set_name("payload");
            source_record.__set_children({source_id, source_shape});
            auto selected =
                    TypeDescriptor::create_struct_type({"id", "renamed_shape"}, {TypeDescriptor(TYPE_INT), binary});
            auto pruned = TypeDescriptor::create_struct_type({"id"}, {TypeDescriptor(TYPE_INT)});
            auto source = source_record;
            std::vector<tparquet::SchemaElement> elements{root, record, id, shape};
            if (container != 0) {
                const bool array = container == 1;
                auto outer = record;
                outer.__set_field_id(5);
                outer.__set_num_children(1);
                outer.__set_converted_type(array ? tparquet::ConvertedType::LIST : tparquet::ConvertedType::MAP);
                auto repeated = record;
                repeated.__set_name(array ? "list" : "key_value");
                repeated.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
                repeated.__set_num_children(array ? 1 : 2);
                repeated.__isset.field_id = false;
                record.__set_name(array ? "element" : "value");
                source_record.__set_name(record.name);
                source.__set_field_id(5);
                elements = {root, outer, repeated};
                if (array) {
                    source.__set_children({source_record});
                    selected = TypeDescriptor::create_array_type(selected);
                    pruned = TypeDescriptor::create_array_type(pruned);
                } else {
                    auto key = id;
                    key.__set_name("key");
                    key.__set_field_id(6);
                    key.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
                    elements.push_back(key);
                    auto source_key = source_id;
                    source_key.__set_field_id(6);
                    source_key.__set_name("key");
                    source.__set_children({source_key, source_record});
                    selected = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), selected);
                    pruned = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), pruned);
                }
                elements.insert(elements.end(), {record, id, shape});
            }
            TIcebergSchema lake;
            lake.__set_fields({source});
            SlotDescriptor slot(1, "payload", selected);
            auto status = validate_scan(elements, &lake, {{0, &slot, true}}, true);
            ASSERT_TRUE(status.ok()) << status;

            auto& nested_source = container == 0 ? lake.fields[0] : lake.fields[0].children[container - 1];
            nested_source.children[1].geo_metadata.__set_crs("EPSG:4326");
            // The readable INT sibling must not short-circuit the GEO comparison.
            EXPECT_TRUE(validate_scan(elements, &lake, {{0, &slot, true}}, true).is_invalid_argument());
            SlotDescriptor projected_id(1, "payload", pruned);
            EXPECT_TRUE(validate_scan(elements, &lake, {{0, &projected_id, true}}, true).ok());
            EXPECT_TRUE(validate_scan(elements, &lake, {}, true).ok());
            if (container == 2) {
                SlotDescriptor keys_only(
                        1, "payload",
                        TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_UNKNOWN)));
                EXPECT_TRUE(validate_scan(elements, &lake, {{0, &keys_only, true}}, true).ok());
            }

            auto complex_elements = elements;
            complex_elements.back().__isset.type = false;
            complex_elements.back().__isset.logicalType = false;
            complex_elements.back().__set_num_children(1);
            auto child = id;
            child.__set_field_id(7);
            complex_elements.push_back(child);
            EXPECT_TRUE(validate_scan(complex_elements, &lake, {{0, &slot, true}}, true).is_invalid_argument());
            EXPECT_TRUE(validate_scan(complex_elements, &lake, {{0, &projected_id, true}}, true).ok());

            // A genuinely absent nested field remains eligible for schema-evolution defaults.
            elements.pop_back();
            elements[elements.size() - 2].__set_num_children(1);
            EXPECT_TRUE(validate_scan(elements, &lake, {{0, &slot, true}}, true).ok());
        }
    }
}

TEST(GeoMetadataTest, GeoSourceCannotBeDiscardedAsMissingWhenFileContainsGroup) {
    auto group = geo_element();
    group.__isset.type = false;
    group.__isset.logicalType = false;
    group.__set_num_children(1);
    auto child = geo_element();
    child.__set_field_id(2);
    child.__set_name("child");
    auto elements = schema_of(group);
    elements.push_back(child);
    TIcebergSchema lake;
    lake.__set_fields({lake_geo()});
    SlotDescriptor slot(1, "shape", TypeDescriptor::create_varbinary_type(1024));
    EXPECT_TRUE(validate_scan(elements, &lake, {{0, &slot, true}}, true).is_invalid_argument());

    // Exercise FileReader's error path even when there are no row-group readers.
    tparquet::FileMetaData metadata;
    metadata.__set_version(1);
    metadata.__set_schema(elements);
    metadata.__set_num_rows(0);
    metadata.__set_row_groups({});
    auto footer = compact_bytes(metadata);
    std::string bytes = "PAR1" + footer;
    put_fixed32_le(&bytes, footer.size());
    bytes += "PAR1";
    const auto size = bytes.size();
    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(bytes)), "geo-group.parquet");
    FormatScannerStats stats;
    FormatScanContext context;
    context.stats = &stats;
    context.lake_schema = &lake;
    context.materialized_columns = {{0, &slot, true}};
    FileReader reader(1024, &file, size);
    auto status = reader.init(&context);
    EXPECT_TRUE(status.is_invalid_argument()) << status;
    EXPECT_TRUE(context.not_existed_slots.empty());
    EXPECT_EQ(0, stats.statistics_tried_counter);
    EXPECT_EQ(0, stats.bloom_filter_tried_counter);
}

TEST(GeoMetadataTest, ProjectionMappingPreservesPhysicalNamesAndIds) {
    auto element = geo_element();
    element.__set_name("old_shape");
    TIcebergSchema lake;
    auto source = lake_geo();
    source.__set_name("shape");
    source.geo_metadata.__set_crs("EPSG:4326");
    lake.__set_fields({source});
    TSlotDescriptor thrift_slot;
    thrift_slot.__set_id(1);
    thrift_slot.__set_colName("shape");
    thrift_slot.__set_col_physical_name("old_shape");
    thrift_slot.__set_col_unique_id(-1);
    thrift_slot.__set_slotType(TypeDescriptor::create_varbinary_type(1024).to_thrift());
    SlotDescriptor slot(thrift_slot);
    for (bool with_ids : {true, false}) {
        element.__isset.field_id = with_ids;
        SchemaDescriptor schema;
        ASSERT_TRUE(schema.from_thrift(schema_of(element), true).ok());
        auto status = validate_scan(schema, &lake, {{0, &slot, true}}, true);
        EXPECT_EQ(with_ids, status.is_invalid_argument());
        EXPECT_EQ(!with_ids, status.ok());
    }

    // With no field IDs, ordinary columns keep the original physical-name
    // fallback, even when an older/incomplete lake schema has no matching field.
    element.__isset.logicalType = false;
    tparquet::FileMetaData thrift;
    thrift.__set_schema(schema_of(element));
    FileMetaData metadata;
    ASSERT_TRUE(metadata.init(thrift, true).ok());
    lake.fields.clear();
    ParquetMetaHelper helper(&metadata, true);
    std::vector<GroupReaderParam::Column> columns;
    std::unordered_set<std::string> names;
    ASSERT_TRUE(helper.prepare_read_columns({{0, &slot, true}}, nullptr, columns, names).ok());
    ASSERT_EQ(1, columns.size());
    EXPECT_EQ(0, columns[0].idx_in_parquet);
    EXPECT_EQ(nullptr, columns[0].t_lake_schema_field);
    EXPECT_EQ(std::unordered_set<std::string>{"shape"}, names);
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
    EXPECT_FALSE(validate_geo_field(*schema.get_stored_column_by_field_idx(0), nullptr).ok());
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

TEST(GeoMetadataTest, MatchingMetadataDoesNotDisableExistingReads) {
    for (bool with_annotation : {true, false}) {
        auto element = geo_element();
        element.__isset.logicalType = with_annotation;
        SchemaDescriptor schema;
        ASSERT_TRUE(schema.from_thrift(schema_of(element), true).ok());
        TIcebergSchema lake;
        lake.__set_fields({lake_geo()});
        SlotDescriptor slot(1, "shape", TypeDescriptor::create_varbinary_type(1024));
        std::vector<FormatColumnInfo> columns{{0, &slot, true}};
        EXPECT_TRUE(validate_scan(schema, &lake, columns, true).ok());
        // Unprojected valid geo metadata must not block supported-column scans.
        EXPECT_TRUE(validate_scan(schema, &lake, {}, true).ok());
        EXPECT_TRUE(validate_scan(schema, nullptr, columns, true).ok());
        lake.fields[0].geo_metadata.__set_crs("EPSG:4326");
        EXPECT_EQ(with_annotation, validate_scan(schema, &lake, columns, true).is_invalid_argument());
    }
}

TEST(GeoMetadataTest, MetadataConflictFailsBeforePruning) {
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
        TIcebergSchema lake;
        auto source = variant % 2 == 0 ? lake_geo() : lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR");
        source.geo_metadata.__set_crs("EPSG:4326");
        lake.__set_fields({source});
        context.lake_schema = &lake;
        // A metadata conflict must not be hidden by ordinary byte pruning.
        std::unique_ptr<ColumnPredicate> predicate(
                new_column_eq_predicate(get_type_info(TYPE_VARCHAR), shape.id(), "not-in-dictionary"));
        PredicateAndNode root;
        root.add_child(PredicateColumnNode(predicate.get()));
        predicates = PredicateTree::create(std::move(root));
        FileReader unsupported(1024, &file, size);
        status = unsupported.init(&context);
        EXPECT_TRUE(status.is_invalid_argument()) << status;
        EXPECT_EQ(0, stats.bloom_filter_tried_counter);
        EXPECT_EQ(0, stats.statistics_tried_counter);
    }
}
TEST(GeoMetadataTest, AnnotatedAndUnannotatedWkbKeepExistingBinaryReads) {
    MemTracker tracker{-1, "geo_binary_control"};
    CurrentThread::set_mem_tracker_source([] { return true; }, []() -> MemTracker* { return nullptr; });
    DeferOp reset_tracker_source([] { CurrentThread::set_mem_tracker_source(nullptr, nullptr); });
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&tracker);
    for (int variant = 0; variant < 6; ++variant) {
        const bool geography = variant % 3 != 1;
        auto bytes = geo_file(geography, variant >= 3, variant % 3 != 2);
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
        TIcebergSchema lake;
        lake.__set_fields({geography ? lake_geo() : lake_geo(TIcebergGeoKind::GEOMETRY, "PLANAR")});
        context.lake_schema = &lake;
        FileReader lake_reader(1024, &file, size);
        ASSERT_TRUE(lake_reader.init(&context).ok());
        chunk->reset();
        ASSERT_TRUE(lake_reader.get_next(&chunk).ok());
        ASSERT_EQ(1, chunk->num_rows());
        EXPECT_EQ(expected, chunk->get_column_by_index(0)->get(0).get_slice().to_string());
        EXPECT_EQ(0, stats.statistics_tried_counter);
        EXPECT_EQ(0, stats.bloom_filter_tried_counter);
    }
}
} // namespace starrocks::parquet
