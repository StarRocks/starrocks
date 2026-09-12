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

#include "column/geo_column.h"

#include <gtest/gtest.h>

#include <stdexcept>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/nullable_column.h"

namespace starrocks {
namespace {

GeoColumnDescriptor descriptor(GeoLogicalTypePB kind = GEO_LOGICAL_TYPE_GEOGRAPHY) {
    GeoColumnDescriptor desc;
    desc.type.logical_type = kind;
    desc.type.coordinate_system =
            kind == GEO_LOGICAL_TYPE_GEOGRAPHY ? GEO_COORDINATE_SYSTEM_SPHERICAL : GEO_COORDINATE_SYSTEM_CARTESIAN;
    desc.type.edge_algorithm =
            kind == GEO_LOGICAL_TYPE_GEOGRAPHY ? GEO_EDGE_ALGORITHM_SPHERICAL : GEO_EDGE_ALGORITHM_PLANAR;
    desc.type.crs = "OGC:CRS84";
    desc.storage.encoding = GEO_ENCODING_WKB;
    return desc;
}
std::string point() {
    // Little-endian POINT (1 2).
    const unsigned char bytes[] = {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0x40};
    return std::string(reinterpret_cast<const char*>(bytes), sizeof(bytes));
}
std::string empty_collection() {
    return std::string("\x01\x07\0\0\0\0\0\0\0", 9);
}

TEST(GeoColumnTest, RawBytesAndDistinctSemantics) {
    const auto bytes = point();
    for (auto kind : {GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_LOGICAL_TYPE_GEOMETRY}) {
        auto column = GeoColumn::create(descriptor(kind));
        column->append_wkb(Slice(bytes));
        EXPECT_EQ(bytes, column->get_wkb(0).to_string());
        EXPECT_EQ(kind, column->descriptor().type.logical_type);
        EXPECT_FALSE(column->is_binary());
        EXPECT_FALSE(column->has_wkb_cache());
        auto info = column->inspect_wkb(0);
        ASSERT_TRUE(info.ok());
        EXPECT_EQ(1, info->geometry_type);
        EXPECT_FALSE(info->empty);
        EXPECT_TRUE(column->has_wkb_cache());
        EXPECT_EQ(GEO_VALIDATION_STATE_UNKNOWN, column->descriptor().storage.validation_state);
    }
}

TEST(GeoColumnTest, BatchIngestionAndCopyOnWrite) {
    auto column = GeoColumn::create(descriptor());
    const auto p = point();
    const auto e = empty_collection();
    const Slice values[] = {Slice(p), Slice(e), Slice(p)};
    column->append_wkb_batch(values, 3);
    ASSERT_TRUE(column->inspect_wkb(0).ok());
    ColumnPtr shared = column;
    auto changed = Column::mutate(shared);
    auto& changed_geo = static_cast<GeoColumn&>(*changed);
    EXPECT_FALSE(changed_geo.has_wkb_cache());
    changed_geo.append_wkb_batch(values, 3);
    EXPECT_EQ(3, column->size());
    EXPECT_EQ(6, changed_geo.size());
    EXPECT_TRUE(column->has_wkb_cache());
    EXPECT_EQ(e, changed_geo.get_wkb(4).to_string());
    column->reserve(100, 8192); // Cached info owns no pointers to the reallocated bytes.
    EXPECT_TRUE(column->inspect_wkb(0).ok());
    column->append_wkb_batch(values, 3);
    EXPECT_FALSE(column->has_wkb_cache());
}

TEST(GeoColumnTest, NullAndEmptyAreDifferent) {
    auto geo = GeoColumn::create(descriptor());
    geo->append_wkb(Slice(empty_collection()));
    geo->append_default();
    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);
    auto nullable = NullableColumn::create(std::move(geo), std::move(nulls));
    EXPECT_FALSE(nullable->is_null(0));
    EXPECT_TRUE(nullable->is_null(1));
    const auto& data = static_cast<const GeoColumn&>(*nullable->data_column());
    EXPECT_EQ(empty_collection(), data.get_wkb(0).to_string());
    EXPECT_EQ(0, data.get_wkb(1).size);
    auto copy = nullable->clone_empty();
    const uint32_t indexes[] = {1, 0};
    copy->append_selective(*nullable, indexes, 0, 2);
    EXPECT_TRUE(copy->is_null(0));
    EXPECT_FALSE(copy->is_null(1));
    EXPECT_EQ(empty_collection(), copy->get(1).get_slice().to_string());
    EXPECT_EQ(descriptor(),
              static_cast<const GeoColumn&>(*static_cast<const NullableColumn&>(*copy).data_column()).descriptor());
}

TEST(GeoColumnTest, CopyFilterReplicateAndConst) {
    auto column = GeoColumn::create(descriptor());
    column->append_wkb(Slice(point()));
    column->append_wkb(Slice(empty_collection()));
    auto selected = column->clone_empty();
    const uint32_t indexes[] = {1, 0, 1};
    selected->append_selective(*column, indexes, 0, 3);
    EXPECT_EQ(empty_collection(), selected->get(0).get_slice().to_string());
    EXPECT_EQ(point(), selected->get(1).get_slice().to_string());
    Filter keep = {0, 1, 1};
    selected->filter(keep);
    EXPECT_EQ(2, selected->size());
    EXPECT_EQ(point(), selected->get(0).get_slice().to_string());
    Buffer<uint32_t> offsets = {0, 2, 3};
    auto replicated = selected->replicate(offsets);
    ASSERT_TRUE(replicated.ok());
    EXPECT_EQ(3, (*replicated)->size());
    EXPECT_EQ(point(), (*replicated)->get(1).get_slice().to_string());
    EXPECT_EQ(empty_collection(), (*replicated)->get(2).get_slice().to_string());
    EXPECT_EQ(descriptor(), static_cast<const GeoColumn&>(**replicated).descriptor());
    auto one = GeoColumn::create(descriptor());
    one->append_wkb(Slice(point()));
    auto constant = ConstColumn::create(std::move(one), 5);
    EXPECT_EQ(point(), constant->get(4).get_slice().to_string());
    EXPECT_EQ(5, constant->size());
}

TEST(GeoColumnTest, CacheOwnershipCloneAndBufferLifetime) {
    auto column = GeoColumn::create(descriptor());
    {
        auto bytes = point();
        column->append_wkb(Slice(bytes));
        bytes.assign(bytes.size(), 'x');
    }
    auto info = column->inspect_wkb(0);
    ASSERT_TRUE(info.ok());
    const auto usage = column->memory_usage();
    EXPECT_GT(usage, column->byte_size());
    EXPECT_TRUE(column->inspect_wkb(0).ok());
    EXPECT_EQ(usage, column->memory_usage()); // fixed-size cache, no growth per lookup
    auto copy = column->clone();
    EXPECT_FALSE(static_cast<const GeoColumn&>(*copy).has_wkb_cache());
    column->reset_column();
    EXPECT_FALSE(column->has_wkb_cache());
    EXPECT_EQ(point(), copy->get(0).get_slice().to_string());
    copy.reset();
    EXPECT_EQ(1, info->geometry_type); // no borrowed WKB pointers
}

TEST(GeoColumnTest, EveryMutationInvalidatesCache) {
    auto column = GeoColumn::create(descriptor());
    auto source = GeoColumn::create(descriptor());
    source->append_wkb(Slice(empty_collection()));
    auto prepare = [&] {
        column->reset_column();
        column->append_wkb(Slice(point()));
        ASSERT_TRUE(column->inspect_wkb(0).ok());
        ASSERT_TRUE(column->has_wkb_cache());
    };
    prepare();
    column->append_wkb(Slice(point()));
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->append(*source);
    EXPECT_FALSE(column->has_wkb_cache());
    const uint32_t index[] = {0};
    prepare();
    column->append_selective(*source, index, 0, 1);
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->append_value_multiple_times(*source, 0, 2);
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->append_default();
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->resize(0);
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->assign(2, 0);
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->remove_first_n_values(1);
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->fill_default(Filter{1});
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->filter(Filter{0});
    EXPECT_FALSE(column->has_wkb_cache());
    prepare();
    column->update_rows(*source, index);
    EXPECT_FALSE(column->has_wkb_cache());
    EXPECT_TRUE(column->inspect_wkb(0)->empty);
    auto other = GeoColumn::create(descriptor(GEO_LOGICAL_TYPE_GEOMETRY));
    other->append_wkb(Slice(empty_collection()));
    ASSERT_TRUE(other->inspect_wkb(0).ok());
    column->swap_column(*other);
    EXPECT_FALSE(column->has_wkb_cache());
    EXPECT_FALSE(other->has_wkb_cache());
    EXPECT_EQ(GEO_LOGICAL_TYPE_GEOMETRY, column->descriptor().type.logical_type);
}

TEST(GeoColumnTest, SelfAppendAndIndependentClone) {
    auto column = GeoColumn::create(descriptor());
    column->append_wkb(Slice(point()));
    column->append_wkb(column->get_wkb(0));
    column->append(*column);
    EXPECT_EQ(4, column->size());
    const uint32_t indexes[] = {3, 0};
    column->append_selective(*column, indexes, 0, 2);
    EXPECT_EQ(6, column->size());
    column->append_value_multiple_times(*column, 0, 2);
    auto slice = column->get_wkb(0);
    column->append_value_multiple_times(&slice, 2);
    EXPECT_EQ(10, column->size());
    for (size_t i = 0; i < column->size(); ++i) EXPECT_EQ(point(), column->get_wkb(i).to_string());
    auto copy = column->clone();
    column->reset_column();
    EXPECT_EQ(10, copy->size());
}

TEST(GeoColumnTest, CopyRejectsDescriptorChangesWithoutSqlCoercion) {
    auto column = GeoColumn::create(descriptor());
    auto other = GeoColumn::create(descriptor(GEO_LOGICAL_TYPE_GEOMETRY));
    other->append_wkb(Slice(point()));
    EXPECT_THROW(column->append(*other), std::invalid_argument);
    EXPECT_EQ(0, column->size());
    auto different = descriptor();
    different.storage.dimension = GEO_DIMENSION_XYZ;
    other = GeoColumn::create(different);
    other->append_wkb(Slice(point()));
    EXPECT_THROW(column->append(*other), std::invalid_argument);
    EXPECT_EQ(0, column->size());
    different.storage.encoding = GEO_ENCODING_UNKNOWN;
    EXPECT_THROW(GeoColumn::create(different), std::invalid_argument);
}

TEST(GeoColumnTest, BoundedInspectionDoesNotTrustProducerFlag) {
    auto desc = descriptor();
    desc.storage.validation_state = GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED;
    GeoWkbLimits limits;
    limits.max_bytes = 20;
    auto column = GeoColumn::create(desc, limits);
    column->append_wkb(Slice(point()));
    EXPECT_FALSE(column->inspect_wkb(0).ok());
    EXPECT_FALSE(column->has_wkb_cache());
    column->reset_column();
    column->append_wkb(Slice("invalid"));
    EXPECT_FALSE(column->inspect_wkb(0).ok());
    EXPECT_FALSE(column->inspect_wkb(1).ok());
    EXPECT_EQ("invalid", column->get_wkb(0).to_string());
}

TEST(GeoColumnTest, UnsupportedPathsDoNotFallBackToBinary) {
    auto column = GeoColumn::create(descriptor());
    column->append_wkb(Slice(point()));
    ColumnVisitor visitor;
    ColumnVisitorMutable mutable_visitor;
    EXPECT_THROW(column->accept(&visitor), std::runtime_error);
    EXPECT_THROW(column->accept_mutable(&mutable_visitor), std::runtime_error);
    EXPECT_THROW(column->immutable_data(), std::runtime_error);
    EXPECT_THROW(column->debug_item(0), std::runtime_error);
    EXPECT_THROW(RunTimeTypeLimits<TYPE_GEOGRAPHY>::min_value(), std::runtime_error);
    EXPECT_THROW(RunTimeTypeLimits<TYPE_GEOGRAPHY>::max_value(), std::runtime_error);
    EXPECT_THROW(RunTimeTypeLimits<TYPE_GEOMETRY>::min_value(), std::runtime_error);
    EXPECT_THROW(RunTimeTypeLimits<TYPE_GEOMETRY>::max_value(), std::runtime_error);
    EXPECT_THROW(column->compare_at(0, 0, *column, 1), std::runtime_error);
    EXPECT_THROW(column->serialize_size(0), std::runtime_error);
    EXPECT_THROW(column->deserialize_and_append(nullptr), std::runtime_error);
    EXPECT_THROW(column->put_mysql_row_buffer(nullptr, 0), std::runtime_error);
    EXPECT_FALSE(column->append_strings(nullptr, 0));
}

TEST(GeoColumnTest, GenericCreationCannotRelabelTypedPayload) {
    for (const auto primitive : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        const auto desc =
                descriptor(primitive == TYPE_GEOGRAPHY ? GEO_LOGICAL_TYPE_GEOGRAPHY : GEO_LOGICAL_TYPE_GEOMETRY);
        auto source = GeoColumn::create(desc);
        source->append_wkb(Slice(point()));
        auto destination = ColumnHelper::create_column(TypeDescriptor::create_geo_type(primitive, desc.type), false);
        EXPECT_THROW(destination->append(*source, 0, 1), std::invalid_argument);
        EXPECT_EQ(0, destination->size());
        auto copied = source->clone();
        EXPECT_EQ(desc, down_cast<const GeoColumn*>(copied.get())->descriptor());
        EXPECT_EQ(point(), down_cast<const GeoColumn*>(copied.get())->get_wkb(0).to_string());
    }
}

template <LogicalType Type>
void check_unsupported_scalar_viewers() {
    ColumnPtr plain = GeoColumn::create(1);
    ColumnPtr constant = ConstColumn::create(GeoColumn::create(1), 3);
    ColumnPtr nullable = NullableColumn::create(GeoColumn::create(1), NullColumn::create(1, 0));
    ColumnPtr nulls = ColumnHelper::create_const_null_column(3);
    for (const auto& column : {plain, constant, nullable, nulls}) {
        EXPECT_THROW((ColumnViewer<Type>(column)), std::runtime_error);
    }
}

TEST(GeoColumnTest, GenericScalarViewersFailInsideGeoColumn) {
    check_unsupported_scalar_viewers<TYPE_GEOGRAPHY>();
    check_unsupported_scalar_viewers<TYPE_GEOMETRY>();
}

TEST(GeoColumnTest, HashVisitorsCannotIgnoreUnsupportedGeo) {
    auto column = GeoColumn::create(descriptor());
    column->append_wkb(Slice(point()));
    uint32_t seed = 17;
    uint8_t selection = 1;
    uint16_t index = 0;
    EXPECT_THROW(column->fnv_hash(&seed, 0, 1), std::runtime_error);
    EXPECT_THROW(column->fnv_hash_with_selection(&seed, &selection, 0, 1), std::runtime_error);
    EXPECT_THROW(column->fnv_hash_selective(&seed, &index, 1), std::runtime_error);
    EXPECT_THROW(column->crc32_hash(&seed, 0, 1), std::runtime_error);
    EXPECT_THROW(column->crc32_hash_with_selection(&seed, &selection, 0, 1), std::runtime_error);
    EXPECT_THROW(column->crc32_hash_selective(&seed, &index, 1), std::runtime_error);
    EXPECT_THROW(column->murmur_hash3_x86_32(&seed, 0, 1), std::runtime_error);
    EXPECT_THROW(column->xxh3_hash(&seed, 0, 1), std::runtime_error);
    EXPECT_THROW(column->xxh3_hash_with_selection(&seed, &selection, 0, 1), std::runtime_error);
    EXPECT_THROW(column->xxh3_hash_selective(&seed, &index, 1), std::runtime_error);
    EXPECT_EQ(17, seed);
    auto elements = NullableColumn::create(std::move(column), NullColumn::create(1, 0));
    auto offsets = UInt32Column::create();
    offsets->append(0);
    offsets->append(1);
    auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
    EXPECT_THROW(array->fnv_hash(&seed, 0, 1), std::runtime_error);
    EXPECT_THROW(array->crc32_hash(&seed, 0, 1), std::runtime_error);
}

} // namespace
} // namespace starrocks
