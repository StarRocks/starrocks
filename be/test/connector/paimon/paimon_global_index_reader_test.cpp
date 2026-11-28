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

#include "connector/hive/paimon/paimon_global_index_reader.h"

#include <limits>

#include "connector/paimon/paimon_global_index_test_util.h"

namespace starrocks {
using namespace paimon_global_index;
using namespace paimon_global_index_test;

namespace {
void expect_ranges(const std::shared_ptr<paimon::GlobalIndexResult>& result,
                   const std::vector<paimon::Range>& expected) {
    ASSERT_NE(nullptr, result);
    auto ranges = result->ToRanges();
    ASSERT_TRUE(ranges.ok()) << ranges.status().ToString();
    ASSERT_EQ(expected.size(), ranges.value().size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].from, ranges.value()[i].from);
        EXPECT_EQ(expected[i].to, ranges.value()[i].to);
    }
}

class FailingIndexResult final : public paimon::GlobalIndexResult {
public:
    explicit FailingIndexResult(bool fail_offset) : _fail_offset(fail_offset) {}
    paimon::Result<bool> IsEmpty() const override { return false; }
    paimon::Result<std::unique_ptr<Iterator>> CreateIterator() const override {
        return paimon::Status::IOError("unexpected iteration");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> AddOffset(int64_t) override {
        if (_fail_offset) return paimon::Status::IOError("injected offset failure");
        return shared_from_this();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexResult>> Or(
            const std::shared_ptr<paimon::GlobalIndexResult>&) override {
        return paimon::Status::IOError("injected union failure");
    }
    std::string ToString() const override { return "failing"; }

private:
    bool _fail_offset;
};

class SchemaCheckingIndexer final : public paimon::GlobalIndexer {
public:
    paimon::Result<std::optional<std::vector<std::string>>> GetExtraFieldNames() const override {
        return std::optional<std::vector<std::string>>();
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexWriter>> CreateWriter(
            const std::string&, ArrowSchema*, const std::shared_ptr<paimon::GlobalIndexFileWriter>&,
            const std::shared_ptr<paimon::MemoryPool>&) const override {
        return paimon::Status::Invalid("unused writer");
    }
    paimon::Result<std::shared_ptr<paimon::GlobalIndexReader>> CreateReader(
            ArrowSchema* schema, const std::shared_ptr<paimon::GlobalIndexFileReader>&,
            const std::vector<paimon::GlobalIndexIOMeta>&, const std::shared_ptr<paimon::MemoryPool>&) const override {
        auto imported = arrow::ImportSchema(schema);
        EXPECT_TRUE(imported.ok()) << imported.status().ToString();
        if (!imported.ok()) return paimon::Status::Invalid("invalid Arrow C layout");
        imported_schema = imported.ValueOrDie();
        EXPECT_EQ(nullptr, schema->release);
        return std::shared_ptr<paimon::GlobalIndexReader>();
    }
    mutable std::shared_ptr<arrow::Schema> imported_schema;
};
} // namespace

TEST(PaimonGlobalIndexReaderTest, VectorSchemaUsesOnlyArrowCListFloatAndConsumesReleaseOnce) {
    SchemaCheckingIndexer indexer;
    auto reader = create_segment_reader(indexer, "vec", "array<float>", nullptr, {}, paimon::GetDefaultPool());
    ASSERT_TRUE(reader.ok());
    ASSERT_NE(nullptr, indexer.imported_schema);
    ASSERT_EQ(1, indexer.imported_schema->num_fields());
    EXPECT_EQ("vec", indexer.imported_schema->field(0)->name());
    EXPECT_TRUE(indexer.imported_schema->field(0)->type()->Equals(arrow::list(arrow::float32())));
    reader = create_segment_reader(indexer, "key", "int", nullptr, {}, paimon::GetDefaultPool());
    ASSERT_TRUE(reader.ok());
    EXPECT_EQ("key", indexer.imported_schema->field(0)->name());
    EXPECT_TRUE(indexer.imported_schema->field(0)->type()->Equals(arrow::int32()));
}

TEST(PaimonGlobalIndexReaderTest, OffsetsLocalIdsBeforeUnionAndColumnIntersection) {
    SegmentedIndexReader first;
    first.add(std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{0, 2}})), 100);
    first.add(std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{1, 3}})), 103);
    auto result = first.VisitEqual(paimon::Literal(10));
    ASSERT_TRUE(result.ok());
    expect_ranges(result.value(), {{100, 102}, {104, 106}});
    SegmentedIndexReader second;
    second.add(std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{0, 3}})), 102);
    auto other = second.VisitEqual(paimon::Literal(10));
    ASSERT_TRUE(other.ok());
    auto intersection = result.value()->And(other.value());
    ASSERT_TRUE(intersection.ok());
    expect_ranges(intersection.value(), {{102, 102}, {104, 105}});
}

TEST(PaimonGlobalIndexReaderTest, DeduplicatesOverlappingResultsAndPreservesEmptyResults) {
    SegmentedIndexReader reader;
    reader.add(std::make_shared<FakeGlobalIndexReader>(result_from_ranges({})), 0);
    reader.add(std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{0, 3}})), 100);
    reader.add(std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{0, 3}})), 102);
    auto result = reader.VisitIsNotNull();
    ASSERT_TRUE(result.ok());
    expect_ranges(result.value(), {{100, 105}});
    SegmentedIndexReader empty;
    empty.add(std::make_shared<FakeGlobalIndexReader>(result_from_ranges({})), 200);
    result = empty.VisitIsNull();
    ASSERT_TRUE(result.ok());
    expect_ranges(result.value(), {});
}

TEST(PaimonGlobalIndexReaderTest, ForwardsAllScalarOperationsToEverySegment) {
    using Action = std::function<SegmentedIndexReader::Result(SegmentedIndexReader&)>;
    const std::vector<std::pair<std::string, Action>> actions = {
            {"is_null", [](auto& r) { return r.VisitIsNull(); }},
            {"is_not_null", [](auto& r) { return r.VisitIsNotNull(); }},
            {"equal", [](auto& r) { return r.VisitEqual(paimon::Literal(1)); }},
            {"not_equal", [](auto& r) { return r.VisitNotEqual(paimon::Literal(1)); }},
            {"less_than", [](auto& r) { return r.VisitLessThan(paimon::Literal(1)); }},
            {"less_or_equal", [](auto& r) { return r.VisitLessOrEqual(paimon::Literal(1)); }},
            {"greater_than", [](auto& r) { return r.VisitGreaterThan(paimon::Literal(1)); }},
            {"greater_or_equal", [](auto& r) { return r.VisitGreaterOrEqual(paimon::Literal(1)); }},
            {"in", [](auto& r) { return r.VisitIn({paimon::Literal(1)}); }},
            {"not_in", [](auto& r) { return r.VisitNotIn({paimon::Literal(1)}); }},
            {"starts_with",
             [](auto& r) { return r.VisitStartsWith(paimon::Literal(paimon::FieldType::STRING, "a", 1)); }},
            {"ends_with", [](auto& r) { return r.VisitEndsWith(paimon::Literal(paimon::FieldType::STRING, "a", 1)); }},
            {"contains", [](auto& r) { return r.VisitContains(paimon::Literal(paimon::FieldType::STRING, "a", 1)); }},
            {"like", [](auto& r) { return r.VisitLike(paimon::Literal(paimon::FieldType::STRING, "a", 1)); }}};
    auto first = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{0, 0}}));
    auto second = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{1, 1}}));
    SegmentedIndexReader reader;
    reader.add(first, 100);
    reader.add(second, 200);
    for (const auto& [name, action] : actions) {
        SCOPED_TRACE(name);
        auto result = action(reader);
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        expect_ranges(result.value(), {{100, 100}, {201, 201}});
        EXPECT_EQ(name, first->last_operation());
        EXPECT_EQ(name, second->last_operation());
    }
    EXPECT_TRUE(reader.VisitVectorSearch(nullptr).status().IsInvalid());
    EXPECT_TRUE(reader.VisitFullTextSearch(nullptr).status().IsInvalid());
    EXPECT_FALSE(reader.IsThreadSafe());
}

TEST(PaimonGlobalIndexReaderTest, PreservesReaderOffsetAndUnionFailures) {
    for (int fault = 0; fault < 4; ++fault) {
        SCOPED_TRACE(fault);
        auto first = std::make_shared<FakeGlobalIndexReader>(std::make_shared<FailingIndexResult>(fault == 2));
        if (fault == 0) first->fail_with(paimon::Status::IOError("injected reader failure"));
        if (fault == 1) first->set_return_null(true);
        auto second = std::make_shared<FakeGlobalIndexReader>(result_from_ranges({{0, 0}}));
        SegmentedIndexReader reader;
        reader.add(first, 100);
        reader.add(second, 200);
        auto result = reader.VisitIsNull();
        ASSERT_FALSE(result.ok());
        const char* reason[] = {"reader failure", "null result", "offset failure", "union failure"};
        EXPECT_NE(std::string::npos, result.status().ToString().find(reason[fault]));
        if (fault < 3) {
            EXPECT_TRUE(second->last_operation().empty());
        }
    }
}

TEST(PaimonGlobalIndexReaderTest, RejectsTruncatedNegativeAndOversizedMetadataLengths) {
    EXPECT_TRUE(valid_btree_metadata(std::string(9, '\0')));
    for (size_t size : {0, 4, 8, 65537}) EXPECT_FALSE(valid_btree_metadata(std::string(size, '\0')));
    for (int32_t length : {-1, 1, std::numeric_limits<int32_t>::max()}) {
        for (size_t offset : {0, 4}) {
            std::string bytes(9, '\0');
            std::memcpy(bytes.data() + offset, &length, sizeof(length));
            EXPECT_FALSE(valid_btree_metadata(bytes));
        }
    }
    // Binary bounds, including embedded NULs and the final null-boundary flag, are preserved.
    std::string bytes;
    int32_t length = 3;
    bytes.append(reinterpret_cast<const char*>(&length), sizeof(length));
    bytes.append("a\0b", 3);
    bytes.append(reinterpret_cast<const char*>(&length), sizeof(length));
    bytes.append("c\0d", 3);
    bytes.push_back('\1');
    EXPECT_TRUE(valid_btree_metadata(bytes));
    bytes.pop_back();
    EXPECT_FALSE(valid_btree_metadata(bytes));
}

namespace {
template <typename Builder, typename T>
std::shared_ptr<arrow::Array> key_values(std::initializer_list<T> values) {
    Builder builder;
    for (const auto& value : values) {
        auto status = builder.Append(value);
        EXPECT_TRUE(status.ok()) << status;
        if (!status.ok()) return nullptr;
    }
    auto status = builder.AppendNull();
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok()) return nullptr;
    auto result = builder.Finish();
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() ? result.ValueOrDie() : nullptr;
}
} // namespace

TEST(PaimonGlobalIndexReaderTest, ReadsAllSupportedNativeKeyTypesAndNullBounds) {
    ASSERT_NE(nullptr, dlopen("libpaimon_local_file_system.so", RTLD_NOW | RTLD_LOCAL));
    ASSERT_NE(nullptr, dlopen("libpaimon_global_index.so", RTLD_NOW | RTLD_LOCAL));
    std::string root = std::filesystem::temp_directory_path() / "paimon-key-types-XXXXXX";
    ASSERT_NE(nullptr, mkdtemp(root.data()));
    DeferOp cleanup([&] { std::filesystem::remove_all(root); });
    auto fs = paimon::FileSystemFactory::Get("local", root, {});
    ASSERT_TRUE(fs.ok()) << fs.status().ToString();
    auto files = std::make_shared<NativeIndexFiles>(std::move(fs).value(), root);
    auto indexer = paimon::GlobalIndexerFactory::Get("btree", {});
    ASSERT_TRUE(indexer.ok()) << indexer.status().ToString();
    struct Case {
        std::string type;
        std::shared_ptr<arrow::Array> values;
        paimon::Literal match;
    };
    std::vector<Case> cases = {
            {"boolean", key_values<arrow::BooleanBuilder>({false, true}), paimon::Literal(true)},
            {"tinyint", key_values<arrow::Int8Builder>({int8_t{-128}, int8_t{127}}), paimon::Literal(int8_t{127})},
            {"smallint", key_values<arrow::Int16Builder>({int16_t{-32768}, int16_t{32767}}),
             paimon::Literal(int16_t{32767})},
            {"int", key_values<arrow::Int32Builder>({INT32_MIN, INT32_MAX}), paimon::Literal(INT32_MAX)},
            {"bigint", key_values<arrow::Int64Builder>({INT64_MIN, INT64_MAX}), paimon::Literal(INT64_MAX)},
            {"string", key_values<arrow::StringBuilder>({std::string("a"), std::string("z\0x", 3)}),
             paimon::Literal(paimon::FieldType::STRING, "z\0x", 3)}};
    for (const auto& c : cases) {
        SCOPED_TRACE(c.type);
        ASSERT_NE(nullptr, c.values);
        auto field = arrow::field("k", c.values->type());
        auto schema = arrow::schema({field});
        ArrowSchema writer_schema{};
        ASSERT_TRUE(arrow::ExportSchema(*schema, &writer_schema).ok());
        DeferOp release_schema([&] {
            if (writer_schema.release) writer_schema.release(&writer_schema);
        });
        auto writer = indexer.value()->CreateWriter("k", &writer_schema, files, paimon::GetDefaultPool());
        ASSERT_TRUE(writer.ok()) << writer.status().ToString();
        auto batch = arrow::StructArray::Make({c.values}, {field});
        ASSERT_TRUE(batch.ok());
        ArrowArray exported{};
        ASSERT_TRUE(arrow::ExportArray(*batch.ValueOrDie(), &exported).ok());
        DeferOp release_batch([&] {
            if (exported.release) exported.release(&exported);
        });
        ASSERT_TRUE(writer.value()->AddBatch(&exported, {0, 1, 2}).ok());
        auto metas = writer.value()->Finish();
        ASSERT_TRUE(metas.ok()) << metas.status().ToString();
        for (const auto& meta : metas.value()) {
            ASSERT_NE(nullptr, meta.metadata);
            EXPECT_TRUE(valid_btree_metadata(std::string_view(meta.metadata->data(), meta.metadata->size())));
        }
        auto reader =
                create_segment_reader(*indexer.value(), "k", c.type, files, metas.value(), paimon::GetDefaultPool());
        ASSERT_TRUE(reader.ok()) << reader.status().ToString();
        auto match = reader.value()->VisitEqual(c.match);
        ASSERT_TRUE(match.ok()) << match.status().ToString();
        expect_ranges(match.value(), {{1, 1}});
        auto nulls = reader.value()->VisitIsNull();
        ASSERT_TRUE(nulls.ok()) << nulls.status().ToString();
        expect_ranges(nulls.value(), {{2, 2}});
        auto unsupported =
                create_segment_reader(*indexer.value(), "k", "decimal", files, metas.value(), paimon::GetDefaultPool());
        EXPECT_TRUE(unsupported.status().IsInvalid());
    }
}

} // namespace starrocks
