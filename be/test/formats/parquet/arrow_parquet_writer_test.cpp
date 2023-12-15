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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <parquet/api/writer.h>

#include "util/slice.h"

namespace starrocks::parquet {
namespace {

class MockOutputStream : public arrow::io::OutputStream {
public:
    MockOutputStream() = default;

    ~MockOutputStream() override = default;

    MOCK_METHOD(arrow::Status, Write, (const void* data, int64_t nbytes), (override));

    arrow::Status Close() override { return arrow::Status::OK(); }

    arrow::Result<int64_t> Tell() const override { return arrow::Result<int64_t>(0); }

    bool closed() const override { return false; }
};

using ::testing::Return;

TEST(ArrowParquetWriterTest, Normal) {
    auto sink = std::make_shared<MockOutputStream>();

    EXPECT_CALL(*sink, Write).WillRepeatedly(Return(arrow::Status::OK()));

    auto node = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED,
            {
                    ::parquet::schema::PrimitiveNode::Make("id1", ::parquet::Repetition::REQUIRED,
                                                           ::parquet::Type::INT32),
                    ::parquet::schema::PrimitiveNode::Make("id2", ::parquet::Repetition::REQUIRED,
                                                           ::parquet::Type::INT32),
            });
    auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto writer = ::parquet::ParquetFileWriter::Open(sink, schema);

    {
        auto* rg_writer = writer->AppendBufferedRowGroup();
        std::vector<int32_t> data = {0, 1, 2};

        auto* col_writer = rg_writer->column(0);
        auto* typed_col_writer = dynamic_cast<::parquet::TypedColumnWriter<::parquet::Int32Type>*>(col_writer);
        typed_col_writer->WriteBatch(3, nullptr, nullptr, data.data());

        col_writer = rg_writer->column(1);
        typed_col_writer = dynamic_cast<::parquet::TypedColumnWriter<::parquet::Int32Type>*>(col_writer);
        typed_col_writer->WriteBatch(3, nullptr, nullptr, data.data());

        try {
            rg_writer->Close();
        } catch (const ::parquet::ParquetException& e) {
            std::cout << "exception: " << e.what() << std::endl;
        }
    }

    writer->Close();
}

TEST(ArrowParquetWriterTest, Exception) {
    auto sink = std::make_shared<MockOutputStream>();

    EXPECT_CALL(*sink, Write)
            .WillOnce(Return(arrow::Status::OK()))
            .WillOnce(Return(arrow::Status::OK()))
            .WillOnce(Return(arrow::Status::IOError("io error"))) // error when flush 2nd column chunk
            .WillRepeatedly(Return(arrow::Status::OK()));

    auto node = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED,
            {
                    ::parquet::schema::PrimitiveNode::Make("id1", ::parquet::Repetition::REQUIRED,
                                                           ::parquet::Type::INT32),
                    ::parquet::schema::PrimitiveNode::Make("id2", ::parquet::Repetition::REQUIRED,
                                                           ::parquet::Type::INT32),
            });
    auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto writer = ::parquet::ParquetFileWriter::Open(sink, schema);

    {
        auto* rg_writer = writer->AppendBufferedRowGroup();
        std::vector<int32_t> data = {0, 1, 2};

        auto* col_writer = rg_writer->column(0);
        auto* typed_col_writer = dynamic_cast<::parquet::TypedColumnWriter<::parquet::Int32Type>*>(col_writer);
        typed_col_writer->WriteBatch(3, nullptr, nullptr, data.data());

        col_writer = rg_writer->column(1);
        typed_col_writer = dynamic_cast<::parquet::TypedColumnWriter<::parquet::Int32Type>*>(col_writer);
        typed_col_writer->WriteBatch(3, nullptr, nullptr, data.data());

        try {
            rg_writer->Close();
        } catch (const ::parquet::ParquetException& e) {
            std::cout << "exception: " << e.what() << std::endl;
        }
    }

    writer->Close();
}

} // namespace
} // namespace starrocks::parquet
