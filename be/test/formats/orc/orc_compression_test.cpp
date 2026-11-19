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

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "formats/orc/apache-orc/c++/src/ByteRLE.hh"
#include "formats/orc/apache-orc/c++/src/Compression.hh"
#include "orc/OrcFile.hh"

namespace orc {
namespace {
class RegressionOutputStream final : public OutputStream {
public:
    uint64_t getLength() const override { return _data.size(); }
    uint64_t getNaturalWriteSize() const override { return 1024; }
    void write(const void* data, size_t size) override { _data.append(static_cast<const char*>(data), size); }
    const std::string& getName() const override { return _name; }
    const char* getData() const { return _data.data(); }
    void close() override {}

private:
    std::string _data;
    const std::string _name = "compression-regression-output";
};

class RegressionInputStream final : public InputStream {
public:
    RegressionInputStream(const char* data, uint64_t size) : _data(data), _size(size) {}
    uint64_t getLength() const override { return _size; }
    uint64_t getNaturalReadSize() const override { return 1024; }
    void read(void* data, uint64_t length, uint64_t offset) override { memcpy(data, _data + offset, length); }
    const std::string& getName() const override { return _name; }

private:
    const char* _data;
    uint64_t _size;
    const std::string _name = "compression-regression-input";
};
} // namespace

TEST(OrcCompressionTest, reuseSuppressedPresentStream) {
    for (auto kind : {CompressionKind_ZLIB, CompressionKind_ZSTD, CompressionKind_LZ4, CompressionKind_SNAPPY}) {
        SCOPED_TRACE(static_cast<int>(kind));
        RegressionOutputStream memStream;
        auto stream = createCompressor(kind, &memStream, CompressionStrategy_SPEED, 1024, 128, *getDefaultPool());
        auto encoder = createBooleanRleEncoder(std::move(stream));
        // A non-null stripe fills multiple compression blocks before its PRESENT stream is omitted.
        std::vector<char> notNull(100000, 1);
        encoder->add(notNull.data(), notNull.size(), nullptr);
        encoder->suppress();
        // The next stripe contains NULLs and must flush its PRESENT stream.
        const char nextStripe[] = {1, 0, 1};
        encoder->add(nextStripe, 3, nullptr);
        try {
            encoder->flush();
        } catch (const std::exception& e) {
            FAIL() << e.what();
        }
        auto input = std::make_unique<SeekableArrayInputStream>(memStream.getData(), memStream.getLength());
        auto decoder = createBooleanRleDecoder(
                createDecompressor(kind, std::move(input), 128, *getDefaultPool(), nullptr), nullptr);
        char actual[3];
        decoder->next(actual, 3, nullptr);
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_EQ(nextStripe[i], actual[i]);
        }
    }
}

TEST(OrcCompressionTest, nullableStripeAfterSuppressedPresentStream) {
    for (auto kind : {CompressionKind_NONE, CompressionKind_ZLIB, CompressionKind_ZSTD, CompressionKind_LZ4,
                      CompressionKind_SNAPPY}) {
        SCOPED_TRACE(static_cast<int>(kind));
        RegressionOutputStream output;
        auto type = Type::buildTypeFromString("struct<value:bigint>");
        WriterOptions options;
        options.setStripeSize(1).setCompressionBlockSize(128).setCompression(kind);
        auto writer = createWriter(*type, &output, options);
        constexpr uint64_t rowsPerStripe = 100000;
        auto batch = writer->createRowBatch(rowsPerStripe);
        auto& root = dynamic_cast<StructVectorBatch&>(*batch);
        auto& values = dynamic_cast<LongVectorBatch&>(*root.fields[0]);
        for (uint64_t stripe = 0; stripe < 3; ++stripe) {
            root.numElements = values.numElements = rowsPerStripe;
            values.hasNulls = stripe == 1;
            for (uint64_t row = 0; row < rowsPerStripe; ++row) {
                root.notNull[row] = 1;
                values.notNull[row] = stripe != 1 || row % 3 != 0;
                values.data[row] = static_cast<int64_t>(stripe * rowsPerStripe + row);
            }
            writer->add(*batch);
        }
        writer->close();
        auto reader = createReader(std::make_unique<RegressionInputStream>(output.getData(), output.getLength()),
                                   ReaderOptions());
        ASSERT_GE(reader->getNumberOfStripes(), 3);
        ASSERT_EQ(3 * rowsPerStripe, reader->getNumberOfRows());
        auto rowReader = reader->createRowReader();
        auto readBatch = rowReader->createRowBatch(1024);
        uint64_t row = 0;
        while (rowReader->next(*readBatch)) {
            auto& readRoot = dynamic_cast<StructVectorBatch&>(*readBatch);
            auto& readValues = dynamic_cast<LongVectorBatch&>(*readRoot.fields[0]);
            for (uint64_t i = 0; i < readRoot.numElements; ++i, ++row) {
                bool expectedNotNull = row / rowsPerStripe != 1 || (row % rowsPerStripe) % 3 != 0;
                ASSERT_EQ(expectedNotNull, !readValues.hasNulls || readValues.notNull[i]);
                if (expectedNotNull) {
                    ASSERT_EQ(static_cast<int64_t>(row), readValues.data[i]);
                }
            }
        }
        ASSERT_EQ(3 * rowsPerStripe, row);
    }
}

} // namespace orc
