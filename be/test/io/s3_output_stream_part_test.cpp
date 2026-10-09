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

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <gtest/gtest.h>

#include <iterator>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "io/io_test_base.h"
#include "io/s3_output_stream.h"

namespace starrocks::io {

namespace {

// Records the requests S3OutputStream sends instead of talking to an object store, so the part
// boundaries can be checked without an endpoint.
class RecordingS3Client : public Aws::S3::S3Client {
public:
    RecordingS3Client()
            : Aws::S3::S3Client(std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("ak", "sk"), client_config(),
                                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false) {}

    Aws::S3::Model::CreateMultipartUploadOutcome CreateMultipartUpload(
            const Aws::S3::Model::CreateMultipartUploadRequest& request) const override {
        ++create_multipart_count;
        Aws::S3::Model::CreateMultipartUploadResult result;
        result.SetUploadId("upload-id");
        return result;
    }

    Aws::S3::Model::UploadPartOutcome UploadPart(const Aws::S3::Model::UploadPartRequest& request) const override {
        if (request.GetPartNumber() == fail_part_number) {
            fail_part_number = 0;
            return Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::INTERNAL_FAILURE,
                                                                             "InternalError", "injected", false));
        }
        EXPECT_EQ(static_cast<int>(parts.size() + 1), request.GetPartNumber());
        auto body = read_body(request);
        EXPECT_EQ(request.GetContentLength(), static_cast<int64_t>(body.size()));
        parts.push_back(std::move(body));
        Aws::S3::Model::UploadPartResult result;
        result.SetETag(std::to_string(request.GetPartNumber()));
        return result;
    }

    Aws::S3::Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(
            const Aws::S3::Model::CompleteMultipartUploadRequest& request) const override {
        completed_part_count = static_cast<int>(request.GetMultipartUpload().GetParts().size());
        return Aws::S3::Model::CompleteMultipartUploadResult();
    }

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest& request) const override {
        single_part = read_body(request);
        return Aws::S3::Model::PutObjectResult();
    }

    std::string uploaded_parts() const {
        std::string all;
        for (const auto& part : parts) {
            all.append(part);
        }
        return all;
    }

    // The client methods are const, so the recorded state is mutable.
    mutable int create_multipart_count = 0;
    mutable std::vector<std::string> parts;
    mutable int completed_part_count = 0;
    mutable std::string single_part;
    // UploadPart fails once for this part number; 0 means never.
    mutable int fail_part_number = 0;

private:
    static Aws::Client::ClientConfiguration client_config() {
        Aws::Client::ClientConfigurationInitValues init_values;
        init_values.shouldDisableIMDS = true;
        Aws::Client::ClientConfiguration config(init_values);
        config.region = "us-east-1";
        return config;
    }

    static std::string read_body(const Aws::AmazonWebServiceRequest& request) {
        auto body = request.GetBody();
        return {std::istreambuf_iterator<char>(*body), std::istreambuf_iterator<char>()};
    }
};

constexpr int64_t kPartSize = 1024;
constexpr int64_t kMaxSinglePartSize = 3000;

// Writes `data` in chunks cycling through `chunks`.
Status write_in_chunks(S3OutputStream* os, const std::string& data, const std::vector<int64_t>& chunks) {
    int64_t offset = 0;
    for (size_t i = 0; offset < static_cast<int64_t>(data.size()); ++i) {
        auto len = std::min<int64_t>(chunks[i % chunks.size()], data.size() - offset);
        RETURN_IF_ERROR(os->write(data.data() + offset, len));
        offset += len;
    }
    return Status::OK();
}

void expect_equal_parts(const RecordingS3Client& client, const std::string& data) {
    const auto expected_parts = static_cast<size_t>((data.size() + kPartSize - 1) / kPartSize);
    ASSERT_EQ(expected_parts, client.parts.size());
    for (size_t i = 0; i + 1 < client.parts.size(); ++i) {
        EXPECT_EQ(kPartSize, client.parts[i].size()) << "part " << i + 1;
    }
    auto last_size = data.size() % kPartSize == 0 ? kPartSize : data.size() % kPartSize;
    EXPECT_EQ(last_size, client.parts.back().size());
    EXPECT_EQ(static_cast<int>(expected_parts), client.completed_part_count);
    EXPECT_TRUE(client.uploaded_parts() == data);
}

} // namespace

// The suite name leaves out "S3" on purpose: run-be-ut.sh skips `*S3*` unless --with-aws is given,
// and these cases need no object store.
class MultipartEqualPartSizeTest : public testing::Test {
protected:
    static void SetUpTestSuite() { Aws::InitAPI(_options); }

    static void TearDownTestSuite() { Aws::ShutdownAPI(_options); }

    void SetUp() override { _client = std::make_shared<RecordingS3Client>(); }

    std::unique_ptr<S3OutputStream> new_stream(bool equal_part_size, int64_t part_size = kPartSize) {
        return std::make_unique<S3OutputStream>(_client, "bucket", "object", kMaxSinglePartSize, part_size,
                                                "application/octet-stream", equal_part_size);
    }

    inline static Aws::SDKOptions _options;
    std::shared_ptr<RecordingS3Client> _client;
};

// One write spanning several parts, and writes that never line up with a part boundary.
TEST_F(MultipartEqualPartSizeTest, test_unaligned_writes) {
    const auto data = random_string(10 * kPartSize + 123);
    const std::vector<std::vector<int64_t>> write_patterns = {{static_cast<int64_t>(data.size())},
                                                              {777, 1500, 333, 41}};
    for (const auto& chunks : write_patterns) {
        _client = std::make_shared<RecordingS3Client>();
        auto os = new_stream(true);
        ASSERT_OK(write_in_chunks(os.get(), data, chunks));
        ASSERT_OK(os->close());
        EXPECT_EQ(1, _client->create_multipart_count);
        expect_equal_parts(*_client, data);
    }
}

// An exact multiple of the part size must not leave an empty trailing part.
TEST_F(MultipartEqualPartSizeTest, test_exact_multiple) {
    const auto data = random_string(8 * kPartSize);
    auto os = new_stream(true);
    ASSERT_OK(write_in_chunks(os.get(), data, {500}));
    ASSERT_OK(os->close());
    expect_equal_parts(*_client, data);
}

// get_direct_buffer_and_advance() grows the buffer without uploading, so close() must still cut
// it into full parts rather than send it as one oversized last part.
TEST_F(MultipartEqualPartSizeTest, test_direct_buffer_before_close) {
    const auto data = random_string(kMaxSinglePartSize + 1 + 5 * kPartSize + 7);
    auto os = new_stream(true);
    ASSERT_OK(os->write(data.data(), kMaxSinglePartSize + 1));
    const int64_t rest = data.size() - (kMaxSinglePartSize + 1);
    ASSIGN_OR_ABORT(auto* buf, os->get_direct_buffer_and_advance(rest));
    memcpy(buf, data.data() + kMaxSinglePartSize + 1, rest);
    ASSERT_OK(os->close());
    expect_equal_parts(*_client, data);
}

// A failed part keeps its bytes buffered, so the next write sends them again under the same part
// number and nothing is duplicated or lost.
TEST_F(MultipartEqualPartSizeTest, test_failed_part_is_retried) {
    const auto data = random_string(kMaxSinglePartSize + 1 + 4 * kPartSize + 9);
    auto os = new_stream(true);
    _client->fail_part_number = 2;
    ASSERT_ERROR(os->write(data.data(), kMaxSinglePartSize + 1));
    EXPECT_EQ(1, _client->parts.size());
    ASSERT_OK(os->write(data.data() + kMaxSinglePartSize + 1, data.size() - (kMaxSinglePartSize + 1)));
    ASSERT_OK(os->close());
    expect_equal_parts(*_client, data);
}

// A non-positive part size can never drain the buffer, so it fails the write instead of looping.
TEST_F(MultipartEqualPartSizeTest, test_invalid_part_size) {
    const auto data = random_string(kMaxSinglePartSize + 1);
    auto os = new_stream(true, 0);
    auto st = os->write(data.data(), data.size());
    EXPECT_TRUE(st.is_invalid_argument()) << st;
    EXPECT_TRUE(_client->parts.empty());
}

// An object below the multipart threshold is still a single PutObject.
TEST_F(MultipartEqualPartSizeTest, test_single_part) {
    const auto data = random_string(kMaxSinglePartSize);
    auto os = new_stream(true);
    ASSERT_OK(write_in_chunks(os.get(), data, {700}));
    ASSERT_OK(os->close());
    EXPECT_EQ(0, _client->create_multipart_count);
    EXPECT_TRUE(_client->parts.empty());
    EXPECT_TRUE(_client->single_part == data);
}

// With the option off, the whole buffer goes out as a part whenever it reaches the part size, so
// parts follow the write boundaries. This is the behavior some S3-compatible stores reject.
TEST_F(MultipartEqualPartSizeTest, test_disabled_keeps_write_boundaries) {
    const auto data = random_string(7000);
    auto os = new_stream(false);
    ASSERT_OK(write_in_chunks(os.get(), data, {1500}));
    ASSERT_OK(os->close());
    // Multipart starts once 4500 bytes are buffered, then each 1500-byte write is a part of its own.
    ASSERT_EQ(3, _client->parts.size());
    EXPECT_EQ(4500, _client->parts[0].size());
    EXPECT_EQ(1500, _client->parts[1].size());
    EXPECT_EQ(1000, _client->parts[2].size());
    EXPECT_TRUE(_client->uploaded_parts() == data);
}

} // namespace starrocks::io
