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

#include "http/action/lake/dump_tablet_metadata_action.h"

#include <event2/http.h>
#include <event2/keyvalq_struct.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <gtest/gtest.h>
#include <json2pb/pb_to_json.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "base/testutil/assert.h"
#include "base/url_coding.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "http/action/lake/dump_tablet_metadata_serializer.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks {
extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);
} // namespace starrocks

namespace starrocks::lake {
namespace {

HttpStatus g_response_status = HttpStatus::OK;
std::string g_response_body;

void capture_reply(HttpRequest*, HttpStatus status, std::string_view body) {
    g_response_status = status;
    g_response_body.assign(body);
}

struct SyntheticRequest {
    explicit SyntheticRequest(DumpTabletMetadataAction* action) {
        ev_req = evhttp_request_new(nullptr, nullptr);
        request = std::make_unique<HttpRequest>(ev_req);
        request->set_handler(action);
        request->set_method(HttpMethod::GET);
    }

    SyntheticRequest(SyntheticRequest&& other) noexcept
            : ev_req(std::exchange(other.ev_req, nullptr)), request(std::move(other.request)) {}
    SyntheticRequest& operator=(SyntheticRequest&&) = delete;
    SyntheticRequest(const SyntheticRequest&) = delete;
    SyntheticRequest& operator=(const SyntheticRequest&) = delete;

    ~SyntheticRequest() {
        request.reset();
        if (ev_req != nullptr) {
            evhttp_request_free(ev_req);
        }
    }

    void set_route(std::string tablet_id) { request->set_route_params({{"TabletId", std::move(tablet_id)}}); }

    void add_query(std::string key, std::string value, size_t count = 1) {
        request->_query_params.emplace(key, std::move(value));
        request->_query_param_counts.emplace(std::move(key), count);
    }

    evhttp_request* ev_req = nullptr;
    std::unique_ptr<HttpRequest> request;
};

SyntheticRequest valid_request(DumpTabletMetadataAction* action, std::string tablet_id = "17",
                               std::string version = "2", std::string is_bundle = "false") {
    SyntheticRequest holder(action);
    holder.set_route(std::move(tablet_id));
    holder.add_query("version", std::move(version));
    holder.add_query("is_bundle", std::move(is_bundle));
    return holder;
}

struct ScopedReplyCapture {
    ScopedReplyCapture() {
        g_response_status = HttpStatus::OK;
        g_response_body.clear();
        s_injected_send_reply = capture_reply;
    }
    ~ScopedReplyCapture() { s_injected_send_reply = nullptr; }
};

void expect_diagnostic_headers(evhttp_request* request) {
    auto* headers = evhttp_request_get_output_headers(request);
    ASSERT_NE(nullptr, evhttp_find_header(headers, "Content-Type"));
    EXPECT_STREQ("application/json", evhttp_find_header(headers, "Content-Type"));
    ASSERT_NE(nullptr, evhttp_find_header(headers, "Cache-Control"));
    EXPECT_STREQ("no-store", evhttp_find_header(headers, "Cache-Control"));
    ASSERT_NE(nullptr, evhttp_find_header(headers, "X-Content-Type-Options"));
    EXPECT_STREQ("nosniff", evhttp_find_header(headers, "X-Content-Type-Options"));
}

constexpr std::array<const char*, 7> kExpectedRedactedFields = {
        "starrocks.DelfileWithRowsetId.encryption_meta",
        "starrocks.DeltaColumnGroupVerPB.encryption_metas",
        "starrocks.FileMetaPB.encryption_meta",
        "starrocks.IndexDeltaGroupEntryPB.encryption_meta",
        "starrocks.PersistentIndexSstablePB.encryption_meta",
        "starrocks.RowsetMetadataPB.deprecated_segment_encryption_metas",
        "starrocks.SegmentMetadataPB.encryption_meta",
};

void collect_reachable_encryption_fields(const google::protobuf::Descriptor* descriptor,
                                         std::unordered_set<const google::protobuf::Descriptor*>* visited,
                                         std::set<std::string>* fields) {
    if (!visited->insert(descriptor).second) {
        return;
    }
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES &&
            field->name().find("encryption_meta") != std::string::npos) {
            fields->insert(field->full_name());
        }
        if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            collect_reachable_encryption_fields(field->message_type(), visited, fields);
        }
    }
}

TabletMetadataPB metadata_with_all_encryption_fields(std::vector<std::string>* secrets) {
    TabletMetadataPB metadata;
    metadata.set_id(11979);
    metadata.set_version(23);

    auto next_secret = [&]() -> const std::string& {
        secrets->emplace_back("secret-material-" + std::to_string(secrets->size()));
        return secrets->back();
    };
    metadata.mutable_sstable_meta()->add_sstables()->set_encryption_meta(next_secret());
    metadata.add_orphan_files()->set_encryption_meta(next_secret());
    (*metadata.mutable_delvec_meta()->mutable_version_to_file())[11].set_encryption_meta(next_secret());
    (*metadata.mutable_dcg_meta()->mutable_dcgs())[12].add_encryption_metas(next_secret());
    (*metadata.mutable_idg_meta()->mutable_idgs())[13].add_entries()->set_encryption_meta(next_secret());
    auto* rowset = metadata.add_rowsets();
    rowset->add_del_files()->set_encryption_meta(next_secret());
    rowset->add_segment_metas()->set_encryption_meta(next_secret());
    rowset->add_deprecated_segment_encryption_metas(next_secret());
    // Exercise the second route into RowsetMetadataPB as well.
    metadata.add_compaction_inputs()->add_segment_metas()->set_encryption_meta(next_secret());
    return metadata;
}

class RefusingOutputStream final : public google::protobuf::io::ZeroCopyOutputStream {
public:
    bool Next(void** data, int* size) override {
        if (_granted) {
            _refused = true;
            return false;
        }
        _granted = true;
        *data = _buffer.data();
        *size = static_cast<int>(_buffer.size());
        _byte_count += *size;
        return true;
    }

    void BackUp(int count) override { _byte_count -= count; }
    int64_t ByteCount() const override { return _byte_count; }
    bool refused() const { return _refused; }

private:
    std::array<char, 8> _buffer{};
    int64_t _byte_count = 0;
    bool _granted = false;
    bool _refused = false;
};

TEST(DumpTabletMetadataSerializerTest, RedactsEveryReachableEncryptionFieldWithoutMutatingInput) {
    std::vector<std::string> secrets;
    TabletMetadataPB metadata = metadata_with_all_encryption_fields(&secrets);
    const std::string original = metadata.SerializeAsString();

    auto result = serialize_dump_tablet_metadata(metadata, 1 << 20);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE(result->redacted);
    EXPECT_EQ(original, metadata.SerializeAsString());

    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(result->body.data(), result->body.size()).HasParseError());
    ASSERT_TRUE(document.IsObject());
    ASSERT_TRUE(document.HasMember("metadata"));
    ASSERT_TRUE(document["metadata"].IsObject());
    ASSERT_TRUE(document["metadata"]["id"].IsInt64());
    ASSERT_TRUE(document["metadata"]["version"].IsInt64());
    EXPECT_EQ(11979, document["metadata"]["id"].GetInt64());
    EXPECT_EQ(23, document["metadata"]["version"].GetInt64());
    ASSERT_TRUE(document.HasMember("redacted_fields"));
    ASSERT_TRUE(document["redacted_fields"].IsArray());
    ASSERT_EQ(kExpectedRedactedFields.size(), document["redacted_fields"].Size());
    for (size_t i = 0; i < kExpectedRedactedFields.size(); ++i) {
        EXPECT_STREQ(kExpectedRedactedFields[i],
                     document["redacted_fields"][static_cast<rapidjson::SizeType>(i)].GetString());
    }
    EXPECT_EQ(std::string::npos, result->body.find('\n'));
    rapidjson::StringBuffer compact;
    rapidjson::Writer<rapidjson::StringBuffer> writer(compact);
    ASSERT_TRUE(document.Accept(writer));
    EXPECT_EQ(result->body, std::string(compact.GetString(), compact.GetSize()));
    for (const auto& secret : secrets) {
        std::string encoded;
        base64_encode(secret, &encoded);
        EXPECT_EQ(std::string::npos, result->body.find(secret));
        EXPECT_EQ(std::string::npos, result->body.find(encoded));
    }
}

TEST(DumpTabletMetadataSerializerTest, ReachableEncryptionFieldPolicyIsComplete) {
    std::unordered_set<const google::protobuf::Descriptor*> visited;
    std::set<std::string> actual;
    collect_reachable_encryption_fields(TabletMetadataPB::descriptor(), &visited, &actual);
    const std::set<std::string> expected(kExpectedRedactedFields.begin(), kExpectedRedactedFields.end());
    EXPECT_EQ(expected, actual);
}

TEST(DumpTabletMetadataSerializerTest, OmitsRedactionListWhenNoEncryptionMaterialIsPresent) {
    TabletMetadataPB metadata;
    metadata.set_id(7);
    metadata.set_version(9);

    auto result = serialize_dump_tablet_metadata(metadata, 1024);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_FALSE(result->redacted);
    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(result->body.data(), result->body.size()).HasParseError());
    EXPECT_TRUE(document.HasMember("metadata"));
    EXPECT_FALSE(document.HasMember("redacted_fields"));
    EXPECT_FALSE(document["metadata"].IsArray());
    EXPECT_EQ(std::string::npos, result->body.find('\n'));
}

TEST(DumpTabletMetadataSerializerTest, AppliesCapToCompleteEnvelopeAtExactBoundary) {
    std::vector<std::string> secrets;
    TabletMetadataPB metadata = metadata_with_all_encryption_fields(&secrets);
    auto baseline_or = serialize_dump_tablet_metadata(metadata, 1 << 20);
    ASSERT_TRUE(baseline_or.ok()) << baseline_or.status();
    auto baseline = std::move(baseline_or).value();
    const size_t size = baseline.body.size();
    ASSERT_GT(size, 1);

    auto under = serialize_dump_tablet_metadata(metadata, size - 1);
    ASSERT_FALSE(under.ok());
    EXPECT_TRUE(under.status().is_capacity_limit_exceeded()) << under.status();

    auto exact_or = serialize_dump_tablet_metadata(metadata, size);
    ASSERT_TRUE(exact_or.ok()) << exact_or.status();
    auto exact = std::move(exact_or).value();
    EXPECT_EQ(size, exact.body.size());
    EXPECT_EQ(baseline.body, exact.body);

    auto over_or = serialize_dump_tablet_metadata(metadata, size + 1);
    ASSERT_TRUE(over_or.ok()) << over_or.status();
    auto over = std::move(over_or).value();
    EXPECT_EQ(baseline.body, over.body);
}

TEST(DumpTabletMetadataSerializerTest, RejectsSinkRefusalEvenWhenJson2pbReportsSuccess) {
    TabletMetadataPB metadata;
    metadata.set_id(123456789);
    metadata.set_version(987654321);
    json2pb::Pb2JsonOptions options;
    options.pretty_json = false;
    RefusingOutputStream refusing_stream;
    std::string error;
    (void)json2pb::ProtoMessageToJson(metadata, &refusing_stream, options, &error);
    EXPECT_TRUE(refusing_stream.refused());

    auto result = serialize_dump_tablet_metadata(metadata, 8);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_capacity_limit_exceeded()) << result.status();
}

TEST(DumpTabletMetadataActionTest, RejectsEveryInputOutsideTheExactReadContractBeforeAdmission) {
    ScopedReplyCapture capture;
    DumpTabletMetadataAction action(nullptr);
    struct InvalidRequest {
        const char* tablet_id;
        const char* version;
        const char* is_bundle;
        size_t version_count = 1;
        size_t bundle_count = 1;
        bool add_unknown = false;
    };
    const std::vector<InvalidRequest> invalid = {
            {nullptr, "2", "false"},
            {"", "2", "false"},
            {"+1", "2", "false"},
            {" 1", "2", "false"},
            {"0", "2", "false"},
            {"-1", "2", "false"},
            {"0x10", "2", "false"},
            {"9223372036854775808", "2", "false"},
            {"17", nullptr, "false"},
            {"17", "", "false"},
            {"17", "+2", "false"},
            {"17", " 2", "false"},
            {"17", "0", "false"},
            {"17", "-2", "false"},
            {"17", "0x2", "false"},
            {"17", "9223372036854775808", "false"},
            {"17", "2", nullptr},
            {"17", "2", "TRUE"},
            {"17", "2", "False"},
            {"17", "1", "true"},
            {"17", "2", "false", 2, 1},
            {"17", "2", "false", 1, 2},
            {"17", "2", "false", 1, 1, true},
    };

    for (const auto& test_case : invalid) {
        SCOPED_TRACE(
                testing::Message() << "tablet=" << (test_case.tablet_id == nullptr ? "<missing>" : test_case.tablet_id)
                                   << " version=" << (test_case.version == nullptr ? "<missing>" : test_case.version)
                                   << " bundle="
                                   << (test_case.is_bundle == nullptr ? "<missing>" : test_case.is_bundle));
        SyntheticRequest request(&action);
        if (test_case.tablet_id != nullptr) {
            request.set_route(test_case.tablet_id);
        }
        if (test_case.version != nullptr) {
            request.add_query("version", test_case.version, test_case.version_count);
        }
        if (test_case.is_bundle != nullptr) {
            request.add_query("is_bundle", test_case.is_bundle, test_case.bundle_count);
        }
        if (test_case.add_unknown) {
            request.add_query("pretty", "true");
        }
        g_response_status = HttpStatus::OK;
        g_response_body.clear();
        EXPECT_EQ(-1, action.on_header(request.request.get()));
        EXPECT_EQ(HttpStatus::BAD_REQUEST, g_response_status);
        EXPECT_EQ(R"({"code":"INVALID_ARGUMENT","message":"invalid diagnostic request"})", g_response_body);
        EXPECT_EQ(nullptr, request.request->handler_ctx());
        expect_diagnostic_headers(request.ev_req);
    }
}

class DumpTabletMetadataActionStorageTest : public testing::Test {
protected:
    void SetUp() override {
        _root = "/tmp/starrocks-dump-tablet-metadata-action-" + std::to_string(getpid());
        (void)fs::remove_all(_root);
        ASSERT_OK(fs::create_directories(join_path(_root, kMetadataDirectoryName)));
        _provider = std::make_shared<FixedLocationProvider>(_root);
        _tablet_manager = std::make_unique<TabletManager>(_provider, 1);
        s_injected_send_reply = capture_reply;
    }

    void TearDown() override {
        s_injected_send_reply = nullptr;
        _tablet_manager.reset();
        _provider.reset();
        (void)fs::remove_all(_root);
    }

    void put_metadata(int64_t tablet_id, int64_t version) {
        TabletMetadataPB metadata;
        metadata.set_id(tablet_id);
        metadata.set_version(version);
        metadata.mutable_schema()->set_id(700);
        metadata.mutable_schema()->set_keys_type(DUP_KEYS);
        ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));
    }

    std::string _root;
    std::shared_ptr<FixedLocationProvider> _provider;
    std::unique_ptr<TabletManager> _tablet_manager;
};

TEST_F(DumpTabletMetadataActionStorageTest, ReturnsOneCompactStandaloneObjectAndSecurityHeaders) {
    constexpr int64_t kTabletId = 11979;
    constexpr int64_t kVersion = 23;
    put_metadata(kTabletId, kVersion);
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion), "false");

    ASSERT_EQ(0, action.on_header(request.request.get()));
    action.handle(request.request.get());

    EXPECT_EQ(HttpStatus::OK, g_response_status);
    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(g_response_body.data(), g_response_body.size()).HasParseError()) << g_response_body;
    ASSERT_TRUE(document.IsObject());
    ASSERT_TRUE(document.HasMember("metadata"));
    EXPECT_FALSE(document["metadata"].IsArray());
    ASSERT_TRUE(document["metadata"]["id"].IsInt64());
    ASSERT_TRUE(document["metadata"]["version"].IsInt64());
    EXPECT_EQ(kTabletId, document["metadata"]["id"].GetInt64());
    EXPECT_EQ(kVersion, document["metadata"]["version"].GetInt64());
    EXPECT_FALSE(document.HasMember("num"));
    EXPECT_FALSE(document.HasMember("is_bundle"));
    EXPECT_EQ(std::string::npos, g_response_body.find('\n'));
    expect_diagnostic_headers(request.ev_req);
}

TEST_F(DumpTabletMetadataActionStorageTest, UsesStableSanitizedErrorsForMissingWrongFormatAndOversize) {
    constexpr int64_t kTabletId = 41;
    constexpr int64_t kVersion = 4;
    put_metadata(kTabletId, kVersion);
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());

    {
        auto request = valid_request(&action, "999", "4", "false");
        ASSERT_EQ(0, action.on_header(request.request.get()));
        action.handle(request.request.get());
        EXPECT_EQ(HttpStatus::NOT_FOUND, g_response_status);
        EXPECT_EQ(R"({"code":"METADATA_NOT_FOUND","message":"tablet metadata is unavailable"})", g_response_body);
        expect_diagnostic_headers(request.ev_req);
    }
    {
        auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion), "true");
        ASSERT_EQ(0, action.on_header(request.request.get()));
        action.handle(request.request.get());
        EXPECT_EQ(HttpStatus::NOT_FOUND, g_response_status);
        EXPECT_EQ(R"({"code":"METADATA_NOT_FOUND","message":"tablet metadata is unavailable"})", g_response_body);
        expect_diagnostic_headers(request.ev_req);
    }

    constexpr int64_t kOversizedTabletId = 42;
    constexpr int64_t kOversizedVersion = 5;
    auto writable_or =
            fs::new_writable_file(_provider->tablet_metadata_location(kOversizedTabletId, kOversizedVersion));
    ASSERT_TRUE(writable_or.ok()) << writable_or.status();
    auto writable = std::move(writable_or).value();
    const std::string oversized((16ULL << 20) + 1, 'x');
    ASSERT_OK(writable->append(Slice(oversized)));
    ASSERT_OK(writable->close());
    {
        auto request =
                valid_request(&action, std::to_string(kOversizedTabletId), std::to_string(kOversizedVersion), "false");
        ASSERT_EQ(0, action.on_header(request.request.get()));
        action.handle(request.request.get());
        EXPECT_EQ(HttpStatus::REQUEST_ENTITY_TOO_LARGE, g_response_status);
        EXPECT_EQ(R"({"code":"METADATA_TOO_LARGE","message":"tablet metadata exceeds a diagnostic limit"})",
                  g_response_body);
        expect_diagnostic_headers(request.ev_req);
    }
}

TEST_F(DumpTabletMetadataActionStorageTest, HoldsAdmissionUntilRequestFreeAndDoesNotQueue) {
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto first = std::make_unique<SyntheticRequest>(valid_request(&action));
    ASSERT_EQ(0, action.on_header(first->request.get()));
    action.handle(first->request.get());

    auto second = valid_request(&action);
    EXPECT_EQ(-1, action.on_header(second.request.get()));
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE, g_response_status);
    EXPECT_EQ(R"({"code":"DIAGNOSTIC_BUSY","message":"another tablet metadata diagnostic is active"})",
              g_response_body);
    EXPECT_EQ(nullptr, second.request->handler_ctx());
    expect_diagnostic_headers(second.ev_req);

    first.reset();
    auto third = valid_request(&action);
    EXPECT_EQ(0, action.on_header(third.request.get()));
    EXPECT_NE(nullptr, third.request->handler_ctx());
}

} // namespace
} // namespace starrocks::lake
