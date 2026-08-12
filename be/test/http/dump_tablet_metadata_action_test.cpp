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

#include <arpa/inet.h>
#include <event2/http.h>
#include <event2/keyvalq_struct.h>
#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <gtest/gtest.h>
#include <json2pb/pb_to_json.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/url_coding.h"
#include "common/config_http_fwd.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "http/action/lake/dump_tablet_metadata_serializer.h"
#include "http/action/lake/dump_tablet_metadata_serializer_internal.h"
#include "platform/http/ev_http_server.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_client.h"
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
bool g_release_handler_ctx_during_reply = false;
constexpr int64_t kReleasedTabletIdSentinel = 8111222333444555LL;
constexpr int64_t kReleasedVersionSentinel = 8555444333222111LL;

void capture_reply(HttpRequest* request, HttpStatus status, std::string_view body) {
    g_response_status = status;
    g_response_body.assign(body);
    if (g_release_handler_ctx_during_reply && request->handler_ctx() != nullptr) {
        DumpTabletMetadataAction::overwrite_handler_ctx_ids_for_test(request->handler_ctx(), kReleasedTabletIdSentinel,
                                                                     kReleasedVersionSentinel);
        request->handler()->free_handler_ctx(request->handler_ctx());
        request->_handler_ctx = nullptr;
    }
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
    ~ScopedReplyCapture() {
        g_release_handler_ctx_during_reply = false;
        s_injected_send_reply = nullptr;
    }
};

class DiagnosticLogSink final : public google::LogSink {
public:
    void send(google::LogSeverity, const char*, const char*, int, const google::LogMessageTime&, const char* message,
              size_t message_len) override {
        std::lock_guard lock(_mutex);
        _messages.emplace_back(message, message_len);
    }

    bool contains(std::string_view first, std::string_view second) const {
        std::lock_guard lock(_mutex);
        return std::any_of(_messages.begin(), _messages.end(), [&](const std::string& message) {
            return message.find(first) != std::string::npos && message.find(second) != std::string::npos;
        });
    }

private:
    mutable std::mutex _mutex;
    std::vector<std::string> _messages;
};

class ScopedDiagnosticLogSink {
public:
    ScopedDiagnosticLogSink() { google::AddLogSink(&_sink); }
    ~ScopedDiagnosticLogSink() { google::RemoveLogSink(&_sink); }

    const DiagnosticLogSink& sink() const { return _sink; }

private:
    DiagnosticLogSink _sink;
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
        if (field->name().find("encryption_meta") != std::string::npos) {
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

TEST(DumpTabletMetadataSerializerTest, RedactionMatchesFieldNameRegardlessOfScalarRepeatedOrMessageType) {
    google::protobuf::FileDescriptorProto schema;
    schema.set_name("dump_tablet_metadata_redaction_test.proto");
    schema.set_package("starrocks.lake.redaction_test");
    schema.set_syntax("proto2");

    auto* nested_proto = schema.add_message_type();
    nested_proto->set_name("SyntheticNested");
    auto* nested_secret_proto = nested_proto->add_field();
    nested_secret_proto->set_name("encryption_meta_inner");
    nested_secret_proto->set_number(1);
    nested_secret_proto->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
    nested_secret_proto->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
    auto* nested_safe_proto = nested_proto->add_field();
    nested_safe_proto->set_name("safe_value");
    nested_safe_proto->set_number(2);
    nested_safe_proto->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
    nested_safe_proto->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);

    auto* root_proto = schema.add_message_type();
    root_proto->set_name("SyntheticMetadata");
    auto add_root_field = [&](const char* name, int number, google::protobuf::FieldDescriptorProto::Label label,
                              google::protobuf::FieldDescriptorProto::Type type) {
        auto* field = root_proto->add_field();
        field->set_name(name);
        field->set_number(number);
        field->set_label(label);
        field->set_type(type);
        return field;
    };
    add_root_field("encryption_meta_text", 1, google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL,
                   google::protobuf::FieldDescriptorProto::TYPE_STRING);
    add_root_field("encryption_metas_numbers", 2, google::protobuf::FieldDescriptorProto::LABEL_REPEATED,
                   google::protobuf::FieldDescriptorProto::TYPE_INT64);
    auto* matching_message_proto =
            add_root_field("encryption_metadata_message", 3, google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL,
                           google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
    matching_message_proto->set_type_name(".starrocks.lake.redaction_test.SyntheticNested");
    auto* safe_message_proto = add_root_field("safe_nested", 4, google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL,
                                              google::protobuf::FieldDescriptorProto::TYPE_MESSAGE);
    safe_message_proto->set_type_name(".starrocks.lake.redaction_test.SyntheticNested");

    google::protobuf::DescriptorPool pool;
    const auto* file = pool.BuildFile(schema);
    ASSERT_NE(nullptr, file);
    const auto* descriptor = file->FindMessageTypeByName("SyntheticMetadata");
    ASSERT_NE(nullptr, descriptor);
    const auto* nested_descriptor = file->FindMessageTypeByName("SyntheticNested");
    ASSERT_NE(nullptr, nested_descriptor);

    google::protobuf::DynamicMessageFactory factory;
    const auto* prototype = factory.GetPrototype(descriptor);
    ASSERT_NE(nullptr, prototype);
    std::unique_ptr<google::protobuf::Message> message(prototype->New());
    const auto* reflection = message->GetReflection();
    const auto* text_secret = descriptor->FindFieldByName("encryption_meta_text");
    const auto* repeated_secret = descriptor->FindFieldByName("encryption_metas_numbers");
    const auto* message_secret = descriptor->FindFieldByName("encryption_metadata_message");
    const auto* safe_nested = descriptor->FindFieldByName("safe_nested");
    const auto* nested_secret = nested_descriptor->FindFieldByName("encryption_meta_inner");
    const auto* nested_safe = nested_descriptor->FindFieldByName("safe_value");
    ASSERT_NE(nullptr, text_secret);
    ASSERT_NE(nullptr, repeated_secret);
    ASSERT_NE(nullptr, message_secret);
    ASSERT_NE(nullptr, safe_nested);
    ASSERT_NE(nullptr, nested_secret);
    ASSERT_NE(nullptr, nested_safe);

    reflection->SetString(message.get(), text_secret, "future-string-secret");
    reflection->AddInt64(message.get(), repeated_secret, 9007199254740993LL);
    auto* matching_message = reflection->MutableMessage(message.get(), message_secret);
    matching_message->GetReflection()->SetString(matching_message, nested_safe, "future-message-secret");
    auto* ordinary_message = reflection->MutableMessage(message.get(), safe_nested);
    ordinary_message->GetReflection()->SetString(ordinary_message, nested_secret, "nested-secret");
    ordinary_message->GetReflection()->SetString(ordinary_message, nested_safe, "keep");

    std::set<std::string> redacted_fields;
    dump_tablet_metadata_internal::redact_encryption_metadata(message.get(), &redacted_fields);

    EXPECT_FALSE(reflection->HasField(*message, text_secret));
    EXPECT_EQ(0, reflection->FieldSize(*message, repeated_secret));
    EXPECT_FALSE(reflection->HasField(*message, message_secret));
    ASSERT_TRUE(reflection->HasField(*message, safe_nested));
    const auto& sanitized_nested = reflection->GetMessage(*message, safe_nested);
    EXPECT_FALSE(sanitized_nested.GetReflection()->HasField(sanitized_nested, nested_secret));
    EXPECT_EQ("keep", sanitized_nested.GetReflection()->GetString(sanitized_nested, nested_safe));

    const std::set<std::string> expected = {text_secret->full_name(), repeated_secret->full_name(),
                                            message_secret->full_name(), nested_secret->full_name()};
    EXPECT_EQ(expected, redacted_fields);
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
    const bool converted = json2pb::ProtoMessageToJson(metadata, &refusing_stream, options, &error);
    EXPECT_TRUE(converted) << error;
    EXPECT_TRUE(refusing_stream.refused());

    constexpr size_t kEnvelopePrefixBytes = sizeof("{\"metadata\":") - 1;
    constexpr size_t kConverterGrantBytes = 8;
    auto result = serialize_dump_tablet_metadata(metadata, kEnvelopePrefixBytes + kConverterGrantBytes);
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

    ScopedDiagnosticLogSink logs;
    auto second = valid_request(&action);
    EXPECT_EQ(-1, action.on_header(second.request.get()));
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE, g_response_status);
    EXPECT_EQ(R"({"code":"DIAGNOSTIC_BUSY","message":"another tablet metadata diagnostic is active"})",
              g_response_body);
    EXPECT_EQ(nullptr, second.request->handler_ctx());
    expect_diagnostic_headers(second.ev_req);
    EXPECT_TRUE(logs.sink().contains("result=DIAGNOSTIC_BUSY", "elapsed_ms="));

    first.reset();
    auto third = valid_request(&action);
    EXPECT_EQ(0, action.on_header(third.request.get()));
    EXPECT_NE(nullptr, third.request->handler_ctx());
}

TEST_F(DumpTabletMetadataActionStorageTest, ReplyMayReleaseContextWithoutPostSendAccessOrDoubleRelease) {
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto first = valid_request(&action, "999", "4", "false");
    ASSERT_EQ(0, action.on_header(first.request.get()));
    ASSERT_NE(nullptr, first.request->handler_ctx());

    ScopedDiagnosticLogSink logs;
    g_release_handler_ctx_during_reply = true;
    action.handle(first.request.get());
    g_release_handler_ctx_during_reply = false;
    EXPECT_EQ(nullptr, first.request->handler_ctx());
    EXPECT_TRUE(logs.sink().contains("tablet_id=999 version=4", "result=METADATA_NOT_FOUND"));
    EXPECT_FALSE(logs.sink().contains("tablet_id=" + std::to_string(kReleasedTabletIdSentinel),
                                      "version=" + std::to_string(kReleasedVersionSentinel)));

    // Acquiring the sole permit again proves the reply hook released the first
    // request's guard; ASAN also verifies the old request's destructor does not
    // release it a second time.
    auto second = valid_request(&action);
    EXPECT_EQ(0, action.on_header(second.request.get()));
    EXPECT_NE(nullptr, second.request->handler_ctx());
}

class CountingLocationProvider final : public LocationProvider {
public:
    explicit CountingLocationProvider(std::string root) : _root(std::move(root)) {}

    std::string root_location(int64_t) const override {
        _root_location_calls.fetch_add(1, std::memory_order_relaxed);
        return _root;
    }

    void reset_calls() { _root_location_calls.store(0, std::memory_order_relaxed); }
    int calls() const { return _root_location_calls.load(std::memory_order_relaxed); }

private:
    std::string _root;
    mutable std::atomic<int> _root_location_calls{0};
};

struct HttpResponse {
    long status = 0;
    std::string content_type;
    std::string body;
};

class ScopedFd {
public:
    explicit ScopedFd(int fd = -1) : _fd(fd) {}
    ~ScopedFd() {
        if (_fd >= 0) {
            ::close(_fd);
        }
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept : _fd(std::exchange(other._fd, -1)) {}
    ScopedFd& operator=(ScopedFd&&) = delete;

    int get() const { return _fd; }
    void close() {
        if (_fd >= 0) {
            ::close(_fd);
            _fd = -1;
        }
    }

private:
    int _fd;
};

class DumpTabletMetadataHttpIntegrationTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_enable_http_auth = config::enable_http_auth;
        config::enable_http_auth = false;
        _root = "/tmp/starrocks-dump-tablet-metadata-http-" + std::to_string(getpid());
        (void)fs::remove_all(_root);
        ASSERT_OK(fs::create_directories(join_path(_root, kMetadataDirectoryName)));
        _provider = std::make_shared<CountingLocationProvider>(_root);
        _tablet_manager = std::make_unique<TabletManager>(_provider, 1);

        TabletMetadataPB metadata;
        metadata.set_id(kTabletId);
        metadata.set_version(kVersion);
        metadata.mutable_schema()->set_id(700);
        metadata.mutable_schema()->set_keys_type(DUP_KEYS);
        ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));

        _action = std::make_unique<DumpTabletMetadataAction>(nullptr, _tablet_manager.get());
        _server = std::make_unique<EvHttpServer>("127.0.0.1", 0, 1);
        ASSERT_TRUE(_server->register_handler(GET, "/api/cloudnative/dump_tablet_metadata/{TabletId}", _action.get()));
        _server->set_auth_verifier([this](HttpRequest*, HttpHandler::RequiredPrivilege privilege,
                                          bool always_require_auth) -> std::optional<EvHttpServer::AuthVerifyFailure> {
            _auth_calls.fetch_add(1, std::memory_order_relaxed);
            _last_privilege.store(static_cast<int>(privilege), std::memory_order_relaxed);
            _last_always_require_auth.store(always_require_auth, std::memory_order_relaxed);
            if (!_reject_auth.load(std::memory_order_relaxed)) {
                return std::nullopt;
            }
            EvHttpServer::AuthVerifyFailure failure;
            failure.http_status = HttpStatus::UNAUTHORIZED;
            failure.www_authenticate = "Basic realm=\"\"";
            failure.body = R"({"code":"AUTH_REJECTED","message":"denied by test verifier"})";
            return failure;
        });
        ASSERT_OK(_server->start());
        _server_started = true;
        _base_url = "http://127.0.0.1:" + std::to_string(_server->get_real_port());
        _provider->reset_calls();
    }

    void TearDown() override {
        if (_server_started) {
            _server->stop();
            _server->join();
        }
        _server.reset();
        _action.reset();
        _tablet_manager.reset();
        _provider.reset();
        (void)fs::remove_all(_root);
        config::enable_http_auth = _saved_enable_http_auth;
    }

    StatusOr<HttpResponse> get(std::string_view path_and_query) {
        HttpClient client;
        Status status = client.init(_base_url + std::string(path_and_query));
        if (!status.ok()) {
            return status;
        }
        client.set_method(GET);
        client.set_fail_on_error(false);
        client.set_timeout_ms(5000);
        HttpResponse response;
        status = client.execute(&response.body);
        if (!status.ok()) {
            return status;
        }
        response.status = client.get_http_status();
        response.content_type = client.get_response_content_type();
        return response;
    }

    StatusOr<ScopedFd> begin_undrained_get(std::string_view path_and_query) {
        ScopedFd fd(::socket(AF_INET, SOCK_STREAM, 0));
        if (fd.get() < 0) {
            return Status::InternalError("failed to create localhost test socket");
        }
        int receive_buffer_bytes = 1024;
        if (::setsockopt(fd.get(), SOL_SOCKET, SO_RCVBUF, &receive_buffer_bytes, sizeof(receive_buffer_bytes)) != 0) {
            return Status::InternalError("failed to bound localhost test receive buffer");
        }
        struct timeval timeout {};
        timeout.tv_sec = 5;
        if (::setsockopt(fd.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
            return Status::InternalError("failed to set localhost test send timeout");
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(_server->get_real_port());
        if (::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1 ||
            ::connect(fd.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            return Status::InternalError("failed to connect localhost test socket");
        }
        const std::string request =
                "GET " + std::string(path_and_query) + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
        size_t sent = 0;
        while (sent < request.size()) {
            const ssize_t bytes = ::send(fd.get(), request.data() + sent, request.size() - sent, 0);
            if (bytes <= 0) {
                return Status::InternalError("failed to write localhost test request");
            }
            sent += static_cast<size_t>(bytes);
        }
        return std::move(fd);
    }

    bool wait_for_provider_call(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (_provider->calls() > 0) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return _provider->calls() > 0;
    }

    static constexpr int64_t kTabletId = 11979;
    static constexpr int64_t kVersion = 23;

    bool _saved_enable_http_auth = false;
    bool _server_started = false;
    std::string _root;
    std::string _base_url;
    std::shared_ptr<CountingLocationProvider> _provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<DumpTabletMetadataAction> _action;
    std::unique_ptr<EvHttpServer> _server;
    std::atomic<int> _auth_calls{0};
    std::atomic<int> _last_privilege{static_cast<int>(HttpHandler::RequiredPrivilege::NONE)};
    std::atomic<bool> _last_always_require_auth{false};
    std::atomic<bool> _reject_auth{false};
};

TEST_F(DumpTabletMetadataHttpIntegrationTest, RealRouteAndQueryParsingEnforceTheExactContract) {
    ASSERT_FALSE(config::enable_http_auth);
    auto valid_or = get("/api/cloudnative/dump_tablet_metadata/11979?version=23&is_bundle=false");
    ASSERT_TRUE(valid_or.ok()) << valid_or.status();
    auto valid = std::move(valid_or).value();
    EXPECT_EQ(200, valid.status);
    EXPECT_EQ("application/json", valid.content_type);
    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(valid.body.data(), valid.body.size()).HasParseError()) << valid.body;
    ASSERT_TRUE(document.HasMember("metadata"));
    EXPECT_EQ(kTabletId, document["metadata"]["id"].GetInt64());
    EXPECT_EQ(kVersion, document["metadata"]["version"].GetInt64());
    EXPECT_EQ(1, _auth_calls.load(std::memory_order_relaxed));
    EXPECT_TRUE(_last_always_require_auth.load(std::memory_order_relaxed));
    EXPECT_EQ(static_cast<int>(HttpHandler::RequiredPrivilege::OPERATE),
              _last_privilege.load(std::memory_order_relaxed));
    EXPECT_GT(_provider->calls(), 0);

    struct RejectedUri {
        const char* path_and_query;
        const char* description;
    };
    const std::array<RejectedUri, 3> rejected = {{
            {"/api/cloudnative/dump_tablet_metadata/11979?version=23&version=24&is_bundle=false", "duplicate version"},
            {"/api/cloudnative/dump_tablet_metadata/11979?version=23&is_bundle=false&pretty=true", "unknown query"},
            {"/api/cloudnative/dump_tablet_metadata/11979?version=23&is_bundle=false&TabletId=999",
             "route shadowing query"},
    }};
    for (const auto& test_case : rejected) {
        SCOPED_TRACE(test_case.description);
        const int auth_calls_before = _auth_calls.load(std::memory_order_relaxed);
        _provider->reset_calls();
        auto response_or = get(test_case.path_and_query);
        ASSERT_TRUE(response_or.ok()) << response_or.status();
        auto response = std::move(response_or).value();
        EXPECT_EQ(400, response.status);
        EXPECT_EQ(R"({"code":"INVALID_ARGUMENT","message":"invalid diagnostic request"})", response.body);
        EXPECT_EQ(auth_calls_before + 1, _auth_calls.load(std::memory_order_relaxed));
        EXPECT_EQ(0, _provider->calls());
    }

    const int auth_calls_before = _auth_calls.load(std::memory_order_relaxed);
    _provider->reset_calls();
    auto no_route_or = get("/api/cloudnative/dump_tablet_metadata/11979/extra?version=23&is_bundle=false");
    ASSERT_TRUE(no_route_or.ok()) << no_route_or.status();
    auto no_route = std::move(no_route_or).value();
    EXPECT_EQ(404, no_route.status);
    EXPECT_EQ(auth_calls_before, _auth_calls.load(std::memory_order_relaxed));
    EXPECT_EQ(0, _provider->calls());
}

TEST_F(DumpTabletMetadataHttpIntegrationTest, ForcedOperateAuthRejectsBeforeActionOrStorage) {
    ASSERT_FALSE(config::enable_http_auth);
    _reject_auth.store(true, std::memory_order_relaxed);
    _provider->reset_calls();
    auto rejected_or = get("/api/cloudnative/dump_tablet_metadata/11979?version=23&is_bundle=false");
    ASSERT_TRUE(rejected_or.ok()) << rejected_or.status();
    auto rejected = std::move(rejected_or).value();
    EXPECT_EQ(401, rejected.status);
    EXPECT_EQ(R"({"code":"AUTH_REJECTED","message":"denied by test verifier"})", rejected.body);
    EXPECT_EQ(1, _auth_calls.load(std::memory_order_relaxed));
    EXPECT_TRUE(_last_always_require_auth.load(std::memory_order_relaxed));
    EXPECT_EQ(static_cast<int>(HttpHandler::RequiredPrivilege::OPERATE),
              _last_privilege.load(std::memory_order_relaxed));
    EXPECT_EQ(0, _provider->calls());

    _reject_auth.store(false, std::memory_order_relaxed);
    auto admitted_or = get("/api/cloudnative/dump_tablet_metadata/11979?version=23&is_bundle=false");
    ASSERT_TRUE(admitted_or.ok()) << admitted_or.status();
    EXPECT_EQ(200, admitted_or->status);
    EXPECT_GT(_provider->calls(), 0);
}

TEST_F(DumpTabletMetadataHttpIntegrationTest, PermitLivesUntilRealUndrainedResponseConnectionIsFreed) {
    constexpr int64_t kLargeTabletId = 12000;
    constexpr int64_t kLargeVersion = 24;
    TabletMetadataPB metadata;
    metadata.set_id(kLargeTabletId);
    metadata.set_version(kLargeVersion);
    auto* column = metadata.mutable_schema()->add_column();
    column->set_unique_id(1);
    column->set_name("large_diagnostic_fixture");
    column->set_type("VARCHAR");
    column->set_default_value(std::string(8 << 20, 'x'));
    ASSERT_OK(_tablet_manager->put_tablet_metadata(metadata));

    _provider->reset_calls();
    auto held_or = begin_undrained_get("/api/cloudnative/dump_tablet_metadata/12000?version=24&is_bundle=false");
    ASSERT_TRUE(held_or.ok()) << held_or.status();
    auto held = std::move(held_or).value();
    ASSERT_TRUE(wait_for_provider_call(std::chrono::seconds(5)));

    const int calls_before_busy = _provider->calls();
    auto busy_or = get("/api/cloudnative/dump_tablet_metadata/11979?version=23&is_bundle=false");
    ASSERT_TRUE(busy_or.ok()) << busy_or.status();
    EXPECT_EQ(503, busy_or->status);
    EXPECT_EQ(R"({"code":"DIAGNOSTIC_BUSY","message":"another tablet metadata diagnostic is active"})", busy_or->body);
    EXPECT_EQ(calls_before_busy, _provider->calls());
    const int calls_while_held = _provider->calls();

    held.close();
    bool admitted = false;
    for (int attempt = 0; attempt < 100 && !admitted; ++attempt) {
        auto response_or = get("/api/cloudnative/dump_tablet_metadata/11979?version=23&is_bundle=false");
        ASSERT_TRUE(response_or.ok()) << response_or.status();
        if (response_or->status == 200) {
            admitted = true;
            break;
        }
        ASSERT_EQ(503, response_or->status);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(admitted);
    EXPECT_GT(_provider->calls(), calls_while_held);
}

} // namespace
} // namespace starrocks::lake
