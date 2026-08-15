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
#include <glog/logging.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/url_coding.h"
#include "common/config_lake_fwd.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "http/action/lake/dump_tablet_metadata_serializer.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/storage_env.h"

namespace starrocks {
extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);
} // namespace starrocks

namespace starrocks::lake {
namespace dump_tablet_metadata_internal {
bool contains_encryption_metadata(const google::protobuf::Message& message);
void redact_encryption_metadata(google::protobuf::Message* message, std::set<std::string>* redacted_fields);
} // namespace dump_tablet_metadata_internal

namespace {

HttpStatus g_response_status = HttpStatus::OK;
std::string g_response_body;

void capture_reply(HttpRequest* request, HttpStatus status, std::string_view body) {
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

    void set_route(std::string tablet_id) { request->add_param("TabletId", tablet_id); }

    void add_query(std::string key, std::string value) {
        request->_query_params.emplace(std::move(key), std::move(value));
    }

    evhttp_request* ev_req = nullptr;
    std::unique_ptr<HttpRequest> request;
};

SyntheticRequest valid_request(DumpTabletMetadataAction* action, std::string tablet_id = "17",
                               std::string version = "2") {
    SyntheticRequest holder(action);
    holder.set_route(std::move(tablet_id));
    holder.add_query("version", std::move(version));
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

class ScopedDeathTestStyle {
public:
    ScopedDeathTestStyle() : _previous(::testing::FLAGS_gtest_death_test_style) {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
    ~ScopedDeathTestStyle() { ::testing::FLAGS_gtest_death_test_style = _previous; }

private:
    std::string _previous;
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

TEST(DumpTabletMetadataSerializerTest, RedactsEveryReachableEncryptionFieldWithoutMutatingInput) {
    std::vector<std::string> secrets;
    TabletMetadataPB metadata = metadata_with_all_encryption_fields(&secrets);
    const std::string original = metadata.SerializeAsString();

    auto result = serialize_dump_tablet_metadata(metadata, 1 << 20);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(original, metadata.SerializeAsString());

    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(result->data(), result->size()).HasParseError());
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
    EXPECT_EQ(std::string::npos, result->find('\n'));
    rapidjson::StringBuffer compact;
    rapidjson::Writer<rapidjson::StringBuffer> writer(compact);
    ASSERT_TRUE(document.Accept(writer));
    EXPECT_EQ(*result, std::string(compact.GetString(), compact.GetSize()));
    for (const auto& secret : secrets) {
        std::string encoded;
        base64_encode(secret, &encoded);
        EXPECT_EQ(std::string::npos, result->find(secret));
        EXPECT_EQ(std::string::npos, result->find(encoded));
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

    EXPECT_FALSE(dump_tablet_metadata_internal::contains_encryption_metadata(*message));

    reflection->SetString(message.get(), text_secret, "future-string-secret");
    reflection->AddInt64(message.get(), repeated_secret, 9007199254740993LL);
    auto* matching_message = reflection->MutableMessage(message.get(), message_secret);
    matching_message->GetReflection()->SetString(matching_message, nested_safe, "future-message-secret");
    auto* ordinary_message = reflection->MutableMessage(message.get(), safe_nested);
    ordinary_message->GetReflection()->SetString(ordinary_message, nested_secret, "nested-secret");
    ordinary_message->GetReflection()->SetString(ordinary_message, nested_safe, "keep");

    EXPECT_TRUE(dump_tablet_metadata_internal::contains_encryption_metadata(*message));

    std::set<std::string> redacted_fields;
    dump_tablet_metadata_internal::redact_encryption_metadata(message.get(), &redacted_fields);

    EXPECT_FALSE(dump_tablet_metadata_internal::contains_encryption_metadata(*message));

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
    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(result->data(), result->size()).HasParseError());
    EXPECT_TRUE(document.HasMember("metadata"));
    EXPECT_FALSE(document.HasMember("redacted_fields"));
    EXPECT_FALSE(document["metadata"].IsArray());
    EXPECT_EQ(std::string::npos, result->find('\n'));
}

TEST(DumpTabletMetadataSerializerTest, AppliesCapToCompleteEnvelopeAtExactBoundary) {
    std::vector<std::string> secrets;
    TabletMetadataPB metadata = metadata_with_all_encryption_fields(&secrets);
    auto baseline_or = serialize_dump_tablet_metadata(metadata, 1 << 20);
    ASSERT_TRUE(baseline_or.ok()) << baseline_or.status();
    auto baseline = std::move(baseline_or).value();
    const size_t size = baseline.size();
    ASSERT_GT(size, 1);

    auto under = serialize_dump_tablet_metadata(metadata, size - 1);
    ASSERT_FALSE(under.ok());
    EXPECT_TRUE(under.status().is_capacity_limit_exceeded()) << under.status();

    auto exact_or = serialize_dump_tablet_metadata(metadata, size);
    ASSERT_TRUE(exact_or.ok()) << exact_or.status();
    auto exact = std::move(exact_or).value();
    EXPECT_EQ(size, exact.size());
    EXPECT_EQ(baseline, exact);

    auto over_or = serialize_dump_tablet_metadata(metadata, size + 1);
    ASSERT_TRUE(over_or.ok()) << over_or.status();
    auto over = std::move(over_or).value();
    EXPECT_EQ(baseline, over);
}

class DumpTabletMetadataActionTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_memory_limit = config::lake_dump_tablet_metadata_per_request_memory_limit_bytes;
        _saved_json_limit = config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes;
        _saved_max_concurrency = config::lake_dump_tablet_metadata_max_concurrency;
        config::lake_dump_tablet_metadata_per_request_memory_limit_bytes = 256LL << 20;
        config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes = 32LL << 20;
        config::lake_dump_tablet_metadata_max_concurrency = 1;
        _root = "/tmp/starrocks-dump-tablet-metadata-action-" + std::to_string(getpid());
        (void)fs::remove_all(_root);
        ASSERT_OK(fs::create_directories(join_path(_root, kMetadataDirectoryName)));
        _provider = std::make_shared<FixedLocationProvider>(_root);
        _tablet_manager = std::make_unique<TabletManager>(_provider, 1LL << 30);
        s_injected_send_reply = capture_reply;
    }

    void TearDown() override {
        s_injected_send_reply = nullptr;
        _tablet_manager.reset();
        _provider.reset();
        (void)fs::remove_all(_root);
        config::lake_dump_tablet_metadata_per_request_memory_limit_bytes = _saved_memory_limit;
        config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes = _saved_json_limit;
        config::lake_dump_tablet_metadata_max_concurrency = _saved_max_concurrency;
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
    int64_t _saved_memory_limit = 0;
    int64_t _saved_json_limit = 0;
    int32_t _saved_max_concurrency = 0;
};

TEST_F(DumpTabletMetadataActionTest, RejectsEveryInputOutsideTheExactCacheReadContractBeforeAdmission) {
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    struct InvalidRequest {
        const char* tablet_id;
        const char* version;
        const char* unexpected_query = nullptr;
    };
    const std::vector<InvalidRequest> invalid = {
            {nullptr, "2"},
            {"", "2"},
            {"+1", "2"},
            {" 1", "2"},
            {"0", "2"},
            {"-1", "2"},
            {"0x10", "2"},
            {"9223372036854775808", "2"},
            {"17", nullptr},
            {"17", ""},
            {"17", "+2"},
            {"17", " 2"},
            {"17", "0"},
            {"17", "-2"},
            {"17", "0x2"},
            {"17", "9223372036854775808"},
            {"17", "2", "is_bundle"},
            {"17", "2", "pretty"},
    };

    for (const auto& test_case : invalid) {
        SCOPED_TRACE(
                testing::Message() << "tablet=" << (test_case.tablet_id == nullptr ? "<missing>" : test_case.tablet_id)
                                   << " version=" << (test_case.version == nullptr ? "<missing>" : test_case.version)
                                   << " unexpected_query="
                                   << (test_case.unexpected_query == nullptr ? "<none>" : test_case.unexpected_query));
        SyntheticRequest request(&action);
        if (test_case.tablet_id != nullptr) {
            request.set_route(test_case.tablet_id);
        }
        if (test_case.version != nullptr) {
            request.add_query("version", test_case.version);
        }
        if (test_case.unexpected_query != nullptr) {
            request.add_query(test_case.unexpected_query, "true");
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

TEST_F(DumpTabletMetadataActionTest, ReturnsDirectlyCachedRequestedMetadataAndSecurityHeaders) {
    constexpr int64_t kTabletId = 11979;
    constexpr int64_t kVersion = 23;
    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(kTabletId);
    metadata->set_version(kVersion);
    metadata->mutable_schema()->set_id(702);
    metadata->mutable_schema()->set_keys_type(DUP_KEYS);
    _tablet_manager->metacache()->cache_tablet_metadata(_tablet_manager->tablet_metadata_location(kTabletId, kVersion),
                                                        metadata);
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion));

    ASSERT_EQ(0, action.on_header(request.request.get()));
    action.handle(request.request.get());

    ASSERT_EQ(HttpStatus::OK, g_response_status);
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

TEST_F(DumpTabletMetadataActionTest, DoesNotApplyRetiredProtobufSizeLimit) {
    constexpr int64_t kTabletId = 11979;
    constexpr int64_t kVersion = 23;
    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(kTabletId);
    metadata->set_version(kVersion);
    metadata->GetReflection()
            ->MutableUnknownFields(metadata.get())
            ->AddLengthDelimited(9999, std::string(17 << 20, 'x'));
    ASSERT_GT(metadata->ByteSizeLong(), 16 << 20);
    _tablet_manager->metacache()->cache_tablet_metadata(_tablet_manager->tablet_metadata_location(kTabletId, kVersion),
                                                        metadata);
    ASSERT_NE(nullptr, _tablet_manager->metacache()->lookup_tablet_metadata(
                               _tablet_manager->tablet_metadata_location(kTabletId, kVersion)));
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion));

    ASSERT_EQ(0, action.on_header(request.request.get()));
    action.handle(request.request.get());

    ASSERT_EQ(HttpStatus::OK, g_response_status);
    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(g_response_body.data(), g_response_body.size()).HasParseError()) << g_response_body;
    EXPECT_EQ(kTabletId, document["metadata"]["id"].GetInt64());
    EXPECT_EQ(kVersion, document["metadata"]["version"].GetInt64());
}

TEST_F(DumpTabletMetadataActionTest, SnapshotsMutableJsonLimitForEachRequest) {
    constexpr int64_t kTabletId = 11979;
    constexpr int64_t kVersion = 23;
    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(kTabletId);
    metadata->set_version(kVersion);
    _tablet_manager->metacache()->cache_tablet_metadata(_tablet_manager->tablet_metadata_location(kTabletId, kVersion),
                                                        metadata);
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());

    config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes = 1024;
    auto admitted = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion));
    ASSERT_EQ(0, action.on_header(admitted.request.get()));
    config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes = 1;
    action.handle(admitted.request.get());
    EXPECT_EQ(HttpStatus::OK, g_response_status);

    admitted.request.reset();
    auto limited = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion));
    ASSERT_EQ(0, action.on_header(limited.request.get()));
    action.handle(limited.request.get());
    EXPECT_EQ(HttpStatus::BAD_REQUEST, g_response_status);
    EXPECT_EQ(R"({"code":"METADATA_TOO_LARGE","message":"tablet metadata exceeds a diagnostic limit"})",
              g_response_body);
}

TEST_F(DumpTabletMetadataActionTest, RejectsNonPositiveResourceConfigurationWithoutAdmissionLeak) {
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    enum class ConfigUnderTest { kMemory, kJson, kConcurrency };
    for (const auto config_under_test :
         {ConfigUnderTest::kMemory, ConfigUnderTest::kJson, ConfigUnderTest::kConcurrency}) {
        config::lake_dump_tablet_metadata_per_request_memory_limit_bytes = 256LL << 20;
        config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes = 32LL << 20;
        config::lake_dump_tablet_metadata_max_concurrency = 1;
        switch (config_under_test) {
        case ConfigUnderTest::kMemory:
            config::lake_dump_tablet_metadata_per_request_memory_limit_bytes = 0;
            break;
        case ConfigUnderTest::kJson:
            config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes = 0;
            break;
        case ConfigUnderTest::kConcurrency:
            config::lake_dump_tablet_metadata_max_concurrency = 0;
            break;
        }

        auto rejected = valid_request(&action);
        EXPECT_EQ(-1, action.on_header(rejected.request.get()));
        EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE, g_response_status);
        EXPECT_EQ(R"({"code":"DIAGNOSTIC_UNAVAILABLE","message":"tablet metadata diagnostic is unavailable"})",
                  g_response_body);
        EXPECT_EQ(nullptr, rejected.request->handler_ctx());

        config::lake_dump_tablet_metadata_per_request_memory_limit_bytes = 256LL << 20;
        config::lake_dump_tablet_metadata_per_request_json_size_limit_bytes = 32LL << 20;
        config::lake_dump_tablet_metadata_max_concurrency = 1;
        auto accepted = valid_request(&action);
        EXPECT_EQ(0, action.on_header(accepted.request.get()));
        EXPECT_NE(nullptr, accepted.request->handler_ctx());
    }
}

TEST_F(DumpTabletMetadataActionTest, ReturnsCacheScopeErrorWhenDurableMetadataIsNotCached) {
    constexpr int64_t kTabletId = 11979;
    constexpr int64_t kVersion = 23;
    put_metadata(kTabletId, kVersion);
    const std::string key = _tablet_manager->tablet_metadata_location(kTabletId, kVersion);
    _tablet_manager->metacache()->erase(key);
    ASSERT_EQ(nullptr, _tablet_manager->metacache()->lookup_tablet_metadata(key));
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion));

    ASSERT_EQ(0, action.on_header(request.request.get()));
    action.handle(request.request.get());

    EXPECT_EQ(HttpStatus::NOT_FOUND, g_response_status);
    EXPECT_EQ(
            R"({"code":"METADATA_NOT_CACHED","message":"tablet metadata is not cached on this compute node; this API only inspects the current compute node's in-memory metadata cache. To inspect metadata in object storage, download the file with the AWS CLI and parse it with meta_tool"})",
            g_response_body);
    expect_diagnostic_headers(request.ev_req);
}

TEST_F(DumpTabletMetadataActionTest, ReturnsDiagnosticUnavailableWhenNoTabletManagerExists) {
    ScopedDeathTestStyle death_test_style;
    ASSERT_EXIT(
            {
                StorageEnv::GetInstance()->destroy();
                if (StorageEnv::GetInstance()->lake_tablet_manager() != nullptr) {
                    _exit(1);
                }
                DumpTabletMetadataAction action(nullptr);
                auto request = valid_request(&action);
                if (action.on_header(request.request.get()) != 0) {
                    _exit(1);
                }
                action.handle(request.request.get());
                _exit(g_response_status == HttpStatus::SERVICE_UNAVAILABLE &&
                                      g_response_body ==
                                              R"({"code":"DIAGNOSTIC_UNAVAILABLE","message":"tablet metadata diagnostic is unavailable"})"
                              ? 0
                              : 1);
            },
            ::testing::ExitedWithCode(0), "");
}

TEST_F(DumpTabletMetadataActionTest, AppliesMutableConcurrencyLimitToNewAdmissionsUntilRequestFree) {
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    config::lake_dump_tablet_metadata_max_concurrency = 1;
    auto first = std::make_unique<SyntheticRequest>(valid_request(&action));
    ASSERT_EQ(0, action.on_header(first->request.get()));
    action.handle(first->request.get());

    ScopedDiagnosticLogSink logs;
    auto second = valid_request(&action);
    EXPECT_EQ(-1, action.on_header(second.request.get()));
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE, g_response_status);
    EXPECT_EQ(R"({"code":"DIAGNOSTIC_BUSY","message":"tablet metadata diagnostic concurrency limit is reached"})",
              g_response_body);
    EXPECT_EQ(nullptr, second.request->handler_ctx());
    expect_diagnostic_headers(second.ev_req);
    EXPECT_TRUE(logs.sink().contains("result=DIAGNOSTIC_BUSY", "elapsed_ms="));

    config::lake_dump_tablet_metadata_max_concurrency = 2;
    auto third = std::make_unique<SyntheticRequest>(valid_request(&action));
    EXPECT_EQ(0, action.on_header(third->request.get()));
    EXPECT_NE(nullptr, third->request->handler_ctx());

    config::lake_dump_tablet_metadata_max_concurrency = 1;
    auto fourth = valid_request(&action);
    EXPECT_EQ(-1, action.on_header(fourth.request.get()));
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE, g_response_status);

    first.reset();
    auto fifth = valid_request(&action);
    EXPECT_EQ(-1, action.on_header(fifth.request.get()));

    third.reset();
    auto sixth = valid_request(&action);
    EXPECT_EQ(0, action.on_header(sixth.request.get()));
    EXPECT_NE(nullptr, sixth.request->handler_ctx());
}

} // namespace
} // namespace starrocks::lake
