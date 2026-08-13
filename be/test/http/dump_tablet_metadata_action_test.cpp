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
#include <google/protobuf/io/zero_copy_stream.h>
#include <gtest/gtest.h>
#include <json2pb/pb_to_json.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
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
#include "storage/protobuf_file.h"

namespace starrocks {
extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);
} // namespace starrocks

namespace starrocks::lake {
namespace dump_tablet_metadata_internal {
void redact_encryption_metadata(google::protobuf::Message* message, std::set<std::string>* redacted_fields);
}

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
        bool add_unknown = false;
    };
    const std::vector<InvalidRequest> invalid = {
            {nullptr, "2", "false"},  {"", "2", "false"},
            {"+1", "2", "false"},     {" 1", "2", "false"},
            {"0", "2", "false"},      {"-1", "2", "false"},
            {"0x10", "2", "false"},   {"9223372036854775808", "2", "false"},
            {"17", nullptr, "false"}, {"17", "", "false"},
            {"17", "+2", "false"},    {"17", " 2", "false"},
            {"17", "0", "false"},     {"17", "-2", "false"},
            {"17", "0x2", "false"},   {"17", "9223372036854775808", "false"},
            {"17", "2", nullptr},     {"17", "2", "TRUE"},
            {"17", "2", "False"},     {"17", "2", "false", true},
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
            request.add_query("version", test_case.version);
        }
        if (test_case.is_bundle != nullptr) {
            request.add_query("is_bundle", test_case.is_bundle);
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
        _tablet_manager = std::make_unique<TabletManager>(_provider, 1 << 20);
        _saved_max_object_size = config::lake_dump_tablet_metadata_max_object_size_bytes;
        s_injected_send_reply = capture_reply;
    }

    void TearDown() override {
        s_injected_send_reply = nullptr;
        config::lake_dump_tablet_metadata_max_object_size_bytes = _saved_max_object_size;
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

    void put_shared_initial_metadata(int64_t anchor_tablet_id) {
        TabletMetadataPB metadata;
        metadata.set_id(anchor_tablet_id);
        metadata.set_version(1);
        metadata.mutable_schema()->set_id(701);
        metadata.mutable_schema()->set_keys_type(DUP_KEYS);
        const std::string path = _tablet_manager->tablet_initial_metadata_location(anchor_tablet_id);
        ProtobufFileWithHeader file(path, LAKE_META_HEADER_MAGIC_NUMBER,
                                    /*allow_plain_protobuf_fallback=*/true);
        ASSERT_OK(file.save(metadata));
    }

    void put_bundle_metadata(int64_t tablet_id, int64_t version, int64_t historical_schema_id = 0) {
        std::map<int64_t, TabletMetadataPB> metadata_by_tablet;
        auto& metadata = metadata_by_tablet[tablet_id];
        metadata.set_id(tablet_id);
        metadata.set_version(version);
        metadata.mutable_schema()->set_id(703);
        metadata.mutable_schema()->set_keys_type(DUP_KEYS);
        if (historical_schema_id > 0) {
            auto& historical_schema = (*metadata.mutable_historical_schemas())[historical_schema_id];
            historical_schema.set_id(historical_schema_id);
            historical_schema.set_keys_type(UNIQUE_KEYS);
            (*metadata.mutable_rowset_to_schema())[11] = historical_schema_id;
        }
        ASSERT_OK(_tablet_manager->put_bundle_tablet_metadata(metadata_by_tablet));
    }

    std::string _root;
    std::shared_ptr<FixedLocationProvider> _provider;
    std::unique_ptr<TabletManager> _tablet_manager;
    int64_t _saved_max_object_size = 0;
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

TEST_F(DumpTabletMetadataActionStorageTest, ReadsBundledVersionOneFromSharedInitialObject) {
    constexpr int64_t kRequestedTabletId = 11979;
    constexpr int64_t kPhysicalAnchorTabletId = 11980;
    put_shared_initial_metadata(kPhysicalAnchorTabletId);
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto request = valid_request(&action, std::to_string(kRequestedTabletId), "1", "true");

    ASSERT_EQ(0, action.on_header(request.request.get()));
    action.handle(request.request.get());

    ASSERT_EQ(HttpStatus::OK, g_response_status) << g_response_body;
    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(g_response_body.data(), g_response_body.size()).HasParseError()) << g_response_body;
    ASSERT_TRUE(document.HasMember("metadata"));
    EXPECT_EQ(kRequestedTabletId, document["metadata"]["id"].GetInt64());
    EXPECT_EQ(1, document["metadata"]["version"].GetInt64());
}

TEST_F(DumpTabletMetadataActionStorageTest, DoesNotFallbackFromNonBundledVersionOneToSharedInitialObject) {
    constexpr int64_t kRequestedTabletId = 11979;
    put_shared_initial_metadata(11980);
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto request = valid_request(&action, std::to_string(kRequestedTabletId), "1", "false");

    ASSERT_EQ(0, action.on_header(request.request.get()));
    action.handle(request.request.get());

    EXPECT_EQ(HttpStatus::NOT_FOUND, g_response_status);
    EXPECT_EQ(R"({"code":"METADATA_NOT_FOUND","message":"tablet metadata is unavailable"})", g_response_body);
}

TEST_F(DumpTabletMetadataActionStorageTest, ReturnsExactMetadataCacheHitWithoutReadingPhysicalObject) {
    constexpr int64_t kTabletId = 11979;
    constexpr int64_t kVersion = 23;
    auto metadata = std::make_shared<TabletMetadataPB>();
    metadata->set_id(kTabletId);
    metadata->set_version(kVersion);
    metadata->mutable_schema()->set_id(702);
    metadata->mutable_schema()->set_keys_type(DUP_KEYS);
    _tablet_manager->metacache()->cache_tablet_metadata(_tablet_manager->tablet_metadata_location(kTabletId, kVersion),
                                                        metadata);
    config::lake_dump_tablet_metadata_max_object_size_bytes = 1;
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());

    // The existing metacache stores logical (tablet, version) metadata and has no
    // physical-layout provenance, so a hit is valid for either request layout.
    for (const char* is_bundle : {"false", "true"}) {
        auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion), is_bundle);
        ASSERT_EQ(0, action.on_header(request.request.get()));
        action.handle(request.request.get());

        ASSERT_EQ(HttpStatus::OK, g_response_status) << g_response_body;
        rapidjson::Document document;
        ASSERT_FALSE(document.Parse(g_response_body.data(), g_response_body.size()).HasParseError()) << g_response_body;
        ASSERT_TRUE(document.HasMember("metadata"));
        EXPECT_EQ(kTabletId, document["metadata"]["id"].GetInt64());
        EXPECT_EQ(kVersion, document["metadata"]["version"].GetInt64());
    }
}

TEST_F(DumpTabletMetadataActionStorageTest, PhysicalReadsDoNotPopulateMetadataCache) {
    constexpr int64_t kStandaloneTabletId = 11979;
    constexpr int64_t kBundleTabletId = 11980;
    constexpr int64_t kVersion = 23;
    put_metadata(kStandaloneTabletId, kVersion);
    put_bundle_metadata(kBundleTabletId, kVersion);
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());

    for (const auto& [tablet_id, is_bundle] :
         std::array<std::pair<int64_t, const char*>, 2>{{{kStandaloneTabletId, "false"}, {kBundleTabletId, "true"}}}) {
        const std::string logical_path = _tablet_manager->tablet_metadata_location(tablet_id, kVersion);
        _tablet_manager->metacache()->erase(logical_path);
        ASSERT_EQ(nullptr, _tablet_manager->metacache()->lookup_tablet_metadata(logical_path));

        auto request = valid_request(&action, std::to_string(tablet_id), std::to_string(kVersion), is_bundle);
        ASSERT_EQ(0, action.on_header(request.request.get()));
        action.handle(request.request.get());
        ASSERT_EQ(HttpStatus::OK, g_response_status) << g_response_body;
        EXPECT_EQ(nullptr, _tablet_manager->metacache()->lookup_tablet_metadata(logical_path));
    }
}

TEST_F(DumpTabletMetadataActionStorageTest, RestoresCurrentAndHistoricalSchemasFromBundle) {
    constexpr int64_t kTabletId = 11980;
    constexpr int64_t kVersion = 23;
    constexpr int64_t kHistoricalSchemaId = 704;
    put_bundle_metadata(kTabletId, kVersion, kHistoricalSchemaId);
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());
    auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion), "true");

    ASSERT_EQ(0, action.on_header(request.request.get()));
    action.handle(request.request.get());

    ASSERT_EQ(HttpStatus::OK, g_response_status) << g_response_body;
    rapidjson::Document document;
    ASSERT_FALSE(document.Parse(g_response_body.data(), g_response_body.size()).HasParseError()) << g_response_body;
    ASSERT_EQ(703, document["metadata"]["schema"]["id"].GetInt64());
    ASSERT_TRUE(document["metadata"]["historical_schemas"].IsArray());
    bool found_historical_schema = false;
    for (const auto& entry : document["metadata"]["historical_schemas"].GetArray()) {
        if (entry["key"].GetInt64() == kHistoricalSchemaId) {
            EXPECT_EQ(kHistoricalSchemaId, entry["value"]["id"].GetInt64());
            found_historical_schema = true;
        }
    }
    EXPECT_TRUE(found_historical_schema);
}

TEST_F(DumpTabletMetadataActionStorageTest, RejectsOversizedStandaloneAndBundleObjectsWithBadRequest) {
    constexpr int64_t kStandaloneTabletId = 11979;
    constexpr int64_t kBundleTabletId = 11980;
    constexpr int64_t kVersion = 23;
    put_metadata(kStandaloneTabletId, kVersion);
    _tablet_manager->metacache()->erase(_tablet_manager->tablet_metadata_location(kStandaloneTabletId, kVersion));
    put_bundle_metadata(kBundleTabletId, kVersion);
    config::lake_dump_tablet_metadata_max_object_size_bytes = 1;
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());

    for (const auto& [tablet_id, is_bundle] :
         std::array<std::pair<int64_t, const char*>, 2>{{{kStandaloneTabletId, "false"}, {kBundleTabletId, "true"}}}) {
        SCOPED_TRACE(testing::Message() << "tablet_id=" << tablet_id << " is_bundle=" << is_bundle);
        auto request = valid_request(&action, std::to_string(tablet_id), std::to_string(kVersion), is_bundle);
        ASSERT_EQ(0, action.on_header(request.request.get()));
        action.handle(request.request.get());
        EXPECT_EQ(HttpStatus::BAD_REQUEST, g_response_status);
        EXPECT_EQ(R"({"code":"METADATA_TOO_LARGE","message":"tablet metadata exceeds a diagnostic limit"})",
                  g_response_body);
    }
}

TEST_F(DumpTabletMetadataActionStorageTest, NonPositiveObjectLimitFailsClosedOnCacheMiss) {
    constexpr int64_t kTabletId = 11979;
    constexpr int64_t kVersion = 23;
    put_metadata(kTabletId, kVersion);
    _tablet_manager->metacache()->erase(_tablet_manager->tablet_metadata_location(kTabletId, kVersion));
    DumpTabletMetadataAction action(nullptr, _tablet_manager.get());

    for (const int64_t limit : {0, -1}) {
        config::lake_dump_tablet_metadata_max_object_size_bytes = limit;
        auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion), "false");
        ASSERT_EQ(0, action.on_header(request.request.get()));
        action.handle(request.request.get());
        EXPECT_EQ(HttpStatus::BAD_REQUEST, g_response_status);
        EXPECT_EQ(R"({"code":"METADATA_TOO_LARGE","message":"tablet metadata exceeds a diagnostic limit"})",
                  g_response_body);
    }
}

TEST_F(DumpTabletMetadataActionStorageTest, UsesStableSanitizedErrorsForMissingAndWrongLayout) {
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
        _tablet_manager->metacache()->erase(_tablet_manager->tablet_metadata_location(kTabletId, kVersion));
        auto request = valid_request(&action, std::to_string(kTabletId), std::to_string(kVersion), "true");
        ASSERT_EQ(0, action.on_header(request.request.get()));
        action.handle(request.request.get());
        EXPECT_EQ(HttpStatus::NOT_FOUND, g_response_status);
        EXPECT_EQ(R"({"code":"METADATA_NOT_FOUND","message":"tablet metadata is unavailable"})", g_response_body);
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

} // namespace
} // namespace starrocks::lake
