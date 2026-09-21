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

#include "http/action/publish_property_action.h"

#include <event2/http.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <memory>
#include <string>
#include <utility>

#include "base/testutil/assert.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_service.pb.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/pk_publish_config.h"

namespace starrocks {
extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);

namespace {

HttpStatus g_response_status = HttpStatus::OK;
std::string g_response_body;

void capture_reply(HttpRequest* request, HttpStatus status, std::string_view body) {
    g_response_status = status;
    g_response_body.assign(body);
}

// One request built without a server behind it, so the handler can be called directly.
struct SyntheticRequest {
    explicit SyntheticRequest(PublishPropertyAction* action) {
        ev_req = evhttp_request_new(nullptr, nullptr);
        request = std::make_unique<HttpRequest>(ev_req);
        request->set_handler(action);
        request->set_method(HttpMethod::GET);
    }

    SyntheticRequest(const SyntheticRequest&) = delete;
    SyntheticRequest& operator=(const SyntheticRequest&) = delete;

    ~SyntheticRequest() {
        request.reset();
        if (ev_req != nullptr) {
            evhttp_request_free(ev_req);
        }
    }

    // What a caller writes in the query string: HttpRequest folds those into the same map the handler
    // reads, so setting them here is what arriving over the wire does.
    void add_param(const std::string& key, const std::string& value) { request->add_param(key, value); }

    evhttp_request* ev_req = nullptr;
    std::unique_ptr<HttpRequest> request;
};

} // namespace

// Covers the endpoint's own decisions -- which parameters it accepts, and what it answers when the
// index it is asked about is not in memory -- against a tablet manager this fixture owns. What the
// values mean once parsed belongs to PkPublishConfigTest; what a real publish delivers belongs to the
// SQL test.
class PublishPropertyActionTest : public testing::Test {
protected:
    void SetUp() override {
        _root = "/tmp/starrocks-publish-property-action-" + std::to_string(getpid());
        (void)fs::remove_all(_root);
        _location_provider = std::make_shared<lake::FixedLocationProvider>(_root);
        ASSERT_OK(fs::create_directories(_location_provider->metadata_root_location(1)));
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 16384);
        _action = std::make_unique<PublishPropertyAction>(_tablet_manager.get());
        g_response_status = HttpStatus::INTERNAL_SERVER_ERROR;
        g_response_body.clear();
        s_injected_send_reply = capture_reply;
    }

    void TearDown() override {
        s_injected_send_reply = nullptr;
        _action.reset();
        _tablet_manager.reset();
        _update_manager.reset();
        _mem_tracker.reset();
        _location_provider.reset();
        (void)fs::remove_all(_root);
    }

    // Puts an index in memory for |tablet_id| holding |property|, which is the state a publish leaves
    // behind and the only state this endpoint can report.
    void load_index_with(int64_t tablet_id, const PublishPropertyPB& property) {
        auto* entry = _update_manager->index_cache().get_or_create(tablet_id);
        entry->value().update_publish_config(property);
        _update_manager->index_cache().release(entry);
    }

    // Every reply is HTTP 200 and JSON; a caller reads the verdict from `status`, never the status line.
    rapidjson::Document ask(const std::vector<std::pair<std::string, std::string>>& params) {
        SyntheticRequest holder(_action.get());
        for (const auto& [key, value] : params) {
            holder.add_param(key, value);
        }
        _action->handle(holder.request.get());
        EXPECT_EQ(HttpStatus::OK, g_response_status);
        auto* headers = evhttp_request_get_output_headers(holder.ev_req);
        EXPECT_STREQ("application/json", evhttp_find_header(headers, "Content-Type"));
        rapidjson::Document document;
        document.Parse(g_response_body.data(), g_response_body.size());
        EXPECT_FALSE(document.HasParseError()) << g_response_body;
        return document;
    }

    static std::string status_of(const rapidjson::Document& document) {
        EXPECT_TRUE(document.HasMember("status"));
        return document["status"].GetString();
    }

    static std::string message_of(const rapidjson::Document& document) {
        EXPECT_TRUE(document.HasMember("message"));
        return document["message"].GetString();
    }

    std::string _root;
    std::shared_ptr<lake::FixedLocationProvider> _location_provider;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::TabletManager> _tablet_manager;
    std::unique_ptr<PublishPropertyAction> _action;
};

// `type` names the consumer to ask, so a request that omits it or names one this release does not have
// is answered rather than guessed at. The reply for an unknown one says which names exist, so a caller
// who guessed wrong does not have to read the source to find out.
TEST_F(PublishPropertyActionTest, test_type_parameter) {
    auto missing = ask({});
    EXPECT_EQ("INVALID_ARGUMENT", status_of(missing));
    EXPECT_NE(std::string::npos, message_of(missing).find("type"));

    auto unknown = ask({{"type", "nosuch"}});
    EXPECT_EQ("INVALID_ARGUMENT", status_of(unknown));
    EXPECT_NE(std::string::npos, message_of(unknown).find("nosuch"));
    EXPECT_NE(std::string::npos, message_of(unknown).find("pk"));
}

// The primary key index keeps one of these per tablet, so `pk` needs a tablet id, and only a number
// that could name a tablet is one. Zero and negatives are rejected here rather than looked up.
TEST_F(PublishPropertyActionTest, test_tablet_id_parameter) {
    for (const auto& raw : {std::string(""), std::string("abc"), std::string("12x"), std::string("0"),
                            std::string("-1"), std::string("9223372036854775808")}) {
        std::vector<std::pair<std::string, std::string>> params{{"type", "pk"}};
        if (!raw.empty()) {
            params.emplace_back("tablet_id", raw);
        }
        auto document = ask(params);
        EXPECT_EQ("INVALID_ARGUMENT", status_of(document)) << "tablet_id='" << raw << "'";
        EXPECT_NE(std::string::npos, message_of(document).find("tablet_id")) << "tablet_id='" << raw << "'";
    }
}

// A publish property lives on the loaded index and is never written down, so a tablet whose index is
// not in memory has no answer rather than an empty one. The message has to say that much, or
// "not found" reads as "no such tablet" and sends the reader looking for a tablet that does exist.
TEST_F(PublishPropertyActionTest, test_index_not_in_memory) {
    auto document = ask({{"type", "pk"}, {"tablet_id", "999999999"}});
    EXPECT_EQ("NOT_FOUND", status_of(document));
    const std::string message = message_of(document);
    EXPECT_NE(std::string::npos, message.find("999999999"));
    EXPECT_NE(std::string::npos, message.find("not in memory"));
    EXPECT_NE(std::string::npos, message.find("never"));
    EXPECT_FALSE(document.HasMember("revision"));
    EXPECT_FALSE(document.HasMember("properties"));
}

// The answer carries the revision the tablet is running with and only what the table set: a property
// left unset is answered at every read from this node's configuration, which is not state the index
// holds and so not something this can report.
TEST_F(PublishPropertyActionTest, test_reports_what_the_table_set) {
    PublishPropertyPB property;
    property.set_revision(7);
    (*property.mutable_properties())["pk_index_memtable_max_count"] = "8";
    (*property.mutable_properties())["pk_rows_mapper_read_parallelism"] = "16";
    load_index_with(10086, property);

    auto document = ask({{"type", "pk"}, {"tablet_id", "10086"}});
    EXPECT_EQ("OK", status_of(document));
    // Empty rather than absent, so a caller reads one field on every reply instead of two shapes.
    EXPECT_EQ("", message_of(document));
    ASSERT_TRUE(document.HasMember("revision"));
    EXPECT_EQ(7, document["revision"].GetInt64());

    ASSERT_TRUE(document.HasMember("properties"));
    const auto& properties = document["properties"];
    ASSERT_TRUE(properties.IsObject());
    EXPECT_EQ(2, properties.MemberCount());
    ASSERT_TRUE(properties.HasMember("pk_index_memtable_max_count"));
    EXPECT_EQ(8, properties["pk_index_memtable_max_count"].GetInt64());
    ASSERT_TRUE(properties.HasMember("pk_rows_mapper_read_parallelism"));
    EXPECT_EQ(16, properties["pk_rows_mapper_read_parallelism"].GetInt64());
    EXPECT_FALSE(properties.HasMember("pk_index_memtable_max_bytes"));
}

// A table that has never set one still has an index, and the honest answer is the set it holds -- empty
// -- at the revision a table starts at, not "not found": the index IS in memory, which is the thing
// NOT_FOUND is reserved for.
TEST_F(PublishPropertyActionTest, test_reports_a_table_that_set_nothing) {
    load_index_with(10087, PublishPropertyPB());

    auto document = ask({{"type", "pk"}, {"tablet_id", "10087"}});
    EXPECT_EQ("OK", status_of(document));
    EXPECT_EQ("", message_of(document));
    ASSERT_TRUE(document.HasMember("revision"));
    EXPECT_EQ(0, document["revision"].GetInt64());
    ASSERT_TRUE(document.HasMember("properties"));
    EXPECT_EQ(0, document["properties"].MemberCount());
}

} // namespace starrocks
