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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/exec/es_scan_reader_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/es/es_scan_reader.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "exec/es/es_scroll_query.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
DIAGNOSTIC_POP

namespace starrocks {

class RestSearchAction : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "root") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
        if (req->method() == HttpMethod::POST) {
            std::string post_body = req->get_request_body();
            rapidjson::Document post_doc;
            post_doc.Parse<0>(post_body.c_str());
            int size = 1;
            if (post_doc.HasMember("size")) {
                rapidjson::Value& size_value = post_doc["size"];
                size = size_value.GetInt();
            }
            std::string _scroll_id(std::to_string(size));
            rapidjson::Document search_result;
            rapidjson::Document::AllocatorType& allocator = search_result.GetAllocator();
            search_result.SetObject();
            rapidjson::Value scroll_id_value(_scroll_id.c_str(), allocator);
            search_result.AddMember("_scroll_id", scroll_id_value, allocator);

            rapidjson::Value outer_hits(rapidjson::kObjectType);
            outer_hits.AddMember("total", 10, allocator);
            rapidjson::Value inner_hits(rapidjson::kArrayType);
            rapidjson::Value source_docuement(rapidjson::kObjectType);
            source_docuement.AddMember("id", 1, allocator);
            rapidjson::Value value_node("1", allocator);
            source_docuement.AddMember("value", value_node, allocator);
            inner_hits.PushBack(source_docuement, allocator);
            outer_hits.AddMember("hits", inner_hits, allocator);
            search_result.AddMember("hits", outer_hits, allocator);

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            search_result.Accept(writer);
            //send DELETE scorll post request
            std::string search_result_json = buffer.GetString();
            HttpChannel::send_reply(req, search_result_json);
        } else {
            std::string response = "test1";
            HttpChannel::send_reply(req, response);
        }
    }
};

class RestSearchScrollAction : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "root") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        if (req->method() == HttpMethod::POST) {
            std::string post_body = req->get_request_body();
            rapidjson::Document post_doc;
            post_doc.Parse<0>(post_body.c_str());
            std::string scroll_id;
            if (!post_doc.HasMember("scroll_id")) {
                HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "invalid scroll request");
                return;
            } else {
                rapidjson::Value& scroll_id_value = post_doc["scroll_id"];
                scroll_id = scroll_id_value.GetString();
                int offset = atoi(scroll_id.c_str());
                if (offset > 10) {
                    rapidjson::Document end_search_result;
                    rapidjson::Document::AllocatorType& allocator = end_search_result.GetAllocator();
                    end_search_result.SetObject();
                    rapidjson::Value scroll_id_value("11", allocator);
                    end_search_result.AddMember("_scroll_id", scroll_id_value, allocator);

                    rapidjson::Value outer_hits(rapidjson::kObjectType);
                    outer_hits.AddMember("total", 0, allocator);
                    end_search_result.AddMember("hits", outer_hits, allocator);
                    rapidjson::StringBuffer buffer;
                    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                    end_search_result.Accept(writer);
                    //send DELETE scorll post request
                    std::string end_search_result_json = buffer.GetString();
                    HttpChannel::send_reply(req, end_search_result_json);
                    return;
                } else {
                    int start = offset + 1;
                    rapidjson::Document search_result;
                    rapidjson::Document::AllocatorType& allocator = search_result.GetAllocator();
                    search_result.SetObject();
                    rapidjson::Value scroll_id_value(std::to_string(start).c_str(), allocator);
                    search_result.AddMember("_scroll_id", scroll_id_value, allocator);

                    rapidjson::Value outer_hits(rapidjson::kObjectType);
                    outer_hits.AddMember("total", 1, allocator);
                    rapidjson::Value inner_hits(rapidjson::kArrayType);
                    rapidjson::Value source_docuement(rapidjson::kObjectType);
                    source_docuement.AddMember("id", start, allocator);
                    rapidjson::Value value_node(std::to_string(start).c_str(), allocator);
                    source_docuement.AddMember("value", value_node, allocator);
                    inner_hits.PushBack(source_docuement, allocator);
                    outer_hits.AddMember("hits", inner_hits, allocator);
                    search_result.AddMember("hits", outer_hits, allocator);

                    rapidjson::StringBuffer buffer;
                    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                    search_result.Accept(writer);
                    //send DELETE scorll post request
                    std::string search_result_json = buffer.GetString();
                    HttpChannel::send_reply(req, search_result_json);
                    return;
                }
            }
        }
    }
};

class RestClearScrollAction : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "root") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        if (req->method() == HttpMethod::DELETE) {
            std::string post_body = req->get_request_body();
            rapidjson::Document post_doc;
            post_doc.Parse<0>(post_body.c_str());
            std::string scroll_id;
            if (!post_doc.HasMember("scroll_id")) {
                HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "invalid scroll request");
                return;
            } else {
                rapidjson::Document clear_scroll_result;
                rapidjson::Document::AllocatorType& allocator = clear_scroll_result.GetAllocator();
                clear_scroll_result.SetObject();
                clear_scroll_result.AddMember("succeeded", true, allocator);
                clear_scroll_result.AddMember("num_freed", 1, allocator);
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                clear_scroll_result.Accept(writer);
                std::string clear_scroll_result_json = buffer.GetString();
                HttpChannel::send_reply(req, clear_scroll_result_json);
                return;
            }
        }
    }
};

static RestSearchAction rest_search_action = RestSearchAction();
static RestSearchScrollAction rest_search_scroll_action = RestSearchScrollAction();
static RestClearScrollAction rest_clear_scroll_action = RestClearScrollAction();
static EvHttpServer* mock_es_server = nullptr;
static int real_port = 0;

class MockESServerTest : public testing::Test {
public:
    MockESServerTest() = default;
    ~MockESServerTest() override = default;

    static void SetUpTestCase() {
        mock_es_server = new EvHttpServer(0);
        mock_es_server->register_handler(POST, "/{index}/{type}/_search", &rest_search_action);
        mock_es_server->register_handler(POST, "/_search/scroll", &rest_search_scroll_action);
        mock_es_server->register_handler(DELETE, "/_search/scroll", &rest_clear_scroll_action);
        mock_es_server->start();
        real_port = mock_es_server->get_real_port();
        ASSERT_NE(0, real_port);
    }

    static void TearDownTestCase() {
        mock_es_server->stop();
        mock_es_server->join();
        delete mock_es_server;
    }
};

} // namespace starrocks
