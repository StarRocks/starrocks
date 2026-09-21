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

#pragma once

#include <rapidjson/document.h>

#include <functional>
#include <string>

#include "platform/http/http_handler.h"

namespace starrocks {

namespace lake {
class TabletManager;
} // namespace lake

// Reports what a table has set for the publish work on this node, as the consumer of those values
// currently holds it.
//
// `type` names the consumer to ask and is required; today the only one is `pk`, the primary key
// index, and each consumer states its own required parameters -- `pk` needs `tablet_id`, because
// that is what it keeps one of these per. A later consumer becomes another `type` rather than
// another field in this answer, which is the same way the backend keeps them: each parses the
// property names it reads and ignores the rest, so nothing here has to know the whole set.
//
// The answer carries only what the table set. A property it left unset is answered at every read
// from this node's own configuration, which is not state any consumer holds and so not something
// this can report; `information_schema.be_configs` is where that question belongs.
//
// What a consumer holds lives in memory and is never persisted, so a tablet whose primary key index
// is not loaded has no answer here rather than an empty one -- publishing to it again is what brings
// one back.
//
// Every reply is HTTP 200 and says how it went in `status` and `message`, so a caller reads one
// place rather than reconciling a body against a status code.
class PublishPropertyAction : public HttpHandler {
public:
    // |tablet_manager| is for a test that wants to answer from its own tablet manager rather than the
    // node's; production passes nothing and this reads the one the node runs with.
    explicit PublishPropertyAction(lake::TabletManager* tablet_manager = nullptr) : _tablet_manager(tablet_manager) {}
    ~PublishPropertyAction() override = default;

    void handle(HttpRequest* req) override;

    RequiredPrivilege required_privilege() const override { return RequiredPrivilege::OPERATE; }

private:
    void _handle_pk(HttpRequest* req);
    void _reply(HttpRequest* req, const std::string& status, const std::string& message,
                const std::function<void(rapidjson::Document& root)>& fill);
    void _reply_error(HttpRequest* req, const std::string& status, const std::string& message);

    lake::TabletManager* _tablet_manager;
};

} // namespace starrocks
