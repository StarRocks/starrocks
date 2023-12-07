// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/pprof_actions.h

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

#pragma once

#include "http/http_handler.h"

namespace starrocks {

class BfdParser;

class HeapAction : public HttpHandler {
public:
    HeapAction() = default;
    ~HeapAction() override = default;

    void handle(HttpRequest* req) override;
};

class GrowthAction : public HttpHandler {
public:
    GrowthAction() = default;
    ~GrowthAction() override = default;

    void handle(HttpRequest* req) override;
};

class ProfileAction : public HttpHandler {
public:
    ProfileAction() = default;
    ~ProfileAction() override = default;

    void handle(HttpRequest* req) override;
};

class IOProfileAction : public HttpHandler {
public:
    IOProfileAction() = default;
    ~IOProfileAction() override = default;

    void handle(HttpRequest* req) override;
};

class PmuProfileAction : public HttpHandler {
public:
    PmuProfileAction() = default;
    ~PmuProfileAction() override = default;
    void handle(HttpRequest* req) override {}
};

class ContentionAction : public HttpHandler {
public:
    ContentionAction() = default;
    ~ContentionAction() override = default;

    void handle(HttpRequest* req) override {}
};

class CmdlineAction : public HttpHandler {
public:
    CmdlineAction() = default;
    ~CmdlineAction() override = default;
    void handle(HttpRequest* req) override;
};

class SymbolAction : public HttpHandler {
public:
    SymbolAction(BfdParser* parser) : _parser(parser) {}
    ~SymbolAction() override = default;

    void handle(HttpRequest* req) override;

private:
    BfdParser* _parser;
};

} // namespace starrocks
