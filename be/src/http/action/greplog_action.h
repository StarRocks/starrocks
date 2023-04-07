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

#include <atomic>

#include "common/status.h"
#include "http/http_handler.h"

namespace starrocks {

class GrepLogAction : public HttpHandler {
public:
    explicit GrepLogAction() = default;

    ~GrepLogAction() override = default;

    void handle(HttpRequest* req) override;
};

} // namespace starrocks