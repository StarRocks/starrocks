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

#include <memory>
#include <utility>

#include "gutil/logging.h"

namespace starrocks {

class PTabletWriterAddSegmentRequest;

class SegmentRequestRef {
public:
    static SegmentRequestRef borrowed(const PTabletWriterAddSegmentRequest* request) {
        DCHECK(request != nullptr);
        return SegmentRequestRef(request, nullptr);
    }

    static SegmentRequestRef owned(std::shared_ptr<const PTabletWriterAddSegmentRequest> request) {
        DCHECK(request != nullptr);
        const auto* request_ptr = request.get();
        return SegmentRequestRef(request_ptr, std::move(request));
    }

    const PTabletWriterAddSegmentRequest* operator->() const { return _request; }
    const PTabletWriterAddSegmentRequest& operator*() const { return *_request; }
    const PTabletWriterAddSegmentRequest* get() const { return _request; }

private:
    SegmentRequestRef(const PTabletWriterAddSegmentRequest* request,
                      std::shared_ptr<const PTabletWriterAddSegmentRequest> keepalive)
            : _request(request), _keepalive(std::move(keepalive)) {}

    const PTabletWriterAddSegmentRequest* _request;
    std::shared_ptr<const PTabletWriterAddSegmentRequest> _keepalive;
};

} // namespace starrocks
