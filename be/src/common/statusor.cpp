// Copyright 2020 The Abseil Authors.
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
#include "common/statusor.h"

#include <cstdlib>
#include <utility>

#include "common/status.h"

namespace starrocks {

BadStatusOrAccess::BadStatusOrAccess(starrocks::Status status) : status_(std::move(status)) {}

BadStatusOrAccess::~BadStatusOrAccess() = default;
const char* BadStatusOrAccess::what() const noexcept {
    return "Bad StatusOr access";
}

const starrocks::Status& BadStatusOrAccess::status() const {
    return status_;
}

namespace internal_statusor {

void Helper::HandleInvalidStatusCtorArg(starrocks::Status* status) {
    const char* kMessage = "An OK status is not a valid constructor argument to StatusOr<T>";
    *status = starrocks::Status::InternalError(kMessage);
}

void Helper::Crash(const starrocks::Status& status) {
    std::cerr << "Attempting to fetch value instead of handling error " << status.to_string();
    std::abort();
}

void ThrowBadStatusOrAccess(starrocks::Status status) {
    throw starrocks::BadStatusOrAccess(std::move(status));
}

} // namespace internal_statusor
} // namespace starrocks
