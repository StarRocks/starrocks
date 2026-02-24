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

// Inline implementations of the fmt-style Status factory overloads declared
// in status.h.  This file is included at the bottom of status.h and should
// not be included directly.

#pragma once

#include <fmt/format.h>

#include "base/status.h"

namespace starrocks {

template <typename... Args>
inline Status Status::Unknown(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return Unknown(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::PublishTimeout(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return PublishTimeout(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::MemoryAllocFailed(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return MemoryAllocFailed(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::BufferAllocFailed(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return BufferAllocFailed(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::InvalidArgument(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return InvalidArgument(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::MinimumReservationUnavailable(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return MinimumReservationUnavailable(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::Corruption(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return Corruption(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::IOError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return IOError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::NotFound(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return NotFound(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::AlreadyExist(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return AlreadyExist(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::NotSupported(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return NotSupported(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::EndOfFile(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return EndOfFile(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::InternalError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return InternalError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::RuntimeError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return RuntimeError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::Cancelled(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return Cancelled(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::MemoryLimitExceeded(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return MemoryLimitExceeded(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::ThriftRpcError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return ThriftRpcError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::TimedOut(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return TimedOut(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::TooManyTasks(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return TooManyTasks(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::ServiceUnavailable(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return ServiceUnavailable(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::Uninitialized(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return Uninitialized(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::Aborted(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return Aborted(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::DataQualityError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return DataQualityError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::VersionAlreadyMerged(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return VersionAlreadyMerged(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::DuplicateRpcInvocation(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return DuplicateRpcInvocation(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::JsonFormatError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return JsonFormatError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::VariantError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return VariantError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::GlobalDictError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return GlobalDictError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::GlobalDictNotMatch(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return GlobalDictNotMatch(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::TransactionInProcessing(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return TransactionInProcessing(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::TransactionNotExists(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return TransactionNotExists(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::LabelAlreadyExists(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return LabelAlreadyExists(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::ResourceBusy(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return ResourceBusy(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::EAgain(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return EAgain(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::RemoteFileNotFound(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return RemoteFileNotFound(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::JitCompileError(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return JitCompileError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::CapacityLimitExceed(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return CapacityLimitExceed(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::Shutdown(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return Shutdown(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::BigQueryCpuSecondLimitExceeded(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return BigQueryCpuSecondLimitExceeded(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::BigQueryScanRowsLimitExceeded(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return BigQueryScanRowsLimitExceeded(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::NotAuthorized(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return NotAuthorized(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::TableNotExist(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return TableNotExist(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status::QueryNotExist(fmt::basic_format_string<char, std::type_identity_t<Args>...> fmt_str, Args&&... args) {
    return QueryNotExist(fmt::format(fmt_str, std::forward<Args>(args)...));
}

} // namespace starrocks
