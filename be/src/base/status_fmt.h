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

// fmt-style factory helpers for starrocks::Status.
//
// This header purposely does NOT include <fmt/format.h>.  It only forward-
// declares the fmt types it needs.  Callers must include <fmt/format.h>
// (or any header that pulls it in) before instantiating these templates.
//
// Usage:
//   #include <fmt/format.h>
//   #include "base/status_fmt.h"
//   ...
//   return Status_InternalError("unexpected value {}", val);

#pragma once

#include "base/status.h"

// Forward-declare the fmt template class used as a parameter type below.
// The full definition is supplied by <fmt/format.h> at instantiation time.
namespace fmt {
template <typename Char, typename... Args>
class basic_format_string;
} // namespace fmt

namespace starrocks {

// ---------------------------------------------------------------------------
// Each helper accepts a compile-time fmt format string and zero or more
// arguments.  The unqualified call to format(...) relies on ADL: because
// fmt_str is of type fmt::basic_format_string<...>, argument-dependent
// lookup finds fmt::format at template-instantiation time (when the caller
// has already included <fmt/format.h>).
// ---------------------------------------------------------------------------

template <typename... Args>
inline Status Status_Unknown(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::Unknown(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_PublishTimeout(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::PublishTimeout(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_MemoryAllocFailed(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::MemoryAllocFailed(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_BufferAllocFailed(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::BufferAllocFailed(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_InvalidArgument(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::InvalidArgument(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_MinimumReservationUnavailable(fmt::basic_format_string<char, Args...> fmt_str,
                                                   Args&&... args) {
    return Status::MinimumReservationUnavailable(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Corruption(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::Corruption(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_IOError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::IOError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_NotFound(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::NotFound(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_AlreadyExist(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::AlreadyExist(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_NotSupported(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::NotSupported(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_EndOfFile(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::EndOfFile(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_InternalError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::InternalError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_RuntimeError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::RuntimeError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Cancelled(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::Cancelled(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_MemoryLimitExceeded(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::MemoryLimitExceeded(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_ThriftRpcError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::ThriftRpcError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TimedOut(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::TimedOut(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TooManyTasks(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::TooManyTasks(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_ServiceUnavailable(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::ServiceUnavailable(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Uninitialized(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::Uninitialized(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Aborted(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::Aborted(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_DataQualityError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::DataQualityError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_VersionAlreadyMerged(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::VersionAlreadyMerged(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_DuplicateRpcInvocation(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::DuplicateRpcInvocation(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_JsonFormatError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::JsonFormatError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_VariantError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::VariantError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_GlobalDictError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::GlobalDictError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_GlobalDictNotMatch(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::GlobalDictNotMatch(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TransactionInProcessing(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::TransactionInProcessing(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TransactionNotExists(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::TransactionNotExists(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_LabelAlreadyExists(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::LabelAlreadyExists(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_ResourceBusy(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::ResourceBusy(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_EAgain(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::EAgain(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_RemoteFileNotFound(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::RemoteFileNotFound(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_JitCompileError(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::JitCompileError(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_CapacityLimitExceed(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::CapacityLimitExceed(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Shutdown(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::Shutdown(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_BigQueryCpuSecondLimitExceeded(fmt::basic_format_string<char, Args...> fmt_str,
                                                    Args&&... args) {
    return Status::BigQueryCpuSecondLimitExceeded(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_BigQueryScanRowsLimitExceeded(fmt::basic_format_string<char, Args...> fmt_str,
                                                   Args&&... args) {
    return Status::BigQueryScanRowsLimitExceeded(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_NotAuthorized(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::NotAuthorized(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TableNotExist(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::TableNotExist(format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_QueryNotExist(fmt::basic_format_string<char, Args...> fmt_str, Args&&... args) {
    return Status::QueryNotExist(format(fmt_str, std::forward<Args>(args)...));
}

} // namespace starrocks
