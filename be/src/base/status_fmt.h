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

// This file extends Status factory methods with fmt-style formatting overloads.
// Include this header (after or instead of status.h) when you want to call
// Status::InternalError("msg {} {}", arg1, arg2) style APIs.

#pragma once

#include <fmt/format.h>

#include "base/status.h"

namespace starrocks {

// --------------------------------------------------------------------------
// fmt-style overloads for every Status factory method.
// Each free function mirrors the corresponding static factory on Status and
// forwards a compiled format-string plus arguments through fmt::format.
// --------------------------------------------------------------------------

template <typename... Args>
inline Status Status_Unknown(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::Unknown(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_PublishTimeout(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::PublishTimeout(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_MemoryAllocFailed(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::MemoryAllocFailed(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_BufferAllocFailed(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::BufferAllocFailed(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_InvalidArgument(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::InvalidArgument(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_MinimumReservationUnavailable(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::MinimumReservationUnavailable(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Corruption(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::Corruption(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_IOError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::IOError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_NotFound(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::NotFound(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_AlreadyExist(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::AlreadyExist(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_NotSupported(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::NotSupported(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_EndOfFile(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::EndOfFile(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_InternalError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::InternalError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_RuntimeError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::RuntimeError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Cancelled(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::Cancelled(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_MemoryLimitExceeded(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::MemoryLimitExceeded(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_ThriftRpcError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::ThriftRpcError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TimedOut(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::TimedOut(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TooManyTasks(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::TooManyTasks(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_ServiceUnavailable(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::ServiceUnavailable(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Uninitialized(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::Uninitialized(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Aborted(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::Aborted(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_DataQualityError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::DataQualityError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_VersionAlreadyMerged(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::VersionAlreadyMerged(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_DuplicateRpcInvocation(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::DuplicateRpcInvocation(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_JsonFormatError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::JsonFormatError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_VariantError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::VariantError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_GlobalDictError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::GlobalDictError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_GlobalDictNotMatch(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::GlobalDictNotMatch(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TransactionInProcessing(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::TransactionInProcessing(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TransactionNotExists(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::TransactionNotExists(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_LabelAlreadyExists(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::LabelAlreadyExists(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_ResourceBusy(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::ResourceBusy(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_EAgain(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::EAgain(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_RemoteFileNotFound(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::RemoteFileNotFound(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_JitCompileError(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::JitCompileError(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_CapacityLimitExceed(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::CapacityLimitExceed(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_Shutdown(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::Shutdown(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_BigQueryCpuSecondLimitExceeded(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::BigQueryCpuSecondLimitExceeded(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_BigQueryScanRowsLimitExceeded(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::BigQueryScanRowsLimitExceeded(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_NotAuthorized(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::NotAuthorized(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_TableNotExist(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::TableNotExist(fmt::format(fmt_str, std::forward<Args>(args)...));
}

template <typename... Args>
inline Status Status_QueryNotExist(fmt::format_string<Args...> fmt_str, Args&&... args) {
    return Status::QueryNotExist(fmt::format(fmt_str, std::forward<Args>(args)...));
}

} // namespace starrocks
