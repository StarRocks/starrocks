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

template <typename FMT, typename... Args>
inline Status Status::Unknown(FMT&& fmt, Args&&... args) {
    return Unknown(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::PublishTimeout(FMT&& fmt, Args&&... args) {
    return PublishTimeout(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::MemoryAllocFailed(FMT&& fmt, Args&&... args) {
    return MemoryAllocFailed(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::BufferAllocFailed(FMT&& fmt, Args&&... args) {
    return BufferAllocFailed(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::InvalidArgument(FMT&& fmt, Args&&... args) {
    return InvalidArgument(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::MinimumReservationUnavailable(FMT&& fmt, Args&&... args) {
    return MinimumReservationUnavailable(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::Corruption(FMT&& fmt, Args&&... args) {
    return Corruption(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::IOError(FMT&& fmt, Args&&... args) {
    return IOError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::NotFound(FMT&& fmt, Args&&... args) {
    return NotFound(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::AlreadyExist(FMT&& fmt, Args&&... args) {
    return AlreadyExist(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::NotSupported(FMT&& fmt, Args&&... args) {
    return NotSupported(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::EndOfFile(FMT&& fmt, Args&&... args) {
    return EndOfFile(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::InternalError(FMT&& fmt, Args&&... args) {
    return InternalError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::RuntimeError(FMT&& fmt, Args&&... args) {
    return RuntimeError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::Cancelled(FMT&& fmt, Args&&... args) {
    return Cancelled(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::MemoryLimitExceeded(FMT&& fmt, Args&&... args) {
    return MemoryLimitExceeded(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::ThriftRpcError(FMT&& fmt, Args&&... args) {
    return ThriftRpcError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::TimedOut(FMT&& fmt, Args&&... args) {
    return TimedOut(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::TooManyTasks(FMT&& fmt, Args&&... args) {
    return TooManyTasks(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::ServiceUnavailable(FMT&& fmt, Args&&... args) {
    return ServiceUnavailable(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::Uninitialized(FMT&& fmt, Args&&... args) {
    return Uninitialized(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::Aborted(FMT&& fmt, Args&&... args) {
    return Aborted(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::DataQualityError(FMT&& fmt, Args&&... args) {
    return DataQualityError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::VersionAlreadyMerged(FMT&& fmt, Args&&... args) {
    return VersionAlreadyMerged(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::DuplicateRpcInvocation(FMT&& fmt, Args&&... args) {
    return DuplicateRpcInvocation(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::JsonFormatError(FMT&& fmt, Args&&... args) {
    return JsonFormatError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::VariantError(FMT&& fmt, Args&&... args) {
    return VariantError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::GlobalDictError(FMT&& fmt, Args&&... args) {
    return GlobalDictError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::GlobalDictNotMatch(FMT&& fmt, Args&&... args) {
    return GlobalDictNotMatch(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::TransactionInProcessing(FMT&& fmt, Args&&... args) {
    return TransactionInProcessing(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::TransactionNotExists(FMT&& fmt, Args&&... args) {
    return TransactionNotExists(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::LabelAlreadyExists(FMT&& fmt, Args&&... args) {
    return LabelAlreadyExists(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::ResourceBusy(FMT&& fmt, Args&&... args) {
    return ResourceBusy(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::EAgain(FMT&& fmt, Args&&... args) {
    return EAgain(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::RemoteFileNotFound(FMT&& fmt, Args&&... args) {
    return RemoteFileNotFound(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::JitCompileError(FMT&& fmt, Args&&... args) {
    return JitCompileError(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::CapacityLimitExceed(FMT&& fmt, Args&&... args) {
    return CapacityLimitExceed(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::Shutdown(FMT&& fmt, Args&&... args) {
    return Shutdown(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::BigQueryCpuSecondLimitExceeded(FMT&& fmt, Args&&... args) {
    return BigQueryCpuSecondLimitExceeded(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::BigQueryScanRowsLimitExceeded(FMT&& fmt, Args&&... args) {
    return BigQueryScanRowsLimitExceeded(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::NotAuthorized(FMT&& fmt, Args&&... args) {
    return NotAuthorized(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::TableNotExist(FMT&& fmt, Args&&... args) {
    return TableNotExist(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

template <typename FMT, typename... Args>
inline Status Status::QueryNotExist(FMT&& fmt, Args&&... args) {
    return QueryNotExist(fmt::format(std::forward<FMT>(fmt), std::forward<Args>(args)...));
}

} // namespace starrocks
