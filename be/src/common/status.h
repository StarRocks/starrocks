// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "gen_cpp/StatusCode_types.h" // for TStatus
#include "util/time.h"

namespace starrocks {

class StatusPB;
class TStatus;

template <typename T>
class StatusOr;
// @TODO this should be removed later after fixing compile issues in ut
#ifndef BE_TEST
#define STATUS_ATTRIBUTE [[nodiscard]]
#else
#define STATUS_ATTRIBUTE
#endif

class STATUS_ATTRIBUTE Status {
public:
    Status() = default;

    ~Status() noexcept {
        if (!is_moved_from(_state)) {
            delete[] _state;
        }
    }

#if defined(ENABLE_STATUS_FAILED)
    static int32_t get_cardinality_of_inject();
    static inline std::unordered_map<std::string, bool> dircetory_enable;
    static void access_directory_of_inject();
    static bool in_directory_of_inject(const std::string&);
#endif

    // Copy c'tor makes copy of error detail so Status can be returned by value
    Status(const Status& s) : _state(s._state == nullptr ? nullptr : copy_state(s._state)) {}

    // Move c'tor
    Status(Status&& s) noexcept : _state(s._state) { s._state = moved_from_state(); }

    // Same as copy c'tor
    Status& operator=(const Status& s) {
        if (this != &s) {
            Status tmp(s);
            std::swap(this->_state, tmp._state);
        }
        return *this;
    }

    // Move assign.
    Status& operator=(Status&& s) noexcept {
        if (this != &s) {
            Status tmp(std::move(s));
            std::swap(this->_state, tmp._state);
        }
        return *this;
    }

    // "Copy" c'tor from TStatus.
    Status(const TStatus& status); // NOLINT

    Status(const StatusPB& pstatus); // NOLINT

    // Inspired by absl::Status::Update()
    // https://github.com/abseil/abseil-cpp/blob/63c9eeca0464c08ccb861b21e33e10faead414c9/absl/status/status.h#L467
    //
    // Status::update()
    //
    // Updates the existing status with `new_status` provided that `this->ok()`.
    // If the existing status already contains a non-OK error, this update has no
    // effect and preserves the current data.
    //
    // `update()` provides a convenient way of keeping track of the first error
    // encountered.
    //
    // Example:
    //   // Instead of "if (overall_status.ok()) overall_status = new_status"
    //   overall_status.update(new_status);
    //
    void update(const Status& new_status);
    void update(Status&& new_status);

    static Status OK() { return Status(); }

    static Status Unknown(std::string_view msg) { return Status(TStatusCode::UNKNOWN, msg); }

    static Status PublishTimeout(std::string_view msg) { return Status(TStatusCode::PUBLISH_TIMEOUT, msg); }
    static Status MemoryAllocFailed(std::string_view msg) { return Status(TStatusCode::MEM_ALLOC_FAILED, msg); }
    static Status BufferAllocFailed(std::string_view msg) { return Status(TStatusCode::BUFFER_ALLOCATION_FAILED, msg); }
    static Status InvalidArgument(std::string_view msg) { return Status(TStatusCode::INVALID_ARGUMENT, msg); }
    static Status MinimumReservationUnavailable(std::string_view msg) {
        return Status(TStatusCode::MINIMUM_RESERVATION_UNAVAILABLE, msg);
    }
    static Status Corruption(std::string_view msg) { return Status(TStatusCode::CORRUPTION, msg); }
    static Status IOError(std::string_view msg) { return Status(TStatusCode::IO_ERROR, msg); }
    static Status NotFound(std::string_view msg) { return Status(TStatusCode::NOT_FOUND, msg); }
    static Status AlreadyExist(std::string_view msg) { return Status(TStatusCode::ALREADY_EXIST, msg); }
    static Status NotSupported(std::string_view msg) { return Status(TStatusCode::NOT_IMPLEMENTED_ERROR, msg); }
    static Status EndOfFile(std::string_view msg) { return Status(TStatusCode::END_OF_FILE, msg); }
    static Status InternalError(std::string_view msg) { return Status(TStatusCode::INTERNAL_ERROR, msg); }
    static Status RuntimeError(std::string_view msg) { return Status(TStatusCode::RUNTIME_ERROR, msg); }
    static Status Cancelled(std::string_view msg) { return Status(TStatusCode::CANCELLED, msg); }

    static Status MemoryLimitExceeded(std::string_view msg) { return Status(TStatusCode::MEM_LIMIT_EXCEEDED, msg); }

    static Status ThriftRpcError(std::string_view msg) { return Status(TStatusCode::THRIFT_RPC_ERROR, msg); }
    static Status TimedOut(std::string_view msg) { return Status(TStatusCode::TIMEOUT, msg); }
    static Status TooManyTasks(std::string_view msg) { return Status(TStatusCode::TOO_MANY_TASKS, msg); }
    static Status ServiceUnavailable(std::string_view msg) { return Status(TStatusCode::SERVICE_UNAVAILABLE, msg); }
    static Status Uninitialized(std::string_view msg) { return Status(TStatusCode::UNINITIALIZED, msg); }
    static Status Aborted(std::string_view msg) { return Status(TStatusCode::ABORTED, msg); }
    static Status DataQualityError(std::string_view msg) { return Status(TStatusCode::DATA_QUALITY_ERROR, msg); }
    static Status VersionAlreadyMerged(std::string_view msg) {
        return Status(TStatusCode::OLAP_ERR_VERSION_ALREADY_MERGED, msg);
    }
    static Status DuplicateRpcInvocation(std::string_view msg) {
        return Status(TStatusCode::DUPLICATE_RPC_INVOCATION, msg);
    }
    static Status JsonFormatError(std::string_view msg) {
        // TODO(mofei) define json format error.
        return Status(TStatusCode::DATA_QUALITY_ERROR, msg);
    }

    static Status GlobalDictError(std::string_view msg) { return Status(TStatusCode::GLOBAL_DICT_ERROR, msg); }

    static Status TransactionInProcessing(std::string_view msg) { return Status(TStatusCode::TXN_IN_PROCESSING, msg); }
    static Status TransactionNotExists(std::string_view msg) { return Status(TStatusCode::TXN_NOT_EXISTS, msg); }
    static Status LabelAlreadyExists(std::string_view msg) { return Status(TStatusCode::LABEL_ALREADY_EXISTS, msg); }

    static Status ResourceBusy(std::string_view msg) { return Status(TStatusCode::RESOURCE_BUSY, msg); }

    static Status EAgain(std::string_view msg) { return Status(TStatusCode::SR_EAGAIN, msg); }

    static Status RemoteFileNotFound(std::string_view msg) { return Status(TStatusCode::REMOTE_FILE_NOT_FOUND, msg); }

    static Status JitCompileError(std::string_view msg) { return Status(TStatusCode::JIT_COMPILE_ERROR, msg); }

    static Status CapacityLimitExceed(std::string_view msg) { return Status(TStatusCode::CAPACITY_LIMIT_EXCEED, msg); }

    static Status Yield() { return {TStatusCode::YIELD, ""}; }

    bool ok() const { return _state == nullptr; }

    bool is_cancelled() const { return code() == TStatusCode::CANCELLED; }

    bool is_mem_limit_exceeded() const { return code() == TStatusCode::MEM_LIMIT_EXCEEDED; }

    bool is_thrift_rpc_error() const { return code() == TStatusCode::THRIFT_RPC_ERROR; }

    bool is_end_of_file() const { return code() == TStatusCode::END_OF_FILE; }

    bool is_ok_or_eof() const { return ok() || is_end_of_file(); }

    bool is_not_found() const { return code() == TStatusCode::NOT_FOUND; }

    bool is_already_exist() const { return code() == TStatusCode::ALREADY_EXIST; }

    bool is_io_error() const { return code() == TStatusCode::IO_ERROR; }

    bool is_not_supported() const { return code() == TStatusCode::NOT_IMPLEMENTED_ERROR; }

    bool is_corruption() const { return code() == TStatusCode::CORRUPTION; }

    bool is_resource_busy() const { return code() == TStatusCode::RESOURCE_BUSY; }

    /// @return @c true if the status indicates Uninitialized.
    bool is_uninitialized() const { return code() == TStatusCode::UNINITIALIZED; }

    // @return @c true if the status indicates an Aborted error.
    bool is_aborted() const { return code() == TStatusCode::ABORTED; }

    /// @return @c true if the status indicates an InvalidArgument error.
    bool is_invalid_argument() const { return code() == TStatusCode::INVALID_ARGUMENT; }

    // @return @c true if the status indicates ServiceUnavailable.
    bool is_service_unavailable() const { return code() == TStatusCode::SERVICE_UNAVAILABLE; }

    bool is_data_quality_error() const { return code() == TStatusCode::DATA_QUALITY_ERROR; }

    bool is_version_already_merged() const { return code() == TStatusCode::OLAP_ERR_VERSION_ALREADY_MERGED; }

    bool is_duplicate_rpc_invocation() const { return code() == TStatusCode::DUPLICATE_RPC_INVOCATION; }

    bool is_time_out() const { return code() == TStatusCode::TIMEOUT; }

    bool is_publish_timeout() const { return code() == TStatusCode::PUBLISH_TIMEOUT; }

    bool is_eagain() const { return code() == TStatusCode::SR_EAGAIN; }

    bool is_yield() const { return code() == TStatusCode::YIELD; }

    // Convert into TStatus. Call this if 'status_container' contains an optional
    // TStatus field named 'status'. This also sets __isset.status.
    template <typename T>
    void set_t_status(T* status_container) const {
        to_thrift(&status_container->status);
        status_container->__isset.status = true;
    }

    // Convert into TStatus.
    void to_thrift(TStatus* status) const;
    void to_protobuf(StatusPB* status) const;

    /// @return A string representation of this status suitable for printing.
    ///   Returns the string "OK" for success.
    std::string to_string(bool with_context_info = true) const;

    /// @return A string representation of the status code, without the message
    ///   text or sub code information.
    std::string code_as_string() const;

    // This is similar to to_string, except that it does not include
    // the context info.
    //
    // @note The returned std::string_view is only valid as long as this Status object
    //   remains live and unchanged.
    //
    // @return The message portion of the Status. For @c OK statuses,
    //   this returns an empty string.
    std::string_view message() const;

    // Error message with extra context info, like file name, line number.
    std::string_view detailed_message() const;

    TStatusCode::type code() const {
        return _state == nullptr ? TStatusCode::OK : static_cast<TStatusCode::type>(_state[4]);
    }

    /// Clone this status and add the specified prefix to the message.
    ///
    /// If this status is OK, then an OK status will be returned.
    ///
    /// @param [in] msg
    ///   The message to prepend.
    /// @return A new Status object with the same state plus an additional
    ///   leading message.
    Status clone_and_prepend(std::string_view msg) const;

    /// Clone this status and add the specified suffix to the message.
    ///
    /// If this status is OK, then an OK status will be returned.
    ///
    /// @param [in] msg
    ///   The message to append.
    /// @return A new Status object with the same state plus an additional
    ///   trailing message.
    Status clone_and_append(std::string_view msg) const;

    Status clone_and_append_context(const char* filename, int line, const char* expr) const;

private:
    static const char* copy_state(const char* state);
    static const char* copy_state_with_extra_ctx(const char* state, std::string_view ctx);

    // Indicates whether this Status was the rhs of a move operation.
    static bool is_moved_from(const char* state);
    static const char* moved_from_state();

    Status(TStatusCode::type code, std::string_view msg) : Status(code, msg, {}) {}
    Status(TStatusCode::type code, std::string_view msg, std::string_view ctx);

private:
    // OK status has a nullptr _state.  Otherwise, _state is a new[] array
    // of the following form:
    //    _state[0..1]                        == len1: length of message
    //    _state[2..3]                        == len2: length of context
    //    _state[4]                           == code
    //    _state[5.. 5 + len1]                == message
    //    _state[5 + len1 .. 5 + len1 + len2] == context
    const char* _state = nullptr;
};

inline void Status::update(const Status& new_status) {
    if (ok()) {
        *this = new_status;
    }
}

inline void Status::update(Status&& new_status) {
    if (ok()) {
        *this = std::move(new_status);
    }
}

inline Status ignore_not_found(const Status& status) {
    return status.is_not_found() ? Status::OK() : status;
}

inline std::ostream& operator<<(std::ostream& os, const Status& st) {
    return os << st.to_string();
}

inline const Status& to_status(const Status& st) {
    return st;
}

template <typename T>
inline const Status& to_status(const StatusOr<T>& st) {
    return st.status();
}

#ifndef AS_STRING
#define AS_STRING(x) AS_STRING_INTERNAL(x)
#define AS_STRING_INTERNAL(x) #x
#endif

#define RETURN_IF_ERROR_INTERNAL(stmt)                                                                \
    do {                                                                                              \
        auto&& status__ = (stmt);                                                                     \
        if (UNLIKELY(!status__.ok())) {                                                               \
            return to_status(status__).clone_and_append_context(__FILE__, __LINE__, AS_STRING(stmt)); \
        }                                                                                             \
    } while (false)

#if defined(ENABLE_STATUS_FAILED)
struct StatusInstance {
    static constexpr Status (*random[])(std::string_view msg) = {&Status::Unknown,
                                                                 &Status::PublishTimeout,
                                                                 &Status::MemoryAllocFailed,
                                                                 &Status::BufferAllocFailed,
                                                                 &Status::InvalidArgument,
                                                                 &Status::MinimumReservationUnavailable,
                                                                 &Status::Corruption,
                                                                 &Status::IOError,
                                                                 &Status::NotFound,
                                                                 &Status::AlreadyExist,
                                                                 &Status::NotSupported,
                                                                 &Status::EndOfFile,
                                                                 &Status::ServiceUnavailable,
                                                                 &Status::Uninitialized,
                                                                 &Status::Aborted,
                                                                 &Status::DataQualityError,
                                                                 &Status::VersionAlreadyMerged,
                                                                 &Status::DuplicateRpcInvocation,
                                                                 &Status::JsonFormatError,
                                                                 &Status::GlobalDictError,
                                                                 &Status::TransactionInProcessing,
                                                                 &Status::TransactionNotExists,
                                                                 &Status::LabelAlreadyExists,
                                                                 &Status::ResourceBusy};

    static constexpr TStatusCode::type codes[] = {TStatusCode::UNKNOWN,
                                                  TStatusCode::PUBLISH_TIMEOUT,
                                                  TStatusCode::MEM_ALLOC_FAILED,
                                                  TStatusCode::BUFFER_ALLOCATION_FAILED,
                                                  TStatusCode::INVALID_ARGUMENT,
                                                  TStatusCode::MINIMUM_RESERVATION_UNAVAILABLE,
                                                  TStatusCode::CORRUPTION,
                                                  TStatusCode::IO_ERROR,
                                                  TStatusCode::NOT_FOUND,
                                                  TStatusCode::ALREADY_EXIST,
                                                  TStatusCode::NOT_IMPLEMENTED_ERROR,
                                                  TStatusCode::END_OF_FILE,
                                                  TStatusCode::SERVICE_UNAVAILABLE,
                                                  TStatusCode::UNINITIALIZED,
                                                  TStatusCode::ABORTED,
                                                  TStatusCode::DATA_QUALITY_ERROR,
                                                  TStatusCode::OLAP_ERR_VERSION_ALREADY_MERGED,
                                                  TStatusCode::DUPLICATE_RPC_INVOCATION,
                                                  TStatusCode::DATA_QUALITY_ERROR,
                                                  TStatusCode::GLOBAL_DICT_ERROR,
                                                  TStatusCode::TXN_IN_PROCESSING,
                                                  TStatusCode::TXN_NOT_EXISTS,
                                                  TStatusCode::LABEL_ALREADY_EXISTS,
                                                  TStatusCode::RESOURCE_BUSY};

    static constexpr int SIZE = sizeof(random) / sizeof(Status(*)(std::string_view msg));
};

#define RETURN_INJECT(index)                                                         \
    std::stringstream ss;                                                            \
    ss << "INJECT ERROR: " << __FILE__ << " " << __LINE__ << " "                     \
       << starrocks::StatusInstance::codes[index % starrocks::StatusInstance::SIZE]; \
    return starrocks::StatusInstance::random[index % starrocks::StatusInstance::SIZE](ss.str());

#define RETURN_IF_ERROR(stmt)                                                                             \
    do {                                                                                                  \
        uint32_t seed = starrocks::GetCurrentTimeNanos();                                                 \
        seed = ::rand_r(&seed);                                                                           \
        uint32_t boundary_value = RAND_MAX / (1.0 * starrocks::Status::get_cardinality_of_inject());      \
        /* Pre-condition of inject errors: probability and File scope*/                                   \
        if (seed <= boundary_value && starrocks::Status::in_directory_of_inject(__FILE__)) {              \
            RETURN_INJECT(seed);                                                                          \
        } else {                                                                                          \
            auto&& status__ = (stmt);                                                                     \
            if (UNLIKELY(!status__.ok())) {                                                               \
                return to_status(status__).clone_and_append_context(__FILE__, __LINE__, AS_STRING(stmt)); \
            }                                                                                             \
        }                                                                                                 \
    } while (false)
#else
#define RETURN_IF_ERROR(stmt) RETURN_IF_ERROR_INTERNAL(stmt)
#endif

#define EXIT_IF_ERROR(stmt)                   \
    do {                                      \
        auto&& status__ = (stmt);             \
        if (UNLIKELY(!status__.ok())) {       \
            LOG(ERROR) << status__.message(); \
            exit(1);                          \
        }                                     \
    } while (false)

/// @brief Emit a warning if @c to_call returns a bad status.
#define WARN_IF_ERROR(to_call, warning_prefix)                \
    do {                                                      \
        auto&& st__ = (to_call);                              \
        if (UNLIKELY(!st__.ok())) {                           \
            LOG(WARNING) << (warning_prefix) << ": " << st__; \
        }                                                     \
    } while (0)

#define RETURN_IF_ERROR_WITH_WARN(stmt, warning_prefix)              \
    do {                                                             \
        auto&& st__ = (stmt);                                        \
        if (UNLIKELY(!st__.ok())) {                                  \
            LOG(WARNING) << (warning_prefix) << ", error: " << st__; \
            return std::move(st__);                                  \
        }                                                            \
    } while (0)

#define DCHECK_IF_ERROR(stmt)      \
    do {                           \
        auto&& st__ = (stmt);      \
        DCHECK(st__.ok()) << st__; \
    } while (0)

} // namespace starrocks

#define RETURN_IF(cond, ret) \
    do {                     \
        if (cond) {          \
            return ret;      \
        }                    \
    } while (0)

#define RETURN_IF_UNLIKELY_NULL(ptr, ret) \
    do {                                  \
        if (UNLIKELY(ptr == nullptr)) {   \
            return ret;                   \
        }                                 \
    } while (0)

#define RETURN_IF_UNLIKELY(cond, ret) \
    do {                              \
        if (UNLIKELY(cond)) {         \
            return ret;               \
        }                             \
    } while (0)

#define RETURN_IF_EXCEPTION(stmt)                   \
    do {                                            \
        try {                                       \
            { stmt; }                               \
        } catch (const std::exception& e) {         \
            return Status::InternalError(e.what()); \
        }                                           \
    } while (0)
