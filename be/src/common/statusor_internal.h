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
#pragma once

#include <type_traits>
#include <utility>

#include "common/status.h"

namespace starrocks {

template <typename T>
class [[nodiscard]] StatusOr;

namespace internal_statusor {

// Detects whether `U` has conversion operator to `StatusOr<T>`, i.e. `operator
// StatusOr<T>()`.
template <typename T, typename U, typename = void>
struct HasConversionOperatorToStatusOr : std::false_type {};

template <typename T, typename U>
void test(char (*)[sizeof(std::declval<U>().operator starrocks::StatusOr<T>())]);

template <typename T, typename U>
struct HasConversionOperatorToStatusOr<T, U, decltype(test<T, U>(0))> : std::true_type {};

// Detects whether `T` is constructible or convertible from `StatusOr<U>`.
template <typename T, typename U>
using IsConstructibleOrConvertibleFromStatusOr =
        std::disjunction<std::is_constructible<T, StatusOr<U>&>, std::is_constructible<T, const StatusOr<U>&>,
                         std::is_constructible<T, StatusOr<U>&&>, std::is_constructible<T, const StatusOr<U>&&>,
                         std::is_convertible<StatusOr<U>&, T>, std::is_convertible<const StatusOr<U>&, T>,
                         std::is_convertible<StatusOr<U>&&, T>, std::is_convertible<const StatusOr<U>&&, T>>;

// Detects whether `T` is constructible or convertible or assignable from
// `StatusOr<U>`.
template <typename T, typename U>
using IsConstructibleOrConvertibleOrAssignableFromStatusOr =
        std::disjunction<IsConstructibleOrConvertibleFromStatusOr<T, U>, std::is_assignable<T&, StatusOr<U>&>,
                         std::is_assignable<T&, const StatusOr<U>&>, std::is_assignable<T&, StatusOr<U>&&>,
                         std::is_assignable<T&, const StatusOr<U>&&>>;

// Detects whether direct initializing `StatusOr<T>` from `U` is ambiguous, i.e.
// when `U` is `StatusOr<V>` and `T` is constructible or convertible from `V`.
template <typename T, typename U>
struct IsDirectInitializationAmbiguous
        : public std::conditional_t<std::is_same<std::remove_cv_t<std::remove_reference_t<U>>, U>::value,
                                    std::false_type,
                                    IsDirectInitializationAmbiguous<T, std::remove_cv_t<std::remove_reference_t<U>>>> {
};

template <typename T, typename V>
struct IsDirectInitializationAmbiguous<T, starrocks::StatusOr<V>>
        : public IsConstructibleOrConvertibleFromStatusOr<T, V> {};

// Checks against the constraints of the direction initialization, i.e. when
// `StatusOr<T>::StatusOr(U&&)` should participate in overload resolution.
template <typename T, typename U>
using IsDirectInitializationValid = std::disjunction<
        // Short circuits if T is basically U.
        std::is_same<T, std::remove_cv_t<std::remove_reference_t<U>>>,
        std::negation<
                std::disjunction<std::is_same<starrocks::StatusOr<T>, std::remove_cv_t<std::remove_reference_t<U>>>,
                                 std::is_same<starrocks::Status, std::remove_cv_t<std::remove_reference_t<U>>>,
                                 std::is_same<std::in_place_t, std::remove_cv_t<std::remove_reference_t<U>>>,
                                 IsDirectInitializationAmbiguous<T, U>>>>;

// This trait detects whether `StatusOr<T>::operator=(U&&)` is ambiguous, which
// is equivalent to whether all the following conditions are met:
// 1. `U` is `StatusOr<V>`.
// 2. `T` is constructible and assignable from `V`.
// 3. `T` is constructible and assignable from `U` (i.e. `StatusOr<V>`).
// For example, the following code is considered ambiguous:
// (`T` is `bool`, `U` is `StatusOr<bool>`, `V` is `bool`)
//   StatusOr<bool> s1 = true;  // s1.ok() && s1.ValueOrDie() == true
//   StatusOr<bool> s2 = false;  // s2.ok() && s2.ValueOrDie() == false
//   s1 = s2;  // ambiguous, `s1 = s2.ValueOrDie()` or `s1 = bool(s2)`?
template <typename T, typename U>
struct IsForwardingAssignmentAmbiguous
        : public std::conditional_t<std::is_same<std::remove_cv_t<std::remove_reference_t<U>>, U>::value,
                                    std::false_type,
                                    IsForwardingAssignmentAmbiguous<T, std::remove_cv_t<std::remove_reference_t<U>>>> {
};

template <typename T, typename U>
struct IsForwardingAssignmentAmbiguous<T, starrocks::StatusOr<U>>
        : public IsConstructibleOrConvertibleOrAssignableFromStatusOr<T, U> {};

// Checks against the constraints of the forwarding assignment, i.e. whether
// `StatusOr<T>::operator(U&&)` should participate in overload resolution.
template <typename T, typename U>
using IsForwardingAssignmentValid = std::disjunction<
        // Short circuits if T is basically U.
        std::is_same<T, std::remove_cv_t<std::remove_reference_t<U>>>,
        std::negation<
                std::disjunction<std::is_same<starrocks::StatusOr<T>, std::remove_cv_t<std::remove_reference_t<U>>>,
                                 std::is_same<starrocks::Status, std::remove_cv_t<std::remove_reference_t<U>>>,
                                 std::is_same<std::in_place_t, std::remove_cv_t<std::remove_reference_t<U>>>,
                                 IsForwardingAssignmentAmbiguous<T, U>>>>;

class Helper {
public:
    // Move type-agnostic error handling to the .cc.
    static void HandleInvalidStatusCtorArg(Status*);
    [[noreturn]] static void Crash(const starrocks::Status& status);
};

// Construct an instance of T in `p` through placement new, passing Args... to
// the constructor.
// This abstraction is here mostly for the gcc performance fix.
template <typename T, typename... Args>
void PlacementNew(void* p, Args&&... args) {
    new (p) T(std::forward<Args>(args)...);
}

// Helper base class to hold the data and all operations.
// We move all this to a base class to allow mixing with the appropriate
// TraitsBase specialization.
template <typename T>
class StatusOrData {
    template <typename U>
    friend class StatusOrData;

public:
    StatusOrData() = delete;

    StatusOrData(const StatusOrData& other) {
        if (other.ok()) {
            MakeValue(other.data_);
            MakeStatus();
        } else {
            MakeStatus(other.status_);
        }
    }

    StatusOrData(StatusOrData&& other) noexcept {
        if (other.ok()) {
            MakeValue(std::move(other.data_));
            MakeStatus();
        } else {
            MakeStatus(std::move(other.status_));
        }
    }

    template <typename U>
    explicit StatusOrData(const StatusOrData<U>& other) {
        if (other.ok()) {
            MakeValue(other.data_);
            MakeStatus();
        } else {
            MakeStatus(other.status_);
        }
    }

    template <typename U>
    explicit StatusOrData(StatusOrData<U>&& other) {
        if (other.ok()) {
            MakeValue(std::move(other.data_));
            MakeStatus();
        } else {
            MakeStatus(std::move(other.status_));
        }
    }

    template <typename... Args>
    explicit StatusOrData(std::in_place_t, Args&&... args) : data_(std::forward<Args>(args)...) {
        MakeStatus();
    }

    explicit StatusOrData(const T& value) : data_(value) { MakeStatus(); }
    explicit StatusOrData(T&& value) : data_(std::move(value)) { MakeStatus(); }

    template <typename U, std::enable_if_t<std::is_constructible<starrocks::Status, U&&>::value, int> = 0>
    explicit StatusOrData(U&& v) : status_(std::forward<U>(v)) {
        EnsureNotOk();
    }

    StatusOrData& operator=(const StatusOrData& other) {
        if (this == &other) return *this;
        if (other.ok())
            Assign(other.data_);
        else
            AssignStatus(other.status_);
        return *this;
    }

    StatusOrData& operator=(StatusOrData&& other) noexcept {
        if (this == &other) return *this;
        if (other.ok())
            Assign(std::move(other.data_));
        else
            AssignStatus(std::move(other.status_));
        return *this;
    }

    ~StatusOrData() {
        if (ok()) {
            status_.~Status();
            data_.~T();
        } else {
            status_.~Status();
        }
    }

    template <typename U>
    void Assign(U&& value) {
        if (ok()) {
            data_ = std::forward<U>(value);
        } else {
            MakeValue(std::forward<U>(value));
            status_ = Status::OK();
        }
    }

    template <typename U>
    void AssignStatus(U&& v) {
        Clear();
        status_ = static_cast<starrocks::Status>(std::forward<U>(v));
        EnsureNotOk();
    }

    bool ok() const { return status_.ok(); }

protected:
    // status_ will always be active after the constructor.
    // We make it a union to be able to initialize exactly how we need without
    // waste.
    // Eg. in the copy constructor we use the default constructor of Status in
    // the ok() path to avoid an extra Ref call.
    union {
        Status status_;
    };

    // 'data_' is active if 'status_.ok() == true'.
    struct Dummy {};
    union {
        // When 'T' is const, we need some non-const object we can cast to void* for
        // the placement new. 'dummy_' is that object.
        Dummy dummy_;
        T data_;
    };

    void Clear() {
        if (ok()) data_.~T();
    }

    void EnsureOk() const {
        if (!ok()) Helper::Crash(status_);
    }

    void EnsureNotOk() {
        if (ok()) Helper::HandleInvalidStatusCtorArg(&status_);
    }

    // Construct the value (ie. data_) through placement new with the passed
    // argument.
    template <typename... Arg>
    void MakeValue(Arg&&... arg) {
        internal_statusor::PlacementNew<T>(&dummy_, std::forward<Arg>(arg)...);
    }

    // Construct the status (ie. status_) through placement new with the passed
    // argument.
    template <typename... Args>
    void MakeStatus(Args&&... args) {
        internal_statusor::PlacementNew<Status>(&status_, std::forward<Args>(args)...);
    }
};

// Helper base classes to allow implicitly deleted constructors and assignment
// operators in `StatusOr`. For example, `CopyCtorBase` will explicitly delete
// the copy constructor when T is not copy constructible and `StatusOr` will
// inherit that behavior implicitly.
template <typename T, bool = std::is_copy_constructible<T>::value>
struct CopyCtorBase {
    CopyCtorBase() = default;
    CopyCtorBase(const CopyCtorBase&) = default;
    CopyCtorBase(CopyCtorBase&&) noexcept = default;
    CopyCtorBase& operator=(const CopyCtorBase&) = default;
    CopyCtorBase& operator=(CopyCtorBase&&) noexcept = default;
};

template <typename T>
struct CopyCtorBase<T, false> {
    CopyCtorBase() = default;
    CopyCtorBase(const CopyCtorBase&) = delete;
    CopyCtorBase(CopyCtorBase&&) noexcept = default;
    CopyCtorBase& operator=(const CopyCtorBase&) = default;
    CopyCtorBase& operator=(CopyCtorBase&&) noexcept = default;
};

template <typename T, bool = std::is_move_constructible<T>::value>
struct MoveCtorBase {
    MoveCtorBase() = default;
    MoveCtorBase(const MoveCtorBase&) = default;
    MoveCtorBase(MoveCtorBase&&) noexcept = default;
    MoveCtorBase& operator=(const MoveCtorBase&) = default;
    MoveCtorBase& operator=(MoveCtorBase&&) noexcept = default;
};

template <typename T>
struct MoveCtorBase<T, false> {
    MoveCtorBase() = default;
    MoveCtorBase(const MoveCtorBase&) = default;
    MoveCtorBase(MoveCtorBase&&) = delete;
    MoveCtorBase& operator=(const MoveCtorBase&) = default;
    MoveCtorBase& operator=(MoveCtorBase&&) noexcept = default;
};

template <typename T, bool = std::is_copy_constructible<T>::value&& std::is_copy_assignable<T>::value>
struct CopyAssignBase {
    CopyAssignBase() = default;
    CopyAssignBase(const CopyAssignBase&) = default;
    CopyAssignBase(CopyAssignBase&&) noexcept = default;
    CopyAssignBase& operator=(const CopyAssignBase&) = default;
    CopyAssignBase& operator=(CopyAssignBase&&) noexcept = default;
};

template <typename T>
struct CopyAssignBase<T, false> {
    CopyAssignBase() = default;
    CopyAssignBase(const CopyAssignBase&) = default;
    CopyAssignBase(CopyAssignBase&&) noexcept = default;
    CopyAssignBase& operator=(const CopyAssignBase&) = delete;
    CopyAssignBase& operator=(CopyAssignBase&&) noexcept = default;
};

template <typename T, bool = std::is_move_constructible<T>::value&& std::is_move_assignable<T>::value>
struct MoveAssignBase {
    MoveAssignBase() = default;
    MoveAssignBase(const MoveAssignBase&) = default;
    MoveAssignBase(MoveAssignBase&&) noexcept = default;
    MoveAssignBase& operator=(const MoveAssignBase&) = default;
    MoveAssignBase& operator=(MoveAssignBase&&) noexcept = default;
};

template <typename T>
struct MoveAssignBase<T, false> {
    MoveAssignBase() = default;
    MoveAssignBase(const MoveAssignBase&) = default;
    MoveAssignBase(MoveAssignBase&&) noexcept = default;
    MoveAssignBase& operator=(const MoveAssignBase&) = default;
    MoveAssignBase& operator=(MoveAssignBase&&) = delete;
};

[[noreturn]] void ThrowBadStatusOrAccess(starrocks::Status status);

} // namespace internal_statusor
} // namespace starrocks
