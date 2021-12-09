// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <linux/futex.h>
#include <syscall.h>

namespace starrocks {

inline int futex_wait_private(void* addr1, int expected, const timespec* timeout) {
    return syscall(SYS_futex, addr1, FUTEX_WAIT_PRIVATE, expected, timeout, nullptr, 0);
}

inline int futex_wake_private(void* addr1, int nwake) {
    return syscall(SYS_futex, addr1, FUTEX_WAKE_PRIVATE, nwake, nullptr, nullptr, 0);
}

} // namespace starrocks
