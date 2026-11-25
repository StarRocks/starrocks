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

#include "common/system/cpu_info.h"

#if defined(__linux__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#endif

#include <sstream>

namespace starrocks {

// ARM64 Linux HWCAP / HWCAP2 constants for getauxval
#ifndef HWCAP_ASIMD
#define HWCAP_ASIMD (1UL << 1)
#endif
#ifndef HWCAP_AES
#define HWCAP_AES (1UL << 3)
#endif
#ifndef HWCAP_PMULL
#define HWCAP_PMULL (1UL << 4)
#endif
#ifndef HWCAP_SHA1
#define HWCAP_SHA1 (1UL << 5)
#endif
#ifndef HWCAP_SHA2
#define HWCAP_SHA2 (1UL << 6)
#endif
#ifndef HWCAP_CRC32
#define HWCAP_CRC32 (1UL << 7)
#endif
#ifndef HWCAP_ATOMICS
#define HWCAP_ATOMICS (1UL << 8)
#endif
#ifndef HWCAP_SVE
#define HWCAP_SVE (1UL << 22)
#endif
#ifndef HWCAP2_SVE2
#define HWCAP2_SVE2 (1UL << 1)
#endif

const std::vector<CpuInfo::FlagMapping>& CpuInfo::_flag_mappings() {
    static const std::vector<FlagMapping> mappings = {
            {"asimd", CpuInfo::ARM_NEON}, {"crc32", CpuInfo::ARM_CRC32}, {"pmull", CpuInfo::ARM_PMULL},
            {"aes", CpuInfo::ARM_AES},    {"atomics", CpuInfo::ARM_LSE}, {"sve", CpuInfo::ARM_SVE},
            {"sve2", CpuInfo::ARM_SVE2},  {"sha1", CpuInfo::ARM_SHA1},   {"sha2", CpuInfo::ARM_SHA2},
    };
    return mappings;
}

int64_t CpuInfo::_init_arm_auxval(unsigned long hwcap, unsigned long hwcap2) {
    int64_t flags = 0;
    if (hwcap & HWCAP_ASIMD) flags |= ARM_NEON;
    if (hwcap & HWCAP_CRC32) flags |= ARM_CRC32;
    if (hwcap & HWCAP_PMULL) flags |= ARM_PMULL;
    if (hwcap & HWCAP_AES) flags |= ARM_AES;
    if (hwcap & HWCAP_ATOMICS) flags |= ARM_LSE;
    if (hwcap & HWCAP_SVE) flags |= ARM_SVE;
    if (hwcap2 & HWCAP2_SVE2) flags |= ARM_SVE2;
    if (hwcap & HWCAP_SHA1) flags |= ARM_SHA1;
    if (hwcap & HWCAP_SHA2) flags |= ARM_SHA2;
    return flags;
}

int64_t CpuInfo::_init_arm_procfs(std::istream& stream) {
    return _intersect_procfs_features(stream, _flag_mappings());
}

int64_t CpuInfo::_init_arm_darwin(const std::function<bool(const char*)>& check_sysctl) {
    int64_t flags = 0;

    // Advanced SIMD / NEON
    if (check_sysctl("hw.optional.AdvSIMD") || check_sysctl("hw.optional.neon") ||
        check_sysctl("hw.optional.arm.AdvSIMD") || check_sysctl("hw.optional.floatingpoint")) {
        flags |= ARM_NEON;
    }

    // CRC32
    if (check_sysctl("hw.optional.armv8_crc32") || check_sysctl("hw.optional.arm.FEAT_CRC32")) {
        flags |= ARM_CRC32;
    }

    // Large System Extensions (LSE Atomics)
    if (check_sysctl("hw.optional.armv8_1_atomics") || check_sysctl("hw.optional.arm.FEAT_LSE")) {
        flags |= ARM_LSE;
    }

    // Crypto extensions
    if (check_sysctl("hw.optional.arm.FEAT_PMULL")) {
        flags |= ARM_PMULL;
    }
    if (check_sysctl("hw.optional.arm.FEAT_AES")) {
        flags |= ARM_AES;
    }
    if (check_sysctl("hw.optional.arm.FEAT_SHA1")) {
        flags |= ARM_SHA1;
    }
    if (check_sysctl("hw.optional.arm.FEAT_SHA256") || check_sysctl("hw.optional.arm.FEAT_SHA2")) {
        flags |= ARM_SHA2;
    }

    // SVE / SVE2
    if (check_sysctl("hw.optional.arm.FEAT_SVE")) {
        flags |= ARM_SVE;
    }
    if (check_sysctl("hw.optional.arm.FEAT_SVE2")) {
        flags |= ARM_SVE2;
    }

    // Fail closed: Unknown capability state must NOT enable optional extensions blindly.
    return flags;
}

int64_t CpuInfo::_detect_hardware_flags(const std::string& cpuinfo_content) {
    int64_t flags = _parse_procfs_flags(cpuinfo_content);
#if defined(__linux__)
    const unsigned long hwcap = getauxval(AT_HWCAP);
    const unsigned long hwcap2 = getauxval(AT_HWCAP2);
    const bool hwcap_available = _hwcap_available(hwcap);
    const int64_t aux_flags = _init_arm_auxval(hwcap, hwcap2);
    int64_t procfs_arm_flags = 0;
    if (!hwcap_available) {
        std::istringstream arm_cpuinfo(cpuinfo_content);
        procfs_arm_flags = _init_arm_procfs(arm_cpuinfo);
    }
    flags |= _resolve_arm_flags(hwcap_available, aux_flags, procfs_arm_flags);
#elif defined(__APPLE__)
    auto check_sysctl = [](const char* name) -> bool {
        int val = 0;
        size_t len = sizeof(val);
        return sysctlbyname(name, &val, &len, nullptr, 0) == 0 && val != 0;
    };
    flags |= _init_arm_darwin(check_sysctl);
#endif
    return flags;
}

int64_t CpuInfo::_compiled_hardware_flags() {
    int64_t flags = 0;
#if defined(__ARM_NEON)
    flags |= ARM_NEON;
#endif
#if defined(__ARM_FEATURE_CRC32)
    flags |= ARM_CRC32;
#endif
#if (defined(__ARM_FEATURE_CRYPTO) || defined(__ARM_FEATURE_PMULL))
    flags |= ARM_PMULL;
#endif
#if (defined(__ARM_FEATURE_CRYPTO) || defined(__ARM_FEATURE_AES))
    flags |= ARM_AES;
#endif
#if defined(__ARM_FEATURE_ATOMICS)
    flags |= ARM_LSE;
#endif
#if defined(__ARM_FEATURE_SVE)
    flags |= ARM_SVE;
#endif
#if defined(__ARM_FEATURE_SVE2)
    flags |= ARM_SVE2;
#endif
#if (defined(__ARM_FEATURE_CRYPTO) || defined(__ARM_FEATURE_SHA1))
    flags |= ARM_SHA1;
#endif
#if (defined(__ARM_FEATURE_CRYPTO) || defined(__ARM_FEATURE_SHA2))
    flags |= ARM_SHA2;
#endif
    return flags;
}

} // namespace starrocks
