// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

#include <string>
#include <tuple>

#if defined(__APPLE__)
#define OS_MACOSX 1
#elif defined(__ANDROID__)
#define OS_ANDROID 1
#elif defined(__linux__)
#define OS_LINUX 1
#elif defined(_WIN32)
#define OS_WIN 1
#endif

#if defined(_M_X64) || defined(__x86_64__)
#define ARCH_CPU_X86_FAMILY 1
#define ARCH_CPU_X86_64 1
#define ARCH_CPU_64_BITS 1
#elif defined(_M_IX86) || defined(__i386__)
#define ARCH_CPU_X86_FAMILY 1
#define ARCH_CPU_X86 1
#define ARCH_CPU_32_BITS 1
#elif defined(__ARMEL__)
#define ARCH_CPU_ARM_FAMILY 1
#define ARCH_CPU_ARMEL 1
#define ARCH_CPU_32_BITS 1
#elif defined(_M_ARM64) || defined(__aarch64__)
#define ARCH_CPU_ARM_FAMILY 1
#define ARCH_CPU_ARM64 1
#define ARCH_CPU_64_BITS 1
#endif

#if defined(OS_MACOSX) || defined(OS_LINUX) || defined(OS_ANDROID)
#define OS_POSIX 1
#endif

namespace base {
#if defined(ARCH_CPU_X86_FAMILY)
namespace internal {
// Compute the CPU family and model based on the vendor and CPUID signature.
// Returns in order: family, model, extended family, extended model.
std::tuple<int, int, int, int> ComputeX86FamilyAndModel(const std::string& vendor, int signature);
} // namespace internal
#endif // defined(ARCH_CPU_X86_FAMILY)
// Query information about the processor.
class CPU final {
public:
    CPU();
    enum IntelMicroArchitecture {
        PENTIUM,
        SSE,
        SSE2,
        SSE3,
        SSSE3,
        SSE41,
        SSE42,
        AVX,
        AVX2,
        MAX_INTEL_MICRO_ARCHITECTURE
    };
    // Accessors for CPU information.
    const std::string& vendor_name() const { return cpu_vendor_; }
    int signature() const { return signature_; }
    int stepping() const { return stepping_; }
    int model() const { return model_; }
    int family() const { return family_; }
    int type() const { return type_; }
    int extended_model() const { return ext_model_; }
    int extended_family() const { return ext_family_; }
    bool has_mmx() const { return has_mmx_; }
    bool has_sse() const { return has_sse_; }
    bool has_sse2() const { return has_sse2_; }
    bool has_sse3() const { return has_sse3_; }
    bool has_ssse3() const { return has_ssse3_; }
    bool has_sse41() const { return has_sse41_; }
    bool has_sse42() const { return has_sse42_; }
    bool has_popcnt() const { return has_popcnt_; }
    bool has_avx() const { return has_avx_; }
    bool has_avx2() const { return has_avx2_; }
    bool has_avx512f() const { return has_avx512f_; }
    bool has_avx512bw() const { return has_avx512bw_; }
    bool has_aesni() const { return has_aesni_; }
    bool has_non_stop_time_stamp_counter() const { return has_non_stop_time_stamp_counter_; }
    bool is_running_in_vm() const { return is_running_in_vm_; }
    IntelMicroArchitecture GetIntelMicroArchitecture() const;
    const std::string& cpu_brand() const { return cpu_brand_; }
    std::string debug_string() const;
    static const CPU* instance();

private:
    // Query the processor for CPUID information.
    void Initialize();
    int signature_{0}; // raw form of type, family, model, and stepping
    int type_{0};      // process type
    int family_{0};    // family of the processor
    int model_{0};     // model of processor
    int stepping_{0};  // processor revision number
    int ext_model_{0};
    int ext_family_{0};
    bool has_mmx_{false};
    bool has_sse_{false};
    bool has_sse2_{false};
    bool has_sse3_{false};
    bool has_ssse3_{false};
    bool has_sse41_{false};
    bool has_sse42_{false};
    bool has_popcnt_{false};
    bool has_avx_{false};
    bool has_avx2_{false};
    bool has_avx512f_{false};
    bool has_avx512bw_{false};
    bool has_aesni_{false};
    bool has_non_stop_time_stamp_counter_{false};
    bool is_running_in_vm_{false};
    std::string cpu_vendor_;
    std::string cpu_brand_;
};

} // namespace base
