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

#include <iostream>
#include <sstream>

#include "gtest/gtest.h"

namespace starrocks {

struct CpuInfoTest : public ::testing::Test {
    void SetUp() {
        CpuInfo::init();
        value = *CpuInfo::TEST_mutable_hardware_flags();
    }

    void TearDown() {
        int64_t* flag = CpuInfo::TEST_mutable_hardware_flags();
        *flag = value;
    }

    int64_t value;
};

TEST_F(CpuInfoTest, test_pass_cpu_flags_check) {
    // should be always success when the runtime env is the same as the env where the binary is built from
    auto sets = CpuInfo::unsupported_cpu_flags_from_current_env();
    EXPECT_TRUE(sets.empty());
}

TEST_F(CpuInfoTest, test_fail_cpu_flags_check) {
#if defined(__x86_64__) && defined(__AVX2__)
    int64_t* flags = CpuInfo::TEST_mutable_hardware_flags();
    EXPECT_TRUE(*flags & CpuInfo::AVX2);
    // clear AVX2 flags, simulate that the platform doesn't support avx2
    *flags &= ~CpuInfo::AVX2;
    EXPECT_FALSE(*flags & CpuInfo::AVX2);
    EXPECT_FALSE(CpuInfo::is_supported(CpuInfo::AVX2));
    auto unsupported_flags = CpuInfo::unsupported_cpu_flags_from_current_env();
    EXPECT_EQ(1, unsupported_flags.size());
    EXPECT_EQ("avx2", unsupported_flags.front());
    // restore the flag
    *flags |= CpuInfo::AVX2;
#else
    GTEST_SKIP() << "avx2 is not supported, skip the test!";
#endif
}

#if defined(__aarch64__)
TEST(ArmCpuInfoConstants, BitmaskValues) {
    EXPECT_EQ(int64_t(1LL << 1), int64_t(CpuInfo::ARM_NEON));
    EXPECT_EQ(int64_t(1LL << 2), int64_t(CpuInfo::ARM_CRC32));
    EXPECT_EQ(int64_t(1LL << 3), int64_t(CpuInfo::ARM_PMULL));
    EXPECT_EQ(int64_t(1LL << 4), int64_t(CpuInfo::ARM_AES));
    EXPECT_EQ(int64_t(1LL << 5), int64_t(CpuInfo::ARM_LSE));
    EXPECT_EQ(int64_t(1LL << 6), int64_t(CpuInfo::ARM_SVE));
    EXPECT_EQ(int64_t(1LL << 7), int64_t(CpuInfo::ARM_SVE2));
    EXPECT_EQ(int64_t(1LL << 8), int64_t(CpuInfo::ARM_SHA1));
    EXPECT_EQ(int64_t(1LL << 9), int64_t(CpuInfo::ARM_SHA2));
    const int64_t all_arm = CpuInfo::ARM_NEON | CpuInfo::ARM_CRC32 | CpuInfo::ARM_PMULL | CpuInfo::ARM_AES |
                            CpuInfo::ARM_LSE | CpuInfo::ARM_SVE | CpuInfo::ARM_SVE2 | CpuInfo::ARM_SHA1 |
                            CpuInfo::ARM_SHA2;
    EXPECT_EQ((1LL << 10) - (1LL << 1), all_arm);
}
#elif defined(__x86_64__) || defined(__i386__)
TEST(X86CpuInfoConstants, BitmaskValues) {
    EXPECT_EQ(int64_t(1LL << 1), int64_t(CpuInfo::SSSE3));
    EXPECT_EQ(int64_t(1LL << 2), int64_t(CpuInfo::SSE4_1));
    EXPECT_EQ(int64_t(1LL << 3), int64_t(CpuInfo::SSE4_2));
    EXPECT_EQ(int64_t(1LL << 4), int64_t(CpuInfo::POPCNT));
    EXPECT_EQ(int64_t(1LL << 5), int64_t(CpuInfo::AVX));
    EXPECT_EQ(int64_t(1LL << 6), int64_t(CpuInfo::AVX2));
    EXPECT_EQ(int64_t(1LL << 7), int64_t(CpuInfo::AVX512F));
    EXPECT_EQ(int64_t(1LL << 8), int64_t(CpuInfo::AVX512BW));
    const int64_t all_x86 = CpuInfo::SSSE3 | CpuInfo::SSE4_1 | CpuInfo::SSE4_2 | CpuInfo::POPCNT | CpuInfo::AVX |
                            CpuInfo::AVX2 | CpuInfo::AVX512F | CpuInfo::AVX512BW;
    EXPECT_EQ((1LL << 9) - (1LL << 1), all_x86);
}
#endif

#if defined(__aarch64__)
TEST_F(CpuInfoTest, ArmHardwareFlagsSet) {
#if defined(__aarch64__)
    // NEON is mandatory on all AArch64 processors — must always be set.
    EXPECT_TRUE(CpuInfo::is_supported(CpuInfo::ARM_NEON))
            << "ARM_NEON must be set on any AArch64 host after CpuInfo::init()";
    // CRC32 is expected on all modern server ARM64 (Graviton 2+, Neoverse N1+, Apple M1+).
    // We do not REQUIRE it here because very old ARMv8.0 cores may lack it, but we log
    // a warning if it's absent so test engineers notice immediately.
    if (!CpuInfo::is_supported(CpuInfo::ARM_CRC32)) {
        std::cerr << "[WARNING] ARM_CRC32 not detected — this host may be a very old ARMv8.0 core.\n";
    }
#else
    GTEST_SKIP() << "ARM hardware flags are not applicable on non-aarch64 hosts.";
#endif
}
#endif

#if defined(__aarch64__)
TEST_F(CpuInfoTest, ArmFailCpuFlagsCheck) {
#if defined(__aarch64__) && defined(__ARM_NEON)
    int64_t* flags = CpuInfo::TEST_mutable_hardware_flags();
    EXPECT_TRUE(*flags & CpuInfo::ARM_NEON);

    // Simulate running an ARM_NEON binary on a hypothetical core without NEON.
    *flags &= ~CpuInfo::ARM_NEON;
    EXPECT_FALSE(*flags & CpuInfo::ARM_NEON);
    EXPECT_FALSE(CpuInfo::is_supported(CpuInfo::ARM_NEON));

    const auto unsupported = CpuInfo::unsupported_cpu_flags_from_current_env();
    bool found_neon = false;
    for (const auto& f : unsupported) {
        if (f == "asimd") {
            found_neon = true;
            break;
        }
    }
    EXPECT_TRUE(found_neon) << "Expected \"asimd\" in unsupported flags when ARM_NEON is cleared";

    // Restore.
    *flags |= CpuInfo::ARM_NEON;
    EXPECT_TRUE(CpuInfo::is_supported(CpuInfo::ARM_NEON));
    EXPECT_TRUE(CpuInfo::unsupported_cpu_flags_from_current_env().empty());
#else
    GTEST_SKIP() << "AArch64 NEON is not active in this build — skipping ARM guard test.";
#endif
}
#endif

#if defined(__aarch64__)
TEST(ArmCpuInfoParsing, StrictTokenMatching) {
    // Compound flag string containing substrings like "sve2", "sveaes", "svepmull"
    // Must NOT spuriously enable "sve", "aes", or "pmull" if the standalone tokens are absent.
    std::string compound_flags = "fp asimd sve2 sveaes svepmull";
    auto flags = CpuInfo::TEST_parse_flags(compound_flags, CpuInfo::TEST_flag_mappings());
    EXPECT_TRUE(flags & CpuInfo::ARM_NEON);
    EXPECT_TRUE(flags & CpuInfo::ARM_SVE2);
    EXPECT_FALSE(flags & CpuInfo::ARM_SVE) << "Compound 'sve2' must not trigger 'sve'";
    EXPECT_FALSE(flags & CpuInfo::ARM_AES) << "Compound 'sveaes' must not trigger 'aes'";
    EXPECT_FALSE(flags & CpuInfo::ARM_PMULL) << "Compound 'svepmull' must not trigger 'pmull'";
}
#endif

#if defined(__aarch64__)
TEST(ArmCpuInfoParsing, ProcCpuinfoFeaturesFallback) {
    // Simulated full /proc/cpuinfo Features line from an ARM64 server core (e.g. Neoverse V1)
    std::string cpuinfo_features =
            "fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm jscvt fcma lrcpc dcpop "
            "sha3 sm3 sm4 asimddp sha512 sve asimdfhm dit uscat ilrcpc flagm ssbs sb paca pacg dcpodp flagm2 frint "
            "sve2";
    int64_t flags = CpuInfo::TEST_parse_flags(cpuinfo_features, CpuInfo::TEST_flag_mappings());
    EXPECT_TRUE(flags & CpuInfo::ARM_NEON);
    EXPECT_TRUE(flags & CpuInfo::ARM_CRC32);
    EXPECT_TRUE(flags & CpuInfo::ARM_PMULL);
    EXPECT_TRUE(flags & CpuInfo::ARM_AES);
    EXPECT_TRUE(flags & CpuInfo::ARM_LSE);
    EXPECT_TRUE(flags & CpuInfo::ARM_SVE);
    EXPECT_TRUE(flags & CpuInfo::ARM_SVE2);
    EXPECT_TRUE(flags & CpuInfo::ARM_SHA1);
    EXPECT_TRUE(flags & CpuInfo::ARM_SHA2);

    const int64_t expected_all = CpuInfo::ARM_NEON | CpuInfo::ARM_CRC32 | CpuInfo::ARM_PMULL | CpuInfo::ARM_AES |
                                 CpuInfo::ARM_LSE | CpuInfo::ARM_SVE | CpuInfo::ARM_SVE2 | CpuInfo::ARM_SHA1 |
                                 CpuInfo::ARM_SHA2;
    EXPECT_EQ(expected_all, flags);

    // Minimal /proc/cpuinfo line
    std::string minimal_features = "fp asimd evtstrm";
    int64_t min_flags = CpuInfo::TEST_parse_flags(minimal_features, CpuInfo::TEST_flag_mappings());
    EXPECT_EQ(int64_t(CpuInfo::ARM_NEON), min_flags);

    // Features line without any recognized StarRocks flags
    std::string no_flags = "fp evtstrm cpuid";
    EXPECT_EQ(0, CpuInfo::TEST_parse_flags(no_flags, CpuInfo::TEST_flag_mappings()));
}
#endif

#if defined(__aarch64__)
TEST(ArmCpuInfoParsing, AuxvalMapping) {
    // Linux HWCAP bits
    const unsigned long hwcap_asimd = (1UL << 1);
    const unsigned long hwcap_aes = (1UL << 3);
    const unsigned long hwcap_pmull = (1UL << 4);
    const unsigned long hwcap_sha1 = (1UL << 5);
    const unsigned long hwcap_sha2 = (1UL << 6);
    const unsigned long hwcap_crc32 = (1UL << 7);
    const unsigned long hwcap_atomics = (1UL << 8);
    const unsigned long hwcap_sve = (1UL << 22);
    const unsigned long hwcap2_sve2 = (1UL << 1);

    unsigned long hwcap =
            hwcap_asimd | hwcap_crc32 | hwcap_aes | hwcap_pmull | hwcap_sha1 | hwcap_sha2 | hwcap_atomics | hwcap_sve;
    unsigned long hwcap2 = hwcap2_sve2;

    int64_t flags = CpuInfo::TEST_init_arm_auxval(hwcap, hwcap2);
    EXPECT_TRUE(flags & CpuInfo::ARM_NEON);
    EXPECT_TRUE(flags & CpuInfo::ARM_CRC32);
    EXPECT_TRUE(flags & CpuInfo::ARM_PMULL);
    EXPECT_TRUE(flags & CpuInfo::ARM_AES);
    EXPECT_TRUE(flags & CpuInfo::ARM_LSE);
    EXPECT_TRUE(flags & CpuInfo::ARM_SVE);
    EXPECT_TRUE(flags & CpuInfo::ARM_SVE2);
    EXPECT_TRUE(flags & CpuInfo::ARM_SHA1);
    EXPECT_TRUE(flags & CpuInfo::ARM_SHA2);

    EXPECT_EQ(0, CpuInfo::TEST_init_arm_auxval(0, 0));
}
#endif

#if defined(__aarch64__)
TEST(ArmCpuInfoParsing, HeterogeneousCoreProcfsIntersection) {
    // In a big.LITTLE / heterogeneous setup, core 0 might have SVE and crypto while core 1 lacks them.
    // The fallback procfs parser must intersect all online core feature sets so non-common
    // extensions are excluded, preventing SIGILL crashes.

    // Case 1: Feature-rich core first, followed by feature-poor core
    std::string stream_rich_first =
            "processor   : 0\n"
            "Features    : fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics sve sve2\n\n"
            "processor   : 1\n"
            "Features    : fp asimd evtstrm crc32\n";
    std::istringstream iss1(stream_rich_first);
    int64_t flags1 = CpuInfo::TEST_init_arm_procfs(iss1);

    EXPECT_TRUE(flags1 & CpuInfo::ARM_NEON);
    EXPECT_TRUE(flags1 & CpuInfo::ARM_CRC32);
    EXPECT_FALSE(flags1 & CpuInfo::ARM_AES) << "Non-common AES must be excluded via intersection";
    EXPECT_FALSE(flags1 & CpuInfo::ARM_PMULL) << "Non-common PMULL must be excluded via intersection";
    EXPECT_FALSE(flags1 & CpuInfo::ARM_SHA1) << "Non-common SHA1 must be excluded via intersection";
    EXPECT_FALSE(flags1 & CpuInfo::ARM_SHA2) << "Non-common SHA2 must be excluded via intersection";
    EXPECT_FALSE(flags1 & CpuInfo::ARM_LSE) << "Non-common LSE must be excluded via intersection";
    EXPECT_FALSE(flags1 & CpuInfo::ARM_SVE) << "Non-common SVE must be excluded via intersection";
    EXPECT_FALSE(flags1 & CpuInfo::ARM_SVE2) << "Non-common SVE2 must be excluded via intersection";
    EXPECT_EQ(CpuInfo::ARM_NEON | CpuInfo::ARM_CRC32, flags1);

    // Case 2: Feature-poor core first, followed by feature-rich core
    std::string stream_poor_first =
            "processor   : 0\n"
            "Features    : fp asimd evtstrm crc32\n\n"
            "processor   : 1\n"
            "Features    : fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics sve sve2\n";
    std::istringstream iss2(stream_poor_first);
    int64_t flags2 = CpuInfo::TEST_init_arm_procfs(iss2);
    EXPECT_EQ(CpuInfo::ARM_NEON | CpuInfo::ARM_CRC32, flags2);

    // Case 3: Empty stream or no Features lines found
    std::string stream_no_features =
            "processor   : 0\n"
            "model name  : ARMv8 Processor rev 0 (v8l)\n";
    std::istringstream iss3(stream_no_features);
    EXPECT_EQ(0, CpuInfo::TEST_init_arm_procfs(iss3));
}
#endif

#if defined(__aarch64__)
TEST(ArmCpuInfoParsing, ProcfsTruncatedStreamFailClosed) {
    // Corrupted or truncated stream with I/O error must fail closed
    std::string stream_data =
            "processor   : 0\n"
            "Features    : fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics\n";
    std::istringstream iss(stream_data);
    iss.setstate(std::ios::badbit);
    EXPECT_EQ(0, CpuInfo::TEST_init_arm_procfs(iss))
            << "Truncated or errored stream must fail closed and return 0 flags";
}
#endif

TEST(ArmCpuInfoParsing, AuxvalPriorityOverProcfsFallback) {
    // Plain bit literals, not CpuInfo::ARM_* constants: this test verifies arch-agnostic
    // precedence-selection logic and must keep compiling on every architecture.
    const int64_t aux_flags = (1LL << 0) | (1LL << 1);
    const int64_t procfs_flags = (1LL << 0) | (1LL << 1) | (1LL << 2) | (1LL << 3);

    // When getauxval() is available, aux_flags are authoritative
    int64_t resolved = CpuInfo::TEST_resolve_arm_flags(true, aux_flags, procfs_flags);
    EXPECT_EQ(aux_flags, resolved);
    EXPECT_FALSE(resolved & (1LL << 2));
    EXPECT_FALSE(resolved & (1LL << 3));

    // Even when aux_flags is 0 (minimal core), if getauxval is available, it does not fall back to procfs
    EXPECT_EQ(0, CpuInfo::TEST_resolve_arm_flags(true, 0, procfs_flags));

    // When auxval is unavailable (aux_available == false), procfs flags are used as fallback
    int64_t resolved_fallback = CpuInfo::TEST_resolve_arm_flags(false, 0, procfs_flags);
    EXPECT_EQ(procfs_flags, resolved_fallback);

    // When neither is available, flags are 0
    EXPECT_EQ(0, CpuInfo::TEST_resolve_arm_flags(false, 0, 0));
}

namespace {
// Synthetic vocabulary for algorithm-level tests. Deliberately NOT CpuInfo::ARM_*/x86 constants:
// these tests verify the tokenizer and multi-core-intersection algorithms themselves, so they
// must keep compiling and passing on every host and target architecture, independent of which
// real vocabulary this binary was built with (see Task 3, which makes the real vocabularies
// arch-exclusive).
const std::vector<CpuInfo::FlagMapping>& test_vocabulary() {
    static const std::vector<CpuInfo::FlagMapping> mappings = {
            {"alpha", 1LL << 0}, {"beta", 1LL << 1}, {"beta2", 1LL << 2}, {"betaextra", 1LL << 3}, {"gamma", 1LL << 4},
    };
    return mappings;
}
} // namespace

TEST(CpuInfoParsingAlgorithm, TokenizesAndMatchesExactly) {
    int64_t flags = CpuInfo::TEST_parse_flags("alpha beta", test_vocabulary());
    EXPECT_TRUE(flags & (1LL << 0));
    EXPECT_TRUE(flags & (1LL << 1));
    EXPECT_EQ((1LL << 0) | (1LL << 1), flags);
}

TEST(CpuInfoParsingAlgorithm, CompoundTokensDoNotMatchSubstrings) {
    // "beta2" and "betaextra" are distinct tokens from "beta" and must not be treated as
    // containing it -- matching must be exact-token, not substring.
    int64_t flags = CpuInfo::TEST_parse_flags("alpha beta2 betaextra", test_vocabulary());
    EXPECT_TRUE(flags & (1LL << 0));
    EXPECT_FALSE(flags & (1LL << 1)) << "'beta2'/'betaextra' must not spuriously match 'beta'";
    EXPECT_TRUE(flags & (1LL << 2));
    EXPECT_TRUE(flags & (1LL << 3));
}

TEST(CpuInfoParsingAlgorithm, UnknownTokensAreIgnored) {
    int64_t flags = CpuInfo::TEST_parse_flags("alpha unknown_token gamma", test_vocabulary());
    EXPECT_EQ((1LL << 0) | (1LL << 4), flags);
}

TEST(CpuInfoParsingAlgorithm, HeterogeneousCoreIntersection) {
    // Core 0 is feature-rich, core 1 is feature-poor: only the common subset must survive.
    std::string stream_data =
            "processor   : 0\n"
            "Features    : alpha beta gamma\n\n"
            "processor   : 1\n"
            "Features    : alpha gamma\n";
    std::istringstream iss(stream_data);
    int64_t flags = CpuInfo::TEST_intersect_procfs_features(iss, test_vocabulary());
    EXPECT_EQ((1LL << 0) | (1LL << 4), flags) << "Non-common 'beta' must be excluded via intersection";
}

TEST(CpuInfoParsingAlgorithm, NoFeaturesLinesYieldsZero) {
    std::string stream_data = "processor   : 0\nmodel name  : Synthetic Core\n";
    std::istringstream iss(stream_data);
    EXPECT_EQ(0, CpuInfo::TEST_intersect_procfs_features(iss, test_vocabulary()));
}

TEST(CpuInfoParsingAlgorithm, FailsClosedOnBadStream) {
    std::string stream_data = "processor   : 0\nFeatures    : alpha beta\n";
    std::istringstream iss(stream_data);
    iss.setstate(std::ios::badbit);
    EXPECT_EQ(0, CpuInfo::TEST_intersect_procfs_features(iss, test_vocabulary()))
            << "A bad stream must fail closed and return 0, never a partial/stale result";
}

TEST(CpuInfoHwcapAvailability, ZeroMeansUnavailable) {
    // NEON/ASIMD is mandatory on every conforming AArch64 core, so a genuinely all-zero HWCAP
    // is the only signal we can trust that the auxv vector could not be populated.
    EXPECT_FALSE(CpuInfo::TEST_hwcap_available(0));
}

TEST(CpuInfoHwcapAvailability, NonZeroMeansAvailable) {
    EXPECT_TRUE(CpuInfo::TEST_hwcap_available(1UL));
    EXPECT_TRUE(CpuInfo::TEST_hwcap_available(1UL << 1));
    EXPECT_TRUE(CpuInfo::TEST_hwcap_available(~0UL));
}

#if defined(__aarch64__)
TEST(ArmCpuInfoDarwin, SysctlProbing) {
    // 1. Modern Darwin sysctl probing (macOS 12+)
    auto mock_sysctl_modern = [](const char* name) -> bool {
        std::string s(name);
        return (s == "hw.optional.AdvSIMD" || s == "hw.optional.arm.FEAT_CRC32" || s == "hw.optional.arm.FEAT_PMULL" ||
                s == "hw.optional.arm.FEAT_AES" || s == "hw.optional.arm.FEAT_LSE" ||
                s == "hw.optional.arm.FEAT_SHA1" || s == "hw.optional.arm.FEAT_SHA256");
    };
    int64_t flags_modern = CpuInfo::TEST_init_arm_darwin(mock_sysctl_modern);
    EXPECT_TRUE(flags_modern & CpuInfo::ARM_NEON);
    EXPECT_TRUE(flags_modern & CpuInfo::ARM_CRC32);
    EXPECT_TRUE(flags_modern & CpuInfo::ARM_PMULL);
    EXPECT_TRUE(flags_modern & CpuInfo::ARM_AES);
    EXPECT_TRUE(flags_modern & CpuInfo::ARM_LSE);
    EXPECT_TRUE(flags_modern & CpuInfo::ARM_SHA1);
    EXPECT_TRUE(flags_modern & CpuInfo::ARM_SHA2);
    EXPECT_FALSE(flags_modern & CpuInfo::ARM_SVE);
    EXPECT_FALSE(flags_modern & CpuInfo::ARM_SVE2);

    // 2. Fail-closed verification (P2):
    // When sysctl queries fail, unknown capability state must fail closed (0 flags).
    auto mock_sysctl_fail_all = [](const char* /*name*/) -> bool { return false; };
    int64_t flags_fail_closed = CpuInfo::TEST_init_arm_darwin(mock_sysctl_fail_all);
    EXPECT_EQ(0, flags_fail_closed) << "Darwin detection must fail closed if capabilities cannot be verified";

    // 3. Restricted capability state: only NEON is affirmative, optional extensions must NOT be assumed.
    auto mock_sysctl_neon_only = [](const char* name) -> bool { return std::string(name) == "hw.optional.AdvSIMD"; };
    int64_t flags_neon_only = CpuInfo::TEST_init_arm_darwin(mock_sysctl_neon_only);
    EXPECT_EQ(int64_t(CpuInfo::ARM_NEON), flags_neon_only);
    EXPECT_FALSE(flags_neon_only & CpuInfo::ARM_CRC32);
    EXPECT_FALSE(flags_neon_only & CpuInfo::ARM_AES);
}
#endif

} // namespace starrocks
