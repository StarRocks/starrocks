# First, check if running on GitHub Actions
if (DEFINED ENV{GITHUB_ACTIONS} AND "$ENV{GITHUB_ACTIONS}" STREQUAL "true")
    message(STATUS "Running on GitHub Actions runner.")
    set(IS_GITHUB_ACTIONS ON)
endif ()

# Check if the compiler supports -mavx512dq
check_cxx_compiler_flag("-mavx512dq" COMPILER_SUPPORTS_AVX512DQ)

# Detect AVX-512DQ hardware support via /proc/cpuinfo
set(HAS_HW_AVX512DQ OFF)
if (EXISTS "/proc/cpuinfo")
    file(READ "/proc/cpuinfo" CPUINFO_CONTENT)
    if (CPUINFO_CONTENT MATCHES "avx512dq")
        set(HAS_HW_AVX512DQ ON)
    endif ()
endif ()

if (COMPILER_SUPPORTS_AVX512DQ AND HAS_HW_AVX512DQ AND NOT IS_GITHUB_ACTIONS)
    message(STATUS "Compiler and hardware both support AVX-512DQ. Adding flag '-mavx512dq'.")
    set(FLAGS "-mavx512dq")
elseif (COMPILER_SUPPORTS_AVX512DQ AND HAS_HW_AVX512DQ AND IS_GITHUB_ACTIONS)
    message(WARNING "Hardware supports AVX-512DQ, but not adding on GitHub Actions runner.")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|i[3-6]86")
    message(STATUS "Setting '-march=native' for x86 processors without AVX-512DQ.")
    set(FLAGS "-march=native")
else ()
    message(STATUS "No instruction set flags applied.")
endif ()


# Define the library
add_library(ALP
        SHARED
        falp.cpp
        fastlanes_generated_unffor.cpp
        fastlanes_generated_ffor.cpp
        fastlanes_ffor.cpp
        fastlanes_unffor.cpp
)

# Add AVX-512DQ flag if supported and not on GitHub Actions
if (FLAGS)
    target_compile_options(ALP PUBLIC ${FLAGS})
endif ()
