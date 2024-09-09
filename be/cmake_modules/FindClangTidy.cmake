option(WITH_CLANG_TIDY "Use clang-tidy static analyzer" OFF)

if(${WITH_CLANG_TIDY})
  find_program(CLANG_TIDY_CACHE_BIN NAMES "clang-tidy-cache")
  find_program(
    CLANG_TIDY_BIN NAMES "clang-tidy-17" "clang-tidy-16" "clang-tidy-15"
                         "clang-tidy-14" "clang-tidy-13" "clang-tidy")

  if(CLANG_TIDY_CACHE_BIN)
    set(CLANG_TIDY_PATH
        "${CLANG_TIDY_CACHE_BIN};${CLANG_TIDY_BIN}"
        CACHE STRING "Cache for clang-tidy")
  else()
    set(CLANG_TIDY_PATH "${CLANG_TIDY_BIN}")
  endif()

  if(CLANG_TIDY_PATH)
    message(STATUS "Find clang-tidy: ${CLANG_TIDY_PATH}.")
  else()
    message(FATAL_ERROR "clang-tidy is not found")
  endif()
endif()
