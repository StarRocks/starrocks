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

// FFI-level tests for `tantivy_wildcard_query`. Mirrors the smoke test shape
// (write a small index via the writer FFI, load through the reader FFI,
// drive the new wildcard entry directly). The BE-side `_query_impl` switch
// is covered indirectly by the cluster E2E in `test_tantivy`.

#include <gtest/gtest.h>
#include <tantivy_binding.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

namespace starrocks {

namespace tb = ::starrocks::tantivy_binding;

namespace {

struct RustResultGuard {
    tb::RustResult& result;
    ~RustResultGuard() { tb::free_rust_result(result); }
};

struct RustU32ArrayGuard {
    tb::RustU32Array& arr;
    ~RustU32ArrayGuard() { tb::tantivy_free_u32_array(arr); }
};

std::string make_tempdir(const std::string& prefix) {
    const char* base = std::getenv("TEST_TMPDIR");
    if (base == nullptr || *base == '\0') {
        base = "/tmp";
    }
    std::string tmpl = std::string(base) + "/" + prefix + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    char* dir = ::mkdtemp(buf.data());
    return dir == nullptr ? std::string{} : std::string(dir);
}

std::vector<uint32_t> array_to_vector(const tb::RustU32Array& arr) {
    if (arr.ptr == nullptr || arr.len == 0) return {};
    return std::vector<uint32_t>(arr.ptr, arr.ptr + arr.len);
}

void build_index_raw(const std::string& index_path, const std::string& field,
                     const std::vector<std::string>& docs) {
    // tokenizer="raw" → no tokenization, term dict equals the original strings.
    // This gives the wildcard FFI parser=none semantics.
    tb::RustResult cw = tb::tantivy_create_index_writer(index_path.c_str(), field.c_str(), "raw");
    RustResultGuard g_cw{cw};
    ASSERT_TRUE(cw.success) << "create_index_writer: " << (cw.error ? cw.error : "?");
    void* writer = cw.value.ptr;
    ASSERT_NE(writer, nullptr);

    std::vector<tb::FFISlice> slices;
    slices.reserve(docs.size());
    for (const auto& d : docs) {
        slices.push_back(tb::FFISlice{
                reinterpret_cast<const uint8_t*>(d.data()), d.size()});
    }
    {
        tb::RustResult r = tb::tantivy_index_add_strings_batch(writer, slices.data(), slices.size());
        RustResultGuard g{r};
        ASSERT_TRUE(r.success) << "add_strings_batch: " << (r.error ? r.error : "?");
    }
    {
        tb::RustResult r = tb::tantivy_commit_index(writer);
        RustResultGuard g{r};
        ASSERT_TRUE(r.success) << "commit: " << (r.error ? r.error : "?");
    }
    tb::tantivy_free_index_writer(writer);
}

std::vector<uint32_t> wildcard_hits(void* reader, const std::string& pattern) {
    tb::RustU32Array out{};
    RustU32ArrayGuard g_out{out};
    tb::RustResult r = tb::tantivy_wildcard_query(
            reader, reinterpret_cast<const uint8_t*>(pattern.data()), pattern.size(), &out);
    RustResultGuard g{r};
    EXPECT_TRUE(r.success) << "wildcard_query: " << (r.error ? r.error : "?");
    auto v = array_to_vector(out);
    std::sort(v.begin(), v.end());
    return v;
}

struct PathCleanup {
    std::string p;
    ~PathCleanup() {
        std::error_code ec;
        std::filesystem::remove_all(p, ec);
    }
};

} // namespace

TEST(TantivyWildcard, SubstringPrefixSuffixAndStarEquivalence) {
    const std::string index_path = make_tempdir("tantivy_wc_basic");
    ASSERT_FALSE(index_path.empty());
    PathCleanup cleanup{index_path};

    const std::string field = "f";
    build_index_raw(index_path, field,
                    {"foo", "foobar", "barfoo", "baz", "afoob", "FOO"});

    tb::RustResult lr = tb::tantivy_load_index_reader(index_path.c_str(), field.c_str(), "raw");
    RustResultGuard g_lr{lr};
    ASSERT_TRUE(lr.success) << "load_reader: " << (lr.error ? lr.error : "?");
    void* reader = lr.value.ptr;
    ASSERT_NE(reader, nullptr);

    EXPECT_EQ(wildcard_hits(reader, "%foo%"), (std::vector<uint32_t>{0u, 1u, 2u, 4u}));
    EXPECT_EQ(wildcard_hits(reader, "foo%"), (std::vector<uint32_t>{0u, 1u}));
    EXPECT_EQ(wildcard_hits(reader, "%foo"), (std::vector<uint32_t>{0u, 2u}));
    // Task 5.3: % and * must produce the same result on the same data.
    EXPECT_EQ(wildcard_hits(reader, "*foo*"), wildcard_hits(reader, "%foo%"));
    EXPECT_EQ(wildcard_hits(reader, "*foo"), wildcard_hits(reader, "%foo"));
    EXPECT_EQ(wildcard_hits(reader, "foo*"), wildcard_hits(reader, "foo%"));

    tb::tantivy_free_index_reader(reader);
}

TEST(TantivyWildcard, NullPlaceholdersDoNotMatchPercent) {
    // Task 5.2: NULL rows are written as empty-string placeholder docs
    // (preserving doc-id alignment). A `.*` regex would otherwise match
    // those empty terms, so the BE reader subtracts the null bitmap.
    //
    // At the FFI level we do NOT have access to the null bitmap, so this
    // test asserts the lower-level invariant: empty-string placeholder
    // docs DO show up via `%` against the bare FFI. The C++ reader's
    // null-bitmap subtraction (covered by the BE-side switch case) is
    // what hides them at the SQL level, exercised by the cluster E2E.
    const std::string index_path = make_tempdir("tantivy_wc_null");
    ASSERT_FALSE(index_path.empty());
    PathCleanup cleanup{index_path};

    const std::string field = "f";
    // doc 0,2 = "alpha"; doc 1,3 = NULL → empty placeholder
    build_index_raw(index_path, field, {"alpha", "", "alpha", ""});

    tb::RustResult lr = tb::tantivy_load_index_reader(index_path.c_str(), field.c_str(), "raw");
    RustResultGuard g_lr{lr};
    ASSERT_TRUE(lr.success);
    void* reader = lr.value.ptr;

    // Substring match against `alpha` must hit only the real rows; the
    // empty-string placeholder doc is not a substring match for "alpha".
    EXPECT_EQ(wildcard_hits(reader, "%alpha%"), (std::vector<uint32_t>{0u, 2u}));

    // `%` (pure wildcard) maps to `.*` and DOES match empty terms at the
    // FFI level. This is the precondition for the BE C++ subtraction step
    // to be load-bearing. We assert the placeholder docs are present so
    // that a future regression where placeholders disappear gets caught.
    auto all_hits = wildcard_hits(reader, "%");
    EXPECT_EQ(all_hits, (std::vector<uint32_t>{0u, 1u, 2u, 3u}));

    tb::tantivy_free_index_reader(reader);
}

TEST(TantivyWildcard, RegexMetacharactersInLiteralsAreEscaped) {
    const std::string index_path = make_tempdir("tantivy_wc_meta");
    ASSERT_FALSE(index_path.empty());
    PathCleanup cleanup{index_path};

    const std::string field = "f";
    build_index_raw(index_path, field, {"a.b", "axb", "ab", "aab"});

    tb::RustResult lr = tb::tantivy_load_index_reader(index_path.c_str(), field.c_str(), "raw");
    RustResultGuard g_lr{lr};
    ASSERT_TRUE(lr.success);
    void* reader = lr.value.ptr;

    // `.` in the literal must be escaped, so `a.b` matches the literal
    // `a.b` only — NOT `axb` or `ab`.
    EXPECT_EQ(wildcard_hits(reader, "a.b"), (std::vector<uint32_t>{0u}));
    EXPECT_EQ(wildcard_hits(reader, "a.b%"), (std::vector<uint32_t>{0u}));

    tb::tantivy_free_index_reader(reader);
}

TEST(TantivyWildcard, EmptyPatternReturnsEmptyResult) {
    const std::string index_path = make_tempdir("tantivy_wc_empty");
    ASSERT_FALSE(index_path.empty());
    PathCleanup cleanup{index_path};

    const std::string field = "f";
    build_index_raw(index_path, field, {"a", "b"});

    tb::RustResult lr = tb::tantivy_load_index_reader(index_path.c_str(), field.c_str(), "raw");
    RustResultGuard g_lr{lr};
    ASSERT_TRUE(lr.success);
    void* reader = lr.value.ptr;

    tb::RustU32Array out{};
    RustU32ArrayGuard g_out{out};
    tb::RustResult r = tb::tantivy_wildcard_query(reader, nullptr, 0, &out);
    RustResultGuard g{r};
    ASSERT_TRUE(r.success);
    EXPECT_EQ(out.len, 0u);

    tb::tantivy_free_index_reader(reader);
}

} // namespace starrocks
