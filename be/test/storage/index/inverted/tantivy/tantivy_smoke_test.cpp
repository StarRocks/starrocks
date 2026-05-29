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

// End-to-end smoke test for the tantivy FFI binding.
//
// Round-trips the four query types implemented for the BE plugin: term,
// match-any, match-all, phrase. Verifies the C ABI, cbindgen header, and
// libtantivy_binding.a static link are wired correctly. Also exercises null
// placeholder docs (empty strings) to confirm doc-id alignment.

#include <gtest/gtest.h>
#include <tantivy_binding.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
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
    if (dir == nullptr) {
        return {};
    }
    return std::string(dir);
}

std::vector<uint32_t> array_to_vector(const tb::RustU32Array& arr) {
    if (arr.ptr == nullptr || arr.len == 0) {
        return {};
    }
    return std::vector<uint32_t>(arr.ptr, arr.ptr + arr.len);
}

// Build a writer at `index_path`, append `docs` in order, commit, and free.
void build_index(const std::string& index_path, const std::string& field,
                 const std::vector<std::string>& docs) {
    tb::RustResult cw =
            tb::tantivy_create_index_writer(index_path.c_str(), field.c_str(), "english");
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

} // namespace

TEST(TantivySmoke, RoundTripTermQuery) {
    const std::string index_path = make_tempdir("tantivy_smoke_term");
    ASSERT_FALSE(index_path.empty());
    struct PathCleanup {
        std::string p;
        ~PathCleanup() {
            std::error_code ec;
            std::filesystem::remove_all(p, ec);
        }
    } cleanup{index_path};

    const std::string field = "title";
    build_index(index_path, field, {"hello world", "tantivy ffi", "starrocks integration"});

    tb::RustResult lr = tb::tantivy_load_index_reader(index_path.c_str(), field.c_str(), "english");
    RustResultGuard g_lr{lr};
    ASSERT_TRUE(lr.success) << "load_reader: " << (lr.error ? lr.error : "?");
    void* reader = lr.value.ptr;
    ASSERT_NE(reader, nullptr);

    {
        tb::RustU32Array out{};
        RustU32ArrayGuard g_out{out};
        const char* term = "tantivy";
        tb::RustResult r = tb::tantivy_term_query(
                reader, reinterpret_cast<const uint8_t*>(term), std::strlen(term), &out);
        RustResultGuard g{r};
        ASSERT_TRUE(r.success) << "term_query: " << (r.error ? r.error : "?");
        std::vector<uint32_t> hits = array_to_vector(out);
        std::sort(hits.begin(), hits.end());
        ASSERT_EQ(hits.size(), 1u);
        EXPECT_EQ(hits[0], 1u) << "term 'tantivy' should match doc 1";
    }
    {
        tb::RustU32Array out{};
        RustU32ArrayGuard g_out{out};
        const char* term = "nonexistent_term";
        tb::RustResult r = tb::tantivy_term_query(
                reader, reinterpret_cast<const uint8_t*>(term), std::strlen(term), &out);
        RustResultGuard g{r};
        ASSERT_TRUE(r.success);
        EXPECT_EQ(out.len, 0u);
    }

    tb::tantivy_free_index_reader(reader);
}

TEST(TantivySmoke, MatchAnyAllPhrase) {
    const std::string index_path = make_tempdir("tantivy_smoke_match");
    ASSERT_FALSE(index_path.empty());
    struct PathCleanup {
        std::string p;
        ~PathCleanup() { std::error_code ec; std::filesystem::remove_all(p, ec); }
    } cleanup{index_path};

    const std::string field = "f";
    build_index(index_path, field,
                {"the quick brown fox", "the lazy brown dog", "quick fox jumps over"});

    tb::RustResult lr = tb::tantivy_load_index_reader(index_path.c_str(), field.c_str(), "english");
    RustResultGuard g_lr{lr};
    ASSERT_TRUE(lr.success);
    void* reader = lr.value.ptr;

    auto make_slices = [](std::initializer_list<const char*> ts) {
        std::vector<tb::FFISlice> v;
        v.reserve(ts.size());
        for (const char* t : ts) {
            v.push_back(tb::FFISlice{reinterpret_cast<const uint8_t*>(t), std::strlen(t)});
        }
        return v;
    };

    // MATCH_ANY: rows containing 'fox' OR 'dog' → docs 0, 1, 2
    {
        auto terms = make_slices({"fox", "dog"});
        tb::RustU32Array out{};
        RustU32ArrayGuard g_out{out};
        tb::RustResult r = tb::tantivy_match_query(reader, terms.data(), terms.size(), &out);
        RustResultGuard g{r};
        ASSERT_TRUE(r.success) << (r.error ? r.error : "?");
        std::vector<uint32_t> hits = array_to_vector(out);
        std::sort(hits.begin(), hits.end());
        EXPECT_EQ(hits, (std::vector<uint32_t>{0u, 1u, 2u}));
    }
    // MATCH_ALL: rows containing 'quick' AND 'fox' → docs 0, 2
    {
        auto terms = make_slices({"quick", "fox"});
        tb::RustU32Array out{};
        RustU32ArrayGuard g_out{out};
        tb::RustResult r = tb::tantivy_match_all_query(reader, terms.data(), terms.size(), &out);
        RustResultGuard g{r};
        ASSERT_TRUE(r.success) << (r.error ? r.error : "?");
        std::vector<uint32_t> hits = array_to_vector(out);
        std::sort(hits.begin(), hits.end());
        EXPECT_EQ(hits, (std::vector<uint32_t>{0u, 2u}));
    }
    // MATCH_PHRASE 'quick brown' (slop=0) → only doc 0
    {
        auto terms = make_slices({"quick", "brown"});
        tb::RustU32Array out{};
        RustU32ArrayGuard g_out{out};
        tb::RustResult r = tb::tantivy_phrase_match_query(reader, terms.data(), terms.size(),
                                                          /*slop=*/0, &out);
        RustResultGuard g{r};
        ASSERT_TRUE(r.success) << (r.error ? r.error : "?");
        std::vector<uint32_t> hits = array_to_vector(out);
        std::sort(hits.begin(), hits.end());
        EXPECT_EQ(hits, (std::vector<uint32_t>{0u}));
    }
    // MATCH_PHRASE 'quick fox' (slop=1) → doc 0 (gap 1) and doc 2 (adjacent)
    {
        auto terms = make_slices({"quick", "fox"});
        tb::RustU32Array out{};
        RustU32ArrayGuard g_out{out};
        tb::RustResult r = tb::tantivy_phrase_match_query(reader, terms.data(), terms.size(),
                                                          /*slop=*/1, &out);
        RustResultGuard g{r};
        ASSERT_TRUE(r.success) << (r.error ? r.error : "?");
        std::vector<uint32_t> hits = array_to_vector(out);
        std::sort(hits.begin(), hits.end());
        EXPECT_EQ(hits, (std::vector<uint32_t>{0u, 2u}));
    }

    tb::tantivy_free_index_reader(reader);
}

TEST(TantivySmoke, NullPlaceholdersPreserveAlignment) {
    const std::string index_path = make_tempdir("tantivy_smoke_nulls");
    ASSERT_FALSE(index_path.empty());
    struct PathCleanup {
        std::string p;
        ~PathCleanup() { std::error_code ec; std::filesystem::remove_all(p, ec); }
    } cleanup{index_path};

    const std::string field = "f";
    // Row 0 = "alpha", row 1 = NULL, row 2 = "alpha", row 3 = NULL.
    build_index(index_path, field, {"alpha", "", "alpha", ""});

    tb::RustResult lr = tb::tantivy_load_index_reader(index_path.c_str(), field.c_str(), "english");
    RustResultGuard g_lr{lr};
    ASSERT_TRUE(lr.success);
    void* reader = lr.value.ptr;

    tb::RustU32Array out{};
    RustU32ArrayGuard g_out{out};
    const char* term = "alpha";
    tb::RustResult r = tb::tantivy_term_query(
            reader, reinterpret_cast<const uint8_t*>(term), std::strlen(term), &out);
    RustResultGuard g{r};
    ASSERT_TRUE(r.success);
    std::vector<uint32_t> hits = array_to_vector(out);
    std::sort(hits.begin(), hits.end());
    EXPECT_EQ(hits, (std::vector<uint32_t>{0u, 2u}));

    tb::tantivy_free_index_reader(reader);
}

TEST(TantivySmoke, CreateWriterReportsError) {
    // /proc is read-only on Linux, so create_dir_all + index init should fail.
    tb::RustResult r = tb::tantivy_create_index_writer("/proc/sr_tantivy_should_fail", "title",
                                                       "english");
    RustResultGuard g{r};
    EXPECT_FALSE(r.success);
    EXPECT_NE(r.error, nullptr);
    if (r.error != nullptr) {
        EXPECT_NE(r.error[0], '\0');
    }
}

TEST(TantivySmoke, UnsupportedTokenizerReportsError) {
    const std::string index_path = make_tempdir("tantivy_smoke_tok");
    ASSERT_FALSE(index_path.empty());
    struct PathCleanup {
        std::string p;
        ~PathCleanup() { std::error_code ec; std::filesystem::remove_all(p, ec); }
    } cleanup{index_path};

    tb::RustResult r =
            tb::tantivy_create_index_writer(index_path.c_str(), "f", "definitely_not_a_tokenizer");
    RustResultGuard g{r};
    EXPECT_FALSE(r.success);
    ASSERT_NE(r.error, nullptr);
    EXPECT_NE(std::string(r.error).find("unsupported tokenizer"), std::string::npos)
            << "got: " << r.error;
}

} // namespace starrocks
