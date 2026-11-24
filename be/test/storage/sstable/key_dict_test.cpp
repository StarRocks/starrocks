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

#include "storage/sstable/key_dict.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/sstable/coding.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/format.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/merger.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table.h"
#include "storage/sstable/table_builder.h"

namespace starrocks::sstable {

namespace {

// The column encodings of PrimaryKeyEncoder for a composite key.
std::string enc_i64(int64_t v) {
    uint64_t u = static_cast<uint64_t>(v) ^ (1ULL << 63);
    std::string s;
    for (int i = 7; i >= 0; --i) s.push_back(static_cast<char>((u >> (i * 8)) & 0xff));
    return s;
}

std::string enc_i32(int32_t v) {
    uint32_t u = static_cast<uint32_t>(v) ^ (1U << 31);
    std::string s;
    for (int i = 3; i >= 0; --i) s.push_back(static_cast<char>((u >> (i * 8)) & 0xff));
    return s;
}

std::string enc_middle(const std::string& v) {
    std::string s;
    for (char c : v) {
        s.push_back(c);
        if (c == 0) s.push_back(1);
    }
    s.push_back(0);
    s.push_back(0);
    return s;
}

// PRIMARY KEY(id BIGINT, type VARCHAR, sub INT)
const KeyLayout kLayout = {{8}, {0}, {4}};

std::string make_key(int64_t id, const std::string& type, int32_t sub) {
    return enc_i64(id) + enc_middle(type) + enc_i32(sub);
}

const std::vector<std::string> kTypes = {"", std::string("a\0b", 3), "apple", "banana", "cherry", "zebra"};

std::shared_ptr<const KeyDict> make_dict() {
    // Some types are left out of the dictionary so that their keys need escapes.
    KeyDict::ColumnDict types{1, {enc_middle("apple"), enc_middle("cherry"), enc_middle(std::string("a\0b", 3))}};
    std::sort(types.values.begin(), types.values.end());
    KeyDict::ColumnDict subs{2, {enc_i32(-5), enc_i32(0), enc_i32(7)}};
    auto dict = KeyDict::create(kLayout, {types, subs});
    CHECK(dict.ok()) << dict.status();
    return dict.value();
}

std::vector<std::string> make_keys(size_t n, uint32_t seed) {
    std::mt19937 rng(seed);
    std::set<std::string> keys;
    // About ten keys per id.
    const int64_t num_ids = std::max<int64_t>(200, n / 10);
    while (keys.size() < n) {
        keys.insert(make_key(static_cast<int64_t>(rng() % num_ids) - num_ids / 2, kTypes[rng() % kTypes.size()],
                             static_cast<int32_t>(rng() % 12) - 6));
    }
    return {keys.begin(), keys.end()};
}

// Targets for Seek(): full keys, keys that are not stored and keys cut at every length, which is what index
// separators and range bounds look like.
std::vector<std::string> make_targets(const std::vector<std::string>& keys, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<std::string> targets = {""};
    for (size_t i = 0; i < 300; ++i) {
        const auto& key = keys[rng() % keys.size()];
        targets.push_back(key);
        targets.push_back(key.substr(0, rng() % (key.size() + 1)));
        targets.push_back(make_key(static_cast<int64_t>(rng() % 220) - 110, std::string(1, 'a' + rng() % 26),
                                   static_cast<int32_t>(rng() % 14) - 7));
    }
    targets.push_back(std::string(40, '\xff'));
    return targets;
}

std::string lower_bound_of(const std::vector<std::string>& sorted, const std::string& target) {
    auto it = std::lower_bound(sorted.begin(), sorted.end(), target);
    return it == sorted.end() ? "<end>" : *it;
}

} // namespace

TEST(KeyDictTest, test_round_trip_and_order) {
    auto dict = make_dict();
    auto keys = make_keys(2000, 1);
    std::vector<std::string> stored;
    std::string decoded;
    for (const auto& key : keys) {
        stored.emplace_back();
        dict->encode(Slice(key), &stored.back());
        ASSERT_OK(dict->decode(Slice(stored.back()), &decoded));
        ASSERT_EQ(key, decoded);
    }
    // |keys| is sorted, so the stored keys must be strictly ascending too.
    for (size_t i = 1; i < stored.size(); ++i) {
        ASSERT_LT(stored[i - 1], stored[i]) << i;
    }
    // A value in the dictionary takes a 1-byte code, one that is not takes an escape plus its bytes.
    std::string out;
    dict->encode(Slice(make_key(1, "apple", 7)), &out);
    ASSERT_EQ(8 + 1 + 1, out.size());
    dict->encode(Slice(make_key(1, "banana", 3)), &out);
    ASSERT_EQ(8 + 1 + enc_middle("banana").size() + 1 + 4, out.size());
}

TEST(KeyDictTest, test_encode_is_a_lower_bound_for_any_target) {
    auto dict = make_dict();
    auto keys = make_keys(1000, 2);
    std::vector<std::string> stored;
    for (const auto& key : keys) {
        stored.emplace_back();
        dict->encode(Slice(key), &stored.back());
    }
    std::string target_stored;
    std::string decoded;
    for (const auto& target : make_targets(keys, 3)) {
        dict->encode(Slice(target), &target_stored);
        std::string expected = lower_bound_of(keys, target);
        auto it = std::lower_bound(stored.begin(), stored.end(), target_stored);
        std::string actual = "<end>";
        if (it != stored.end()) {
            ASSERT_OK(dict->decode(Slice(*it), &decoded));
            actual = decoded;
        }
        ASSERT_EQ(expected, actual) << Slice(target).to_string();
    }
}

TEST(KeyDictTest, test_values_sharing_long_prefixes) {
    // Values that agree on their first 8 bytes, are prefixes of one another or hold zero bytes, so that a lookup
    // has to fall back from comparing the integer prefixes to comparing the bytes.
    const std::vector<std::string> raw = {"abcdefgh",
                                          "abcdefgh1",
                                          "abcdefgh2",
                                          "abcdefghij",
                                          "ab",
                                          std::string("ab\0", 3),
                                          std::string("ab\0\0", 4),
                                          "abcdefgg",
                                          "abcdefgi",
                                          "",
                                          std::string("abcdefgh\0", 9)};
    std::vector<std::string> values;
    for (size_t i = 0; i < raw.size(); i += 2) {
        values.push_back(enc_middle(raw[i]));
    }
    std::sort(values.begin(), values.end());
    ASSIGN_OR_ABORT(auto dict, KeyDict::create(kLayout, {{1, values}}));
    std::vector<std::string> keys;
    for (int64_t id = -2; id < 2; ++id) {
        for (const auto& value : raw) {
            for (int32_t sub : {-1, 0, 1}) {
                keys.push_back(make_key(id, value, sub));
            }
        }
    }
    std::sort(keys.begin(), keys.end());
    std::vector<std::string> stored;
    std::string decoded;
    for (const auto& key : keys) {
        stored.emplace_back();
        dict->encode(Slice(key), &stored.back());
        ASSERT_OK(dict->decode(Slice(stored.back()), &decoded));
        ASSERT_EQ(key, decoded);
        if (stored.size() > 1) ASSERT_LT(stored[stored.size() - 2], stored.back());
    }
    // Every cut of every key, so the lookup also meets partial values.
    std::string target_stored;
    for (const auto& key : keys) {
        for (size_t len = 0; len <= key.size(); ++len) {
            const std::string target = key.substr(0, len);
            dict->encode(Slice(target), &target_stored);
            auto it = std::lower_bound(stored.begin(), stored.end(), target_stored);
            std::string actual = "<end>";
            if (it != stored.end()) {
                ASSERT_OK(dict->decode(Slice(*it), &decoded));
                actual = decoded;
            }
            ASSERT_EQ(lower_bound_of(keys, target), actual) << len;
        }
    }
}

TEST(KeyDictTest, test_serialize) {
    auto dict = make_dict();
    std::string data;
    dict->serialize(&data);
    ASSIGN_OR_ABORT(auto loaded, KeyDict::deserialize(Slice(data)));
    ASSERT_EQ(dict->layout().size(), loaded->layout().size());
    ASSERT_EQ(dict->columns().size(), loaded->columns().size());
    for (size_t i = 0; i < dict->columns().size(); ++i) {
        ASSERT_EQ(dict->columns()[i].column, loaded->columns()[i].column);
        ASSERT_EQ(dict->columns()[i].values, loaded->columns()[i].values);
    }
    ASSERT_GT(loaded->memory_usage(), 0);

    std::string bad_version = data;
    bad_version[0] = 9;
    ASSERT_TRUE(KeyDict::deserialize(Slice(bad_version)).status().is_corruption());
    for (size_t len = 0; len < data.size(); ++len) {
        ASSERT_FALSE(KeyDict::deserialize(Slice(data.data(), len)).ok()) << len;
    }
    ASSERT_FALSE(KeyDict::deserialize(Slice(data + "x")).ok());
}

TEST(KeyDictTest, test_create_rejects_bad_dictionaries) {
    // Unsorted values.
    ASSERT_FALSE(KeyDict::create(kLayout, {{1, {enc_middle("b"), enc_middle("a")}}}).ok());
    // A value that is not a whole column: no terminator, or the wrong width.
    ASSERT_FALSE(KeyDict::create(kLayout, {{1, {"abc"}}}).ok());
    ASSERT_FALSE(KeyDict::create(kLayout, {{2, {std::string("abc")}}}).ok());
    // Columns out of order or out of the layout.
    ASSERT_FALSE(KeyDict::create(kLayout, {{2, {enc_i32(1)}}, {1, {enc_middle("a")}}}).ok());
    ASSERT_FALSE(KeyDict::create(kLayout, {{3, {enc_i32(1)}}}).ok());
    ASSERT_FALSE(KeyDict::create(kLayout, {}).ok());
}

TEST(KeyDictTest, test_decode_rejects_bad_codes) {
    auto dict = make_dict();
    std::string decoded;
    // types has 3 values: odd codes up to 5 and escapes up to 6 are valid.
    ASSERT_TRUE(dict->decode(Slice(enc_i64(1) + std::string(1, 7)), &decoded).is_corruption());
    ASSERT_TRUE(dict->decode(Slice(enc_i64(1) + std::string(1, 8)), &decoded).is_corruption());
    ASSERT_TRUE(dict->decode(Slice(enc_i64(1) + std::string(1, 1) + std::string(1, 9)), &decoded).is_corruption());
}

TEST(KeyDictTest, test_two_byte_codes) {
    std::vector<std::string> values;
    for (int i = 0; i < 1000; ++i) {
        values.push_back(enc_middle(fmt::format("type_{:04d}", i)));
    }
    ASSIGN_OR_ABORT(auto dict, KeyDict::create(kLayout, {{1, values}}));
    std::string out;
    std::string decoded;
    std::string prev;
    for (int i = 0; i < 1000; ++i) {
        auto key = make_key(42, fmt::format("type_{:04d}", i), 1);
        dict->encode(Slice(key), &out);
        ASSERT_EQ(8 + 2 + 4, out.size());
        ASSERT_LT(prev, out);
        prev = out;
        ASSERT_OK(dict->decode(Slice(out), &decoded));
        ASSERT_EQ(key, decoded);
    }
}

TEST(KeyDictTest, test_collector_chooses_columns) {
    // PRIMARY KEY(id BIGINT, type VARCHAR, sub INT, flag TINYINT, tail VARCHAR)
    const KeyLayout layout = {{8}, {0}, {4}, {1}, {0}};
    std::vector<std::string> keys;
    for (int64_t id = 0; id < 1000; ++id) {
        for (int t = 0; t < 3; ++t) {
            keys.push_back(enc_i64(id) + enc_middle(kTypes[2 + t]) + enc_i32(t) + std::string(1, 1) +
                           fmt::format("tail_{}", id * 3 + t));
        }
    }
    KeyDictCollector collector(layout, 256);
    for (const auto& key : keys) {
        collector.add(Slice(key));
    }
    ASSERT_FALSE(collector.exhausted());
    ASSIGN_OR_ABORT(auto dict, collector.finish(true));
    ASSERT_NE(nullptr, dict);
    // Not the first column, not the 1-byte column, not the high-cardinality tail.
    ASSERT_EQ(2, dict->columns().size());
    ASSERT_EQ(1, dict->columns()[0].column);
    ASSERT_EQ(3, dict->columns()[0].values.size());
    ASSERT_EQ(2, dict->columns()[1].column);
    ASSERT_EQ(std::vector<std::string>({enc_i32(0), enc_i32(1), enc_i32(2)}), dict->columns()[1].values);
}

TEST(KeyDictTest, test_collector_skips_columns_without_gain) {
    // A 2-byte column with more than 127 values would need 2-byte codes, which save nothing.
    const KeyLayout layout = {{8}, {2}};
    KeyDictCollector collector(layout, 1000);
    for (int64_t i = 0; i < 1000; ++i) {
        std::string key = enc_i64(i);
        key.push_back(0);
        key.push_back(static_cast<char>(i % 200));
        collector.add(Slice(key));
    }
    ASSIGN_OR_ABORT(auto dict, collector.finish(true));
    ASSERT_EQ(nullptr, dict);

    // Too few keys to pay for the dictionary.
    KeyDictCollector few(kLayout, 256);
    few.add(Slice(make_key(1, "a_long_type_name_that_is_used_once", 1)));
    ASSIGN_OR_ABORT(dict, few.finish(true));
    ASSERT_EQ(nullptr, dict);
}

TEST(KeyDictTest, test_collector_overflow) {
    KeyDictCollector collector({{8}, {0}}, 10);
    for (int64_t i = 0; i < 100; ++i) {
        collector.add(Slice(enc_i64(i) + fmt::format("v{}", i)));
    }
    ASSERT_TRUE(collector.exhausted());
    ASSIGN_OR_ABORT(auto dict, collector.finish(true));
    ASSERT_EQ(nullptr, dict);
}

TEST(KeyDictTest, test_collector_sample) {
    // PRIMARY KEY(dt INT, id BIGINT, type VARCHAR): in a sample from the start of the sorted keys `id` only goes
    // up and `type` cycles.
    const KeyLayout layout = {{4}, {8}, {0}};
    auto key = [](int32_t dt, int64_t id, const std::string& type) { return enc_i32(dt) + enc_i64(id) + type; };
    KeyDictCollector sampled(layout, 256);
    for (int64_t id = 0; id < 50; ++id) {
        for (const auto& type : {"click", "order", "view"}) {
            sampled.add(Slice(key(20260101, id, type)));
        }
    }
    ASSIGN_OR_ABORT(auto dict, sampled.finish(false));
    ASSERT_NE(nullptr, dict);
    ASSERT_EQ(1, dict->columns().size());
    ASSERT_EQ(2, dict->columns()[0].column);

    // When the keys are all the keys, `id` is low-cardinality indeed.
    KeyDictCollector complete(layout, 256);
    for (int64_t id = 0; id < 50; ++id) {
        for (const auto& type : {"click", "order", "view"}) {
            complete.add(Slice(key(20260101, id, type)));
        }
    }
    ASSIGN_OR_ABORT(dict, complete.finish(true));
    ASSERT_EQ(2, dict->columns().size());

    // A seed vouches for a column that only went up in the sample.
    ASSIGN_OR_ABORT(auto seed, KeyDict::create(layout, {{1, {enc_i64(0), enc_i64(1), enc_i64(2)}}}));
    KeyDictCollector seeded(layout, 256);
    seeded.add_seed(*seed);
    for (int64_t id = 0; id < 3; ++id) {
        for (int i = 0; i < 100; ++i) {
            seeded.add(Slice(key(20260101, id, fmt::format("t{:03d}", i))));
        }
    }
    ASSIGN_OR_ABORT(dict, seeded.finish(false));
    ASSERT_NE(nullptr, dict);
    ASSERT_EQ(2, dict->columns().size());
    ASSERT_EQ(1, dict->columns()[0].column);
    ASSERT_EQ(3, dict->columns()[0].values.size());

    // A seed with another layout is ignored.
    KeyDictCollector other(kLayout, 256);
    other.add_seed(*seed);
    other.add(Slice(make_key(1, "x", 1)));
    ASSIGN_OR_ABORT(dict, other.finish(false));
    ASSERT_EQ(nullptr, dict);
}

class KeyDictTableTest : public ::testing::Test {
public:
    static void SetUpTestCase() { CHECK_OK(fs::create_directories(kTestDir)); }
    static void TearDownTestCase() { (void)fs::remove_all(kTestDir); }

protected:
    struct Built {
        std::unique_ptr<RandomAccessFile> file;
        std::unique_ptr<Table> table;
        uint64_t file_size = 0;
    };

    // Builds a table of |keys| with |key_dict_options| and opens it.
    void build(const std::string& name, const std::vector<std::string>& keys,
               std::shared_ptr<const KeyDictOptions> key_dict_options, Built* built) {
        const std::string path = std::string(kTestDir) + "/" + name;
        ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(path));
        Options options;
        options.filter_policy = _filter_policy.get();
        options.key_dict_options = std::move(key_dict_options);
        TableBuilder builder(options, wf.get());
        for (size_t i = 0; i < keys.size(); ++i) {
            ASSERT_OK(builder.Add(Slice(keys[i]), Slice(fmt::format("value_{}", i))));
        }
        ASSERT_EQ(keys.size(), builder.NumEntries());
        ASSERT_OK(builder.Finish());
        ASSERT_EQ(keys.front(), builder.KeyRange().first.to_string());
        ASSERT_EQ(keys.back(), builder.KeyRange().second.to_string());
        ASSERT_OK(wf->close());
        built->file_size = builder.FileSize();
        ASSIGN_OR_ABORT(built->file, fs::new_random_access_file(path));
        Options read_options;
        read_options.filter_policy = _filter_policy.get();
        ASSERT_OK(Table::Open(read_options, built->file.get(), built->file_size, built->table));
    }

    // The table must behave as if it stored |keys| as they are.
    void verify(Table* table, const std::vector<std::string>& keys) {
        ReadOptions read_options;
        std::unique_ptr<Iterator> iter(table->NewIterator(read_options));
        size_t i = 0;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++i) {
            ASSERT_LT(i, keys.size());
            ASSERT_EQ(keys[i], iter->key().to_string());
            ASSERT_EQ(fmt::format("value_{}", i), iter->value().to_string());
        }
        ASSERT_OK(iter->status());
        ASSERT_EQ(keys.size(), i);

        for (const auto& target : make_targets(keys, 5)) {
            iter->Seek(Slice(target));
            ASSERT_OK(iter->status());
            ASSERT_EQ(lower_bound_of(keys, target), iter->Valid() ? iter->key().to_string() : "<end>");
        }

        // Present keys and absent ones, some of them with values the dictionary does not have.
        std::vector<std::string> lookups;
        std::set<size_t> indexes;
        std::mt19937 rng(7);
        for (size_t j = 0; j < 500; ++j) {
            lookups.push_back(keys[rng() % keys.size()]);
            lookups.push_back(make_key(static_cast<int64_t>(rng() % 220) - 110, std::string(1, 'a' + rng() % 26), 3));
        }
        std::sort(lookups.begin(), lookups.end());
        lookups.erase(std::unique(lookups.begin(), lookups.end()), lookups.end());
        std::vector<Slice> slices;
        for (size_t j = 0; j < lookups.size(); ++j) {
            slices.emplace_back(lookups[j]);
            indexes.insert(j);
        }
        std::vector<std::string> values(lookups.size());
        ASSERT_OK(table->MultiGet(read_options, slices.data(), indexes.begin(), indexes.end(), &values));
        for (size_t j = 0; j < lookups.size(); ++j) {
            auto it = std::lower_bound(keys.begin(), keys.end(), lookups[j]);
            if (it != keys.end() && *it == lookups[j]) {
                ASSERT_EQ(fmt::format("value_{}", it - keys.begin()), values[j]);
            } else {
                ASSERT_TRUE(values[j].empty()) << j;
            }
        }

        // Samples are in order. A table with a dictionary keeps whole keys in its index, so its samples are
        // real keys; a table without one samples shortened separators.
        const bool whole_keys = table->key_dict() != nullptr;
        std::vector<std::string> samples;
        ASSERT_OK(table->sample_keys(&samples, 4096));
        ASSERT_FALSE(samples.empty());
        for (size_t j = 0; j < samples.size(); ++j) {
            if (whole_keys) ASSERT_TRUE(std::binary_search(keys.begin(), keys.end(), samples[j]));
            if (j > 0) ASSERT_LT(samples[j - 1], samples[j]);
        }
        const std::string& seek_key = keys[keys.size() / 4];
        const std::string stop_key = keys[keys.size() * 3 / 4].substr(0, 9);
        std::vector<std::string> range_samples;
        ASSERT_OK(table->sample_keys_in_range(&range_samples, Slice(seek_key), Slice(stop_key), 10));
        ASSERT_FALSE(range_samples.empty());
        for (const auto& sample : range_samples) {
            if (whole_keys) ASSERT_TRUE(std::binary_search(keys.begin(), keys.end(), sample));
            ASSERT_GE(sample, seek_key);
            ASSERT_LT(sample, stop_key);
        }
    }

    constexpr static const char* const kTestDir = "./key_dict_table_test";
    std::unique_ptr<const FilterPolicy> _filter_policy{NewBloomFilterPolicy(10)};
};

TEST_F(KeyDictTableTest, test_sampled_dictionary) {
    auto keys = make_keys(20000, 11);
    auto options = std::make_shared<KeyDictOptions>();
    options->layout = kLayout;
    options->sample_entries = 1000;
    Built built;
    build("sampled.sst", keys, options, &built);
    const auto& dict = built.table->key_dict();
    ASSERT_NE(nullptr, dict);
    ASSERT_EQ(2, dict->columns().size());
    verify(built.table.get(), keys);

    Built plain;
    build("sampled_plain.sst", keys, nullptr, &plain);
    ASSERT_EQ(nullptr, plain.table->key_dict());
    ASSERT_LT(built.file_size, plain.file_size);
    ASSERT_LT(built.table->index_block_memory_usage(), plain.table->index_block_memory_usage() + dict->memory_usage());
    verify(plain.table.get(), keys);
}

TEST_F(KeyDictTableTest, test_exact_dictionary_when_smaller_than_sample) {
    auto keys = make_keys(8000, 12);
    auto options = std::make_shared<KeyDictOptions>();
    options->layout = kLayout;
    Built built;
    build("exact.sst", keys, options, &built);
    const auto& dict = built.table->key_dict();
    ASSERT_NE(nullptr, dict);
    // Every type of the table made it into the dictionary.
    ASSERT_EQ(kTypes.size(), dict->columns()[0].values.size());
    verify(built.table.get(), keys);
}

TEST_F(KeyDictTableTest, test_given_dictionary) {
    auto keys = make_keys(5000, 13);
    auto options = std::make_shared<KeyDictOptions>();
    options->layout = kLayout;
    options->dict = make_dict();
    Built built;
    build("given.sst", keys, options, &built);
    ASSERT_NE(nullptr, built.table->key_dict());
    ASSERT_EQ(3, built.table->key_dict()->columns()[0].values.size());
    verify(built.table.get(), keys);
}

TEST_F(KeyDictTableTest, test_no_dictionary_keeps_the_old_format) {
    // Every key has its own type: nothing to gain, so the table is written exactly as before.
    std::vector<std::string> keys;
    for (int64_t i = 0; i < 3000; ++i) {
        keys.push_back(make_key(i, fmt::format("type_{:06d}", i), 0));
    }
    auto options = std::make_shared<KeyDictOptions>();
    options->layout = kLayout;
    options->sample_entries = 100;
    Built built;
    build("no_dict.sst", keys, options, &built);
    ASSERT_EQ(nullptr, built.table->key_dict());
    Built plain;
    build("no_dict_plain.sst", keys, nullptr, &plain);
    ASSERT_EQ(plain.file_size, built.file_size);

    std::string footer(Footer::kEncodedLength, '\0');
    ASSERT_OK(built.file->read_at_fully(built.file_size - Footer::kEncodedLength, footer.data(), footer.size()));
    ASSERT_EQ(kTableMagicNumber, DecodeFixed64(footer.data() + footer.size() - 8));
}

TEST_F(KeyDictTableTest, test_dictionary_table_has_its_own_magic) {
    auto keys = make_keys(2000, 14);
    auto options = std::make_shared<KeyDictOptions>();
    options->layout = kLayout;
    Built built;
    build("magic.sst", keys, options, &built);
    ASSERT_NE(nullptr, built.table->key_dict());
    std::string footer(Footer::kEncodedLength, '\0');
    ASSERT_OK(built.file->read_at_fully(built.file_size - Footer::kEncodedLength, footer.data(), footer.size()));
    ASSERT_EQ(kKeyDictTableMagicNumber, DecodeFixed64(footer.data() + footer.size() - 8));

    // A table claiming a dictionary it does not have cannot be opened.
    Built plain;
    build("magic_plain.sst", keys, nullptr, &plain);
    std::string data(plain.file_size, '\0');
    ASSERT_OK(plain.file->read_at_fully(0, data.data(), data.size()));
    EncodeFixed64(data.data() + data.size() - 8, kKeyDictTableMagicNumber);
    const std::string path = std::string(kTestDir) + "/magic_forged.sst";
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(path));
    ASSERT_OK(wf->append(Slice(data)));
    ASSERT_OK(wf->close());
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    Options read_options;
    std::unique_ptr<Table> table;
    auto st = Table::Open(read_options, rf.get(), data.size(), table);
    ASSERT_TRUE(st.is_corruption()) << st;
    ASSERT_EQ(nullptr, table);
}

TEST_F(KeyDictTableTest, test_merge_tables_with_different_dictionaries) {
    // Two tables with their own dictionaries, and one without: merged, the keys come out decoded and in order.
    auto keys0 = make_keys(3000, 21);
    auto keys1 = make_keys(3000, 22);
    std::vector<std::string> keys2;
    for (int64_t i = 0; i < 1000; ++i) {
        keys2.push_back(make_key(i * 7 - 3500, "zebra", static_cast<int32_t>(i % 5)));
    }
    auto sampled = std::make_shared<KeyDictOptions>();
    sampled->layout = kLayout;
    sampled->sample_entries = 100;
    auto given = std::make_shared<KeyDictOptions>();
    given->layout = kLayout;
    given->dict = make_dict();
    Built t0, t1, t2;
    build("merge_0.sst", keys0, sampled, &t0);
    build("merge_1.sst", keys1, given, &t1);
    build("merge_2.sst", keys2, nullptr, &t2);
    ASSERT_NE(nullptr, t0.table->key_dict());
    ASSERT_NE(nullptr, t1.table->key_dict());

    std::set<std::string> expected(keys0.begin(), keys0.end());
    expected.insert(keys1.begin(), keys1.end());
    expected.insert(keys2.begin(), keys2.end());

    ReadOptions read_options;
    Iterator* children[] = {t0.table->NewIterator(read_options), t1.table->NewIterator(read_options),
                            t2.table->NewIterator(read_options)};
    std::unique_ptr<Iterator> merged(NewMergingIterator(BytewiseComparator(), children, 3));
    std::vector<std::string> out;
    for (merged->SeekToFirst(); merged->Valid(); merged->Next()) {
        if (out.empty() || out.back() != merged->key().to_string()) {
            out.push_back(merged->key().to_string());
        }
    }
    ASSERT_OK(merged->status());
    ASSERT_EQ(std::vector<std::string>(expected.begin(), expected.end()), out);

    // Seeded with the inputs' dictionaries, the output keeps their values even if its sample misses some.
    auto output_options = std::make_shared<KeyDictOptions>();
    output_options->layout = kLayout;
    output_options->sample_entries = 10;
    output_options->seeds = {t0.table->key_dict(), t1.table->key_dict(), nullptr};
    Built output;
    build("merge_out.sst", out, output_options, &output);
    ASSERT_NE(nullptr, output.table->key_dict());
    ASSERT_EQ(kTypes.size(), output.table->key_dict()->columns()[0].values.size());
    verify(output.table.get(), out);
}

} // namespace starrocks::sstable
