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

#include "runtime/memory/jemalloc_conf_updater.h"

#include <gtest/gtest.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdlib>
#include <string>

#include "common/config.h"
#include "common/configbase.h"
#include "fmt/format.h"
#include "jemalloc/jemalloc.h"
#include "testutil/assert.h"

namespace starrocks {

namespace {

// The same option set the BE is started with, so that the diff of a test only
// contains what the test itself changed.
std::string make_conf(std::string_view dirty_decay_ms, std::string_view muzzy_decay_ms, std::string_view prof_active) {
    return fmt::format(
            "percpu_arena:percpu,oversize_threshold:134217728,muzzy_decay_ms:{},dirty_decay_ms:{},metadata_thp:auto,"
            "background_thread:true,prof:true,prof_active:{}",
            muzzy_decay_ms, dirty_decay_ms, prof_active);
}

std::string startup_conf() {
    return make_conf("5000", "5000", "false");
}

constexpr const char* kJemallocConfEnv = "JEMALLOC_CONF";
constexpr const char* kJemallocConfName = "jemalloc_conf";

bool prof_enabled_at_startup() {
    bool enabled = false;
    size_t size = sizeof(enabled);
    return je_mallctl("opt.prof", &enabled, &size, nullptr, 0) == 0 && enabled;
}

ssize_t read_default_decay_ms(bool dirty) {
    ssize_t decay_ms = 0;
    size_t size = sizeof(decay_ms);
    EXPECT_EQ(0, je_mallctl(dirty ? "arenas.dirty_decay_ms" : "arenas.muzzy_decay_ms", &decay_ms, &size, nullptr, 0));
    return decay_ms;
}

} // namespace

class JemallocConfUpdaterTest : public testing::Test {
public:
    void SetUp() override {
        if (const char* env = std::getenv(kJemallocConfEnv); env != nullptr) {
            _saved_env = env;
            _env_was_set = true;
        }
        // init() prefers the environment, so clear it to make the seed of a test the string
        // the test passes in. The cases that exercise the environment set it themselves.
        ::unsetenv(kJemallocConfEnv);
        _saved_config = config::jemalloc_conf.value();
        _saved_dirty_decay_ms = read_default_decay_ms(true);
        _saved_muzzy_decay_ms = read_default_decay_ms(false);
        JemallocConfUpdater::instance().init(startup_conf());
    }

    // Push the saved values back through the updater, so that the arenas of the test
    // process are left with the decay times they had before the test.
    void TearDown() override {
        ::unsetenv(kJemallocConfEnv);
        JemallocConfUpdater::instance().init(startup_conf());
        EXPECT_OK(JemallocConfUpdater::instance().update(
                make_conf(std::to_string(_saved_dirty_decay_ms), std::to_string(_saved_muzzy_decay_ms), "false")));
        EXPECT_OK(config::set_config(kJemallocConfName, _saved_config));
        if (_env_was_set) {
            ::setenv(kJemallocConfEnv, _saved_env.c_str(), 1);
        }
    }

private:
    ssize_t _saved_dirty_decay_ms = 0;
    ssize_t _saved_muzzy_decay_ms = 0;
    std::string _saved_config;
    std::string _saved_env;
    bool _env_was_set = false;
};

TEST_F(JemallocConfUpdaterTest, parse_conf) {
    ASSIGN_OR_ABORT(auto options, parse_jemalloc_conf(startup_conf()));
    EXPECT_EQ(8, options.size());
    EXPECT_EQ("percpu", options["percpu_arena"]);
    EXPECT_EQ("5000", options["dirty_decay_ms"]);
    EXPECT_EQ("false", options["prof_active"]);

    // Surrounding spaces are ignored and empty segments are tolerated.
    ASSIGN_OR_ABORT(options, parse_jemalloc_conf("  dirty_decay_ms : 100 ,,prof_active:true,"));
    EXPECT_EQ(2, options.size());
    EXPECT_EQ("100", options["dirty_decay_ms"]);
    EXPECT_EQ("true", options["prof_active"]);

    // Only spaces are trimmed. jemalloc does not accept any other whitespace inside
    // JEMALLOC_CONF either, so a stray tab stays part of the name and is reported as an
    // unknown option instead of being silently accepted.
    ASSIGN_OR_ABORT(options, parse_jemalloc_conf("\tdirty_decay_ms:100"));
    EXPECT_EQ(1, options.size());
    EXPECT_EQ("100", options["\tdirty_decay_ms"]);

    // jemalloc itself lets the last assignment win.
    ASSIGN_OR_ABORT(options, parse_jemalloc_conf("dirty_decay_ms:1,dirty_decay_ms:2"));
    EXPECT_EQ(1, options.size());
    EXPECT_EQ("2", options["dirty_decay_ms"]);

    ASSIGN_OR_ABORT(options, parse_jemalloc_conf(""));
    EXPECT_TRUE(options.empty());

    EXPECT_TRUE(parse_jemalloc_conf("dirty_decay_ms").status().is_invalid_argument());
    EXPECT_TRUE(parse_jemalloc_conf("dirty_decay_ms:1,:2").status().is_invalid_argument());
}

TEST_F(JemallocConfUpdaterTest, startup_conf_prefers_the_environment) {
    ::setenv(kJemallocConfEnv, "dirty_decay_ms:1234", 1);
    EXPECT_EQ("dirty_decay_ms:1234", startup_jemalloc_conf("dirty_decay_ms:5000"));

    // A set but empty variable is an empty option set, not a missing one.
    ::setenv(kJemallocConfEnv, "", 1);
    EXPECT_EQ("", startup_jemalloc_conf("dirty_decay_ms:5000"));

    ::unsetenv(kJemallocConfEnv);
    EXPECT_EQ("dirty_decay_ms:5000", startup_jemalloc_conf("dirty_decay_ms:5000"));
}

TEST_F(JemallocConfUpdaterTest, init_leaves_the_config_alone_when_it_took_effect) {
    ::setenv(kJemallocConfEnv, startup_conf().c_str(), 1);
    ASSERT_OK(config::set_config(kJemallocConfName, startup_conf()));

    JemallocConfUpdater::instance().init(startup_conf());
    EXPECT_EQ(startup_conf(), config::jemalloc_conf.value());
}

// bin/start_backend.sh forces its own JEMALLOC_CONF under --jemalloc_debug and
// --check_mem_leak. The config then describes options jemalloc never saw, so init() has to
// publish the effective string; otherwise an operator is shown one option set and diffed
// against another.
TEST_F(JemallocConfUpdaterTest, init_publishes_the_effective_conf_when_the_environment_differs) {
    const std::string forced = "junk:true,tcache:false,prof:true";
    ::setenv(kJemallocConfEnv, forced.c_str(), 1);
    ASSERT_OK(config::set_config(kJemallocConfName, startup_conf()));

    JemallocConfUpdater::instance().init(startup_conf());
    EXPECT_EQ(forced, config::jemalloc_conf.value());

    // And an update on top of what be_configs now shows goes through, instead of being
    // rejected over options the operator can see nowhere.
    ASSERT_OK(JemallocConfUpdater::instance().update(forced + ",dirty_decay_ms:4000"));
    EXPECT_EQ(4000, read_default_decay_ms(true));
}

TEST_F(JemallocConfUpdaterTest, reject_immutable_option) {
    auto& updater = JemallocConfUpdater::instance();
    const auto before = updater.applied_options();

    // Changed.
    Status st = updater.update(
            "percpu_arena:disabled,oversize_threshold:134217728,muzzy_decay_ms:5000,dirty_decay_ms:5000,"
            "metadata_thp:auto,background_thread:true,prof:true,prof_active:false");
    EXPECT_TRUE(st.is_not_supported()) << st;
    EXPECT_TRUE(st.message().find("percpu_arena") != std::string::npos) << st;

    // Added.
    st = updater.update(startup_conf() + ",lg_prof_sample:0");
    EXPECT_TRUE(st.is_not_supported()) << st;
    EXPECT_TRUE(st.message().find("lg_prof_sample (added)") != std::string::npos) << st;

    // Removed.
    st = updater.update(
            "percpu_arena:percpu,oversize_threshold:134217728,muzzy_decay_ms:5000,dirty_decay_ms:5000,"
            "metadata_thp:auto,background_thread:true,prof:true");
    EXPECT_TRUE(st.is_not_supported()) << st;
    EXPECT_TRUE(st.message().find("prof_active (removed)") != std::string::npos) << st;

    // A mutable option changed together with an immutable one is rejected as well.
    st = updater.update(
            "percpu_arena:disabled,oversize_threshold:134217728,muzzy_decay_ms:5000,dirty_decay_ms:6000,"
            "metadata_thp:auto,background_thread:true,prof:true,prof_active:false");
    EXPECT_TRUE(st.is_not_supported()) << st;

    EXPECT_EQ(before, updater.applied_options());
}

TEST_F(JemallocConfUpdaterTest, reject_invalid_value) {
    auto& updater = JemallocConfUpdater::instance();
    const auto before = updater.applied_options();

    for (const auto& conf :
         {make_conf("abc", "5000", "false"), make_conf("-2", "5000", "false"), make_conf("5000", "5000", "yes")}) {
        Status st = updater.update(conf);
        EXPECT_TRUE(st.is_invalid_argument()) << st;
        // Nothing is applied when a single value is invalid.
        EXPECT_EQ(before, updater.applied_options());
    }
}

TEST_F(JemallocConfUpdaterTest, update_without_change_is_a_noop) {
    auto& updater = JemallocConfUpdater::instance();
    ASSERT_OK(updater.update(startup_conf()));
    EXPECT_EQ("5000", updater.applied_options()["dirty_decay_ms"]);

    // The option order does not matter either.
    ASSERT_OK(
            updater.update("prof_active:false,prof:true,background_thread:true,metadata_thp:auto,"
                           "dirty_decay_ms:5000,muzzy_decay_ms:5000,oversize_threshold:134217728,percpu_arena:percpu"));
    EXPECT_EQ("5000", updater.applied_options()["muzzy_decay_ms"]);
}

// The decay times must reach the automatic arenas only. jemalloc keeps its dedicated huge
// arena at index narenas_auto and puts it on eager purge on purpose, and the arenas created
// through `arenas.create` sit above that, so neither may be retuned from here.
//
// A manually created arena stands in for the huge one here: it sits on the same side of
// narenas_auto, so if the walk stopped at `arenas.narenas` it would reach this arena too.
// The test binary inherits no JEMALLOC_CONF, so whether a huge arena exists at all depends
// on jemalloc's own `oversize_threshold` default; the assertion below holds either way.
TEST_F(JemallocConfUpdaterTest, decay_ms_skips_the_arenas_above_narenas_auto) {
    unsigned manual_arena = 0;
    size_t size = sizeof(manual_arena);
    // Not destroyed afterwards: that needs `arena.<i>.reset` plus `arena.<i>.destroy` and the
    // arena is empty anyway. It cannot disturb the other cases, because the walk is now
    // bounded by narenas_auto rather than by the arena count this bumps.
    ASSERT_EQ(0, je_mallctl("arenas.create", &manual_arena, &size, nullptr, 0));

    // Mirrors auto_arena_count(): narenas_auto = min(opt.narenas, MALLOCX_ARENA_LIMIT - 1).
    // Without a huge arena the first manually created arena lands exactly on narenas_auto,
    // so the relation is >=, not >.
    unsigned opt_narenas = 0;
    size = sizeof(opt_narenas);
    ASSERT_EQ(0, je_mallctl("opt.narenas", &opt_narenas, &size, nullptr, 0));
    const unsigned narenas_auto = std::min(opt_narenas, (1u << 12) - 2);
    ASSERT_GE(manual_arena, narenas_auto) << "a manually created arena must sit at or above narenas_auto";

    const std::string name = fmt::format("arena.{}.dirty_decay_ms", manual_arena);
    ssize_t sentinel = 12345;
    ASSERT_EQ(0, je_mallctl(name.c_str(), nullptr, nullptr, &sentinel, sizeof(sentinel)));

    ASSERT_OK(JemallocConfUpdater::instance().update(make_conf("6000", "5000", "false")));

    // The automatic arenas took the new value...
    EXPECT_EQ(6000, read_default_decay_ms(true));
    // ... while the arena above narenas_auto kept its own.
    ssize_t after = 0;
    size = sizeof(after);
    ASSERT_EQ(0, je_mallctl(name.c_str(), &after, &size, nullptr, 0));
    EXPECT_EQ(sentinel, after);
}

TEST_F(JemallocConfUpdaterTest, apply_decay_ms) {
    auto& updater = JemallocConfUpdater::instance();
    ASSERT_OK(updater.update(make_conf("6000", "7000", "false")));

    EXPECT_EQ("6000", updater.applied_options()["dirty_decay_ms"]);
    EXPECT_EQ("7000", updater.applied_options()["muzzy_decay_ms"]);
    EXPECT_EQ(6000, read_default_decay_ms(true));
    EXPECT_EQ(7000, read_default_decay_ms(false));

    // -1 disables purging entirely and 0 purges eagerly; both are accepted.
    ASSERT_OK(updater.update(make_conf("-1", "0", "false")));
    EXPECT_EQ(-1, read_default_decay_ms(true));
    EXPECT_EQ(0, read_default_decay_ms(false));
}

TEST_F(JemallocConfUpdaterTest, apply_prof_active) {
    auto& updater = JemallocConfUpdater::instance();
    Status st = updater.update(make_conf("5000", "5000", "true"));
#ifdef __APPLE__
    EXPECT_TRUE(st.is_not_supported()) << st;
#else
    if (prof_enabled_at_startup()) {
        ASSERT_OK(st);
        EXPECT_EQ("true", updater.applied_options()["prof_active"]);
        ASSERT_OK(updater.update(startup_conf()));
        EXPECT_EQ("false", updater.applied_options()["prof_active"]);
    } else {
        // The test binary is normally not started with prof:true.
        EXPECT_TRUE(st.is_not_supported()) << st;
        EXPECT_TRUE(st.message().find("prof:true") != std::string::npos) << st;
    }
#endif
}

} // namespace starrocks
