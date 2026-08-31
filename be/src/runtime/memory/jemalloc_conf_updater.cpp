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

#include <sys/types.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <vector>

#include "common/configbase.h"
#include "common/logging.h"
#include "common/prof/heap_prof.h"
#include "fmt/format.h"
#include "gutil/strings/join.h"
#include "jemalloc/jemalloc.h"
#include "util/trim.h"

namespace starrocks {

namespace {

// The variable jemalloc derives as JEMALLOC_CPREFIX "MALLOC_CONF". The prefix is "JE" because
// thirdparty builds jemalloc with `--with-jemalloc-prefix=je`, but that macro lives in jemalloc's
// internal headers, so the name is spelled out here the way bin/start_backend.sh spells it out.
const char* const kJemallocConfEnv = "JEMALLOC_CONF";
const char* const kJemallocConfName = "jemalloc_conf";

const char* const kDirtyDecayMs = "dirty_decay_ms";
const char* const kMuzzyDecayMs = "muzzy_decay_ms";
const char* const kProfActive = "prof_active";

StatusOr<ssize_t> parse_decay_ms(const std::string& option, const std::string& value) {
    errno = 0;
    char* end = nullptr;
    long long parsed = std::strtoll(value.c_str(), &end, 10);
    if (value.empty() || end != value.c_str() + value.size() || errno != 0 || parsed < -1) {
        return Status::InvalidArgument(
                fmt::format("invalid value of jemalloc option '{}': '{}', expect an integer >= -1", option, value));
    }
    return static_cast<ssize_t>(parsed);
}

StatusOr<bool> parse_bool_option(const std::string& option, const std::string& value) {
    if (value == "true") {
        return true;
    }
    if (value == "false") {
        return false;
    }
    return Status::InvalidArgument(
            fmt::format("invalid value of jemalloc option '{}': '{}', expect 'true' or 'false'", option, value));
}

Status mallctl_failed(const std::string& name, int err) {
    return Status::InternalError(fmt::format("mallctl('{}') failed: {}", name, std::strerror(err)));
}

// jemalloc lays the arena indexes out as three groups:
//
//   [0, narenas_auto)  the automatic arenas, which serve the ordinary allocations
//   narenas_auto       the "huge" arena, present when `oversize_threshold` is in effect
//   > narenas_auto     the arenas created through `arenas.create`
//
// Only the first group belongs to this config. The huge arena in particular must be left
// alone: arena_choose_huge() deliberately puts it on eager purge (decay 0) whenever the
// default is positive, because huge allocations are few and rarely reused, so writing the
// configured decay time into it would both defeat that and spawn a background thread that
// arena_new_create_background_thread() purposely skips for it. Future arenas are unaffected
// either way, since the huge arena re-applies its own policy when it is created.
//
// `narenas_auto` has no mallctl node of its own, but malloc_init_narenas() derives it from
// `opt.narenas`, which does, by the two steps mirrored below. Note that the clamp does not
// write back into `opt_narenas`, so reading `opt.narenas` alone is not enough.
StatusOr<unsigned> auto_arena_count() {
    // MALLOCX_ARENA_LIMIT lives in jemalloc's internal headers, but its value is pinned by
    // the public MALLOCX_ARENA(a) == ((a) + 1) << 20 macro, which encodes an arena index in
    // the 12 flag bits starting at bit 20.
    constexpr unsigned kMallocxArenaLimit = (1u << 12) - 1;

    unsigned opt_narenas = 0;
    size_t size = sizeof(opt_narenas);
    if (int err = je_mallctl("opt.narenas", &opt_narenas, &size, nullptr, 0); err != 0) {
        return mallctl_failed("opt.narenas", err);
    }
    return std::min(opt_narenas, kMallocxArenaLimit - 1);
}

// Sets `arenas.<dirty|muzzy>_decay_ms`, which only takes effect for the arenas
// created afterwards, and then walks the existing automatic arenas.
//
// Note that `arena.<i>.*_decay_ms` does not accept MALLCTL_ARENAS_ALL: unlike
// `arena.<i>.purge`, its ctl handler resolves the index through
// arena_get(ind, false) and returns EFAULT for the pseudo index. So the arenas
// have to be walked one by one.
//
// The walk stops at `narenas_auto` rather than at `arenas.narenas`, see
// auto_arena_count() for why.
Status apply_decay_ms(const std::string& option, bool dirty, ssize_t decay_ms) {
    const std::string default_name = dirty ? "arenas.dirty_decay_ms" : "arenas.muzzy_decay_ms";
    if (int err = je_mallctl(default_name.c_str(), nullptr, nullptr, &decay_ms, sizeof(decay_ms)); err != 0) {
        return mallctl_failed(default_name, err);
    }

    // `arenas.narenas` reports the arena count cached by the ctl layer, which is
    // only refreshed when the epoch advances.
    uint64_t epoch = 1;
    size_t epoch_size = sizeof(epoch);
    (void)je_mallctl("epoch", &epoch, &epoch_size, &epoch, epoch_size);

    unsigned narenas = 0;
    size_t narenas_size = sizeof(narenas);
    if (int err = je_mallctl("arenas.narenas", &narenas, &narenas_size, nullptr, 0); err != 0) {
        return mallctl_failed("arenas.narenas", err);
    }

    // `arenas.narenas` counts every arena, including the huge one and the manually created
    // ones that must keep their own decay policy.
    ASSIGN_OR_RETURN(unsigned narenas_auto, auto_arena_count());
    narenas = std::min(narenas, narenas_auto);

    size_t updated = 0;
    size_t absent = 0;
    for (unsigned i = 0; i < narenas; ++i) {
        std::string name = fmt::format("arena.{}.{}_decay_ms", i, dirty ? "dirty" : "muzzy");
        int err = je_mallctl(name.c_str(), nullptr, nullptr, &decay_ms, sizeof(decay_ms));
        if (err == EFAULT) {
            // The arena has not been created yet. Under `percpu_arena` arenas are
            // created lazily, and such an arena will pick up the default set above.
            ++absent;
            continue;
        }
        if (err != 0) {
            return mallctl_failed(name, err);
        }
        ++updated;
    }

    if (decay_ms == 0) {
        LOG(WARNING) << "set jemalloc " << option << " to 0, which purges every unused page of " << updated
                     << " automatic arenas synchronously in the current thread";
    } else {
        // With `background_thread:true` the calling thread does not purge, but the
        // decay backlog is restarted from scratch, so the background thread purges
        // the currently unused pages on its next run.
        LOG(INFO) << "set jemalloc " << option << " to " << decay_ms << " for " << updated << " automatic arenas, "
                  << absent << " not created yet";
    }
    return Status::OK();
}

Status apply_prof_active(bool active) {
#ifdef __APPLE__
    return Status::NotSupported("jemalloc option 'prof_active' cannot be changed on macOS");
#else
    // Heap profiling has to be armed at startup: `opt.prof` is read-only, so
    // `prof.active` is meaningless when the process was started with `prof:false`.
    bool prof_enabled = false;
    size_t size = sizeof(prof_enabled);
    if (je_mallctl("opt.prof", &prof_enabled, &size, nullptr, 0) != 0 || !prof_enabled) {
        return Status::NotSupported(
                "cannot change jemalloc option 'prof_active' because the process was not started with 'prof:true', "
                "restart the BE with 'prof:true' in jemalloc_conf first");
    }

    if (active) {
        HeapProf::getInstance().enable_prof();
    } else {
        HeapProf::getInstance().disable_prof();
    }
    if (HeapProf::getInstance().has_enable() != active) {
        return Status::InternalError(fmt::format("failed to set jemalloc prof.active to {}", active));
    }
    return Status::OK();
#endif
}

} // namespace

StatusOr<JemallocOptions> parse_jemalloc_conf(std::string_view conf) {
    JemallocOptions options;
    for (size_t pos = 0; pos < conf.size();) {
        size_t comma = conf.find(',', pos);
        size_t len = comma == std::string_view::npos ? conf.size() - pos : comma - pos;
        std::string_view segment = trim_spaces(conf.substr(pos, len));
        pos = comma == std::string_view::npos ? conf.size() : comma + 1;
        if (segment.empty()) {
            continue;
        }
        size_t colon = segment.find(':');
        std::string_view name =
                colon == std::string_view::npos ? std::string_view() : trim_spaces(segment.substr(0, colon));
        if (name.empty()) {
            return Status::InvalidArgument(
                    fmt::format("invalid jemalloc option '{}', expect '<name>:<value>'", segment));
        }
        options[std::string(name)] = std::string(trim_spaces(segment.substr(colon + 1)));
    }
    return options;
}

JemallocConfUpdater& JemallocConfUpdater::instance() {
    static JemallocConfUpdater updater;
    return updater;
}

const std::set<std::string>& JemallocConfUpdater::mutable_options() {
    static const std::set<std::string> kMutableOptions{kDirtyDecayMs, kMuzzyDecayMs, kProfActive};
    return kMutableOptions;
}

std::string startup_jemalloc_conf(std::string_view config_value) {
    // Only an unset variable falls back. jemalloc treats a set but empty variable as a valid,
    // empty option set rather than a missing one, and falling back there would claim options
    // that were never applied.
    if (const char* env = std::getenv(kJemallocConfEnv); env != nullptr) {
        return env;
    }
    return std::string(config_value);
}

void JemallocConfUpdater::init(std::string_view config_value) {
    std::lock_guard guard(_mutex);

    std::string startup_conf = startup_jemalloc_conf(config_value);
    if (startup_conf != config_value) {
        // The config describes a set of options jemalloc never saw. Publish what is really in
        // effect instead, so that be_configs shows it and an update is diffed against the same
        // string the operator is looking at. set_config() is used rather than assigning the
        // field, to keep the rollback bookkeeping of ConfigUpdateRegistry intact, and the
        // update hook is deliberately not invoked: there is nothing to re-apply here.
        LOG(WARNING) << "jemalloc was started with '" << startup_conf << "' rather than the configured '"
                     << config_value << "', publishing the effective option string as " << kJemallocConfName;
        if (Status st = config::set_config(kJemallocConfName, startup_conf); !st.ok()) {
            LOG(WARNING) << "failed to publish the effective jemalloc option string: " << st;
        }
    }

    auto options = parse_jemalloc_conf(startup_conf);
    if (!options.ok()) {
        // Keep the baseline empty: every option of the new value then looks newly
        // added, so an immutable option can still never be changed silently.
        LOG(WARNING) << "failed to parse the jemalloc_conf the process was started with '" << startup_conf
                     << "': " << options.status();
        _applied.clear();
        return;
    }
    _applied = std::move(options).value();
}

JemallocOptions JemallocConfUpdater::applied_options() {
    std::lock_guard guard(_mutex);
    return _applied;
}

void JemallocConfUpdater::refresh_prof_active(JemallocOptions* options) {
#ifndef __APPLE__
    auto it = options->find(kProfActive);
    if (it == options->end()) {
        return;
    }
    bool prof_enabled = false;
    size_t size = sizeof(prof_enabled);
    if (je_mallctl("opt.prof", &prof_enabled, &size, nullptr, 0) != 0 || !prof_enabled) {
        return;
    }
    std::string live = HeapProf::getInstance().has_enable() ? "true" : "false";
    if (it->second != live) {
        LOG(INFO) << "jemalloc prof.active was changed outside of jemalloc_conf, move the baseline of 'prof_active' "
                  << "from " << it->second << " to " << live;
        it->second = std::move(live);
    }
#endif
}

Status JemallocConfUpdater::update(std::string_view new_conf) {
    ASSIGN_OR_RETURN(JemallocOptions new_options, parse_jemalloc_conf(new_conf));

    std::lock_guard guard(_mutex);
    refresh_prof_active(&_applied);

    std::vector<std::string> rejected;
    JemallocOptions changed;
    for (const auto& [name, value] : new_options) {
        auto it = _applied.find(name);
        if (it != _applied.end() && it->second == value) {
            continue;
        }
        if (mutable_options().count(name) == 0) {
            rejected.emplace_back(it == _applied.end() ? fmt::format("{} (added)", name) : name);
        } else {
            changed.emplace(name, value);
        }
    }
    for (const auto& entry : _applied) {
        if (new_options.count(entry.first) == 0) {
            // Dropping an option would mean guessing the value to restore, so the
            // set of options itself has to stay stable.
            rejected.emplace_back(fmt::format("{} (removed)", entry.first));
        }
    }
    if (!rejected.empty()) {
        std::vector<std::string> mutables(mutable_options().begin(), mutable_options().end());
        return Status::NotSupported(fmt::format(
                "these jemalloc options cannot be changed at runtime: {}. only {} can, changing anything else "
                "requires restarting the BE",
                JoinStrings(rejected, ", "), JoinStrings(mutables, ", ")));
    }
    if (changed.empty()) {
        return Status::OK();
    }

    // Parse every changed value before touching jemalloc, so that an invalid value
    // is rejected without leaving the other options half applied.
    std::optional<ssize_t> dirty_decay_ms;
    std::optional<ssize_t> muzzy_decay_ms;
    std::optional<bool> prof_active;
    for (const auto& [name, value] : changed) {
        if (name == kDirtyDecayMs) {
            ASSIGN_OR_RETURN(dirty_decay_ms, parse_decay_ms(name, value));
        } else if (name == kMuzzyDecayMs) {
            ASSIGN_OR_RETURN(muzzy_decay_ms, parse_decay_ms(name, value));
        } else if (name == kProfActive) {
            ASSIGN_OR_RETURN(prof_active, parse_bool_option(name, value));
        } else {
            return Status::InternalError(fmt::format("jemalloc option '{}' has no runtime applier", name));
        }
    }

    // Apply the options one by one and record each one that landed, because the config value
    // is rolled back on failure and the baseline should follow jemalloc rather than the rolled
    // back string. This is per option, not per arena: apply_decay_ms() writes the default for
    // future arenas before it walks the existing ones, so a failure in the middle of that walk
    // still leaves the option half applied with the baseline claiming the old value. Only a
    // wrong newlen makes that ctl fail, which cannot happen here, so the gap is left unclosed.
    if (dirty_decay_ms.has_value()) {
        RETURN_IF_ERROR(apply_decay_ms(kDirtyDecayMs, true, *dirty_decay_ms));
        _applied[kDirtyDecayMs] = changed.at(kDirtyDecayMs);
    }
    if (muzzy_decay_ms.has_value()) {
        RETURN_IF_ERROR(apply_decay_ms(kMuzzyDecayMs, false, *muzzy_decay_ms));
        _applied[kMuzzyDecayMs] = changed.at(kMuzzyDecayMs);
    }
    if (prof_active.has_value()) {
        RETURN_IF_ERROR(apply_prof_active(*prof_active));
        _applied[kProfActive] = changed.at(kProfActive);
    }
    _applied = std::move(new_options);
    return Status::OK();
}

} // namespace starrocks
