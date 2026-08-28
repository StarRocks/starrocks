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

#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <vector>

#include "common/logging.h"
#include "fmt/format.h"
#include "gutil/strings/join.h"
#include "jemalloc/jemalloc.h"
#include "runtime/prof/heap_prof.h"

namespace starrocks {

namespace {

const char* const kDirtyDecayMs = "dirty_decay_ms";
const char* const kMuzzyDecayMs = "muzzy_decay_ms";
const char* const kProfActive = "prof_active";

std::string_view trim(std::string_view str) {
    while (!str.empty() && std::isspace(static_cast<unsigned char>(str.front()))) {
        str.remove_prefix(1);
    }
    while (!str.empty() && std::isspace(static_cast<unsigned char>(str.back()))) {
        str.remove_suffix(1);
    }
    return str;
}

StatusOr<ssize_t> parse_decay_ms(const std::string& option, const std::string& value) {
    errno = 0;
    char* end = nullptr;
    long long parsed = std::strtoll(value.c_str(), &end, 10);
    if (value.empty() || end != value.c_str() + value.size() || errno != 0 || parsed < -1) {
        return Status::InvalidArgument(fmt::format(
                "invalid value of jemalloc option '{}': '{}', expect an integer >= -1", option, value));
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

// Sets `arenas.<dirty|muzzy>_decay_ms`, which only takes effect for the arenas
// created afterwards, and then walks the existing arenas.
//
// Note that `arena.<i>.*_decay_ms` does not accept MALLCTL_ARENAS_ALL: unlike
// `arena.<i>.purge`, its ctl handler resolves the index through
// arena_get(ind, false) and returns EFAULT for the pseudo index. So the arenas
// have to be walked one by one.
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
                     << " arenas synchronously in the current thread";
    } else {
        // With `background_thread:true` the calling thread does not purge, but the
        // decay backlog is restarted from scratch, so the background thread purges
        // the currently unused pages on its next run.
        LOG(INFO) << "set jemalloc " << option << " to " << decay_ms << " for " << updated << " arenas, " << absent
                  << " arenas not created yet";
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
        std::string_view segment = trim(conf.substr(pos, len));
        pos = comma == std::string_view::npos ? conf.size() : comma + 1;
        if (segment.empty()) {
            continue;
        }
        size_t colon = segment.find(':');
        if (colon == std::string_view::npos || trim(segment.substr(0, colon)).empty()) {
            return Status::InvalidArgument(
                    fmt::format("invalid jemalloc option '{}', expect '<name>:<value>'", segment));
        }
        options[std::string(trim(segment.substr(0, colon)))] = std::string(trim(segment.substr(colon + 1)));
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

void JemallocConfUpdater::init(std::string_view startup_conf) {
    std::lock_guard guard(_mutex);
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

    // Apply the options one by one and record what actually landed: the config value
    // is rolled back on failure, so the baseline must keep describing the real state
    // of jemalloc instead of the rolled back string.
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
