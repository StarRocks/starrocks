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
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include "base/concurrency/stopwatch.hpp"
#include "base/string/trim.h"
#include "common/config_memory_allocator_fwd.h"
#include "common/config_update_registry.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "common/thread/threadpool.h"
#include "fmt/format.h"
#include "gutil/strings/join.h"
#include "jemalloc/jemalloc.h"
#include "runtime/prof/heap_prof.h"

namespace starrocks {

namespace {

// The variable jemalloc derives as JEMALLOC_CPREFIX "MALLOC_CONF". The prefix is "JE" because
// thirdparty builds jemalloc with `--with-jemalloc-prefix=je`, but that macro lives in jemalloc's
// internal headers, so the name is spelled out here the way bin/start_backend.sh spells it out.
const char* const kJemallocConfEnv = "JEMALLOC_CONF";
const char* const kJemallocConfName = "jemalloc_conf";

// Bumped whenever a jemalloc_conf update writes a decay, so that a caller holding the arenas at
// a decay of its own can notice that it was overwritten. Not under the updater's mutex on the
// read side: a reader only needs to see that it changed, not to be ordered against the write.
std::atomic<uint64_t> g_decay_write_generation{0};

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

    // Deliberately no "epoch" refresh first. Writing to `epoch` runs ctl_refresh(), which
    // re-snapshots what the stats.* nodes report, and nothing here reads one.
    //
    // `arenas.narenas` in particular is not one of them. It does change over a process's life --
    // ctl_init() sets it and ctl_arena_init() increments it when an arena is created -- but both
    // write it directly, while ctl_refresh() only iterates the count it already holds. So a
    // refresh cannot make this read see an arena it would otherwise miss.
    //
    // `opt.narenas` is fixed at startup, and `arena.<i>.dirty_decay_ms` resolves its arena
    // through arena_get() -- which is also where its EFAULT for an arena that does not exist yet
    // comes from. A refresh would only walk every arena merging statistics under ctl_mtx.
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

// Exposed for the memory watchdog: under pressure it switches the arenas to returning every
// freed page immediately, and puts the configured decay back once the pressure is gone.
//
// Setting a decay of 0 purges that arena's backlog synchronously, so the cost of the walk is
// proportional to how much is dirty. With ~65GiB spread over 32 arenas a serial walk did not
// finish before the process was killed. The arenas have their own locks, so a pool turns that
// into a fraction of the time; a null pool asks for the serial walk back.
Status set_jemalloc_decay_ms(bool dirty, ssize_t decay_ms, ThreadPool* pool) {
    const char* option = dirty ? kDirtyDecayMs : kMuzzyDecayMs;
    const std::string default_name = dirty ? "arenas.dirty_decay_ms" : "arenas.muzzy_decay_ms";
    if (int err = je_mallctl(default_name.c_str(), nullptr, nullptr, &decay_ms, sizeof(decay_ms)); err != 0) {
        return mallctl_failed(default_name, err);
    }

    // No "epoch" refresh here either, for the reasons in apply_decay_ms().
    unsigned narenas = 0;
    size_t narenas_size = sizeof(narenas);
    if (int err = je_mallctl("arenas.narenas", &narenas, &narenas_size, nullptr, 0); err != 0) {
        return mallctl_failed("arenas.narenas", err);
    }
    ASSIGN_OR_RETURN(unsigned narenas_auto, auto_arena_count());
    narenas = std::min(narenas, narenas_auto);

    MonotonicStopWatch watch;
    watch.start();
    std::atomic<size_t> updated{0};
    std::atomic<size_t> absent{0};
    // The first errno that was neither success nor "not created yet", kept so the walk can
    // report a failure the way the serial path does instead of returning OK with an arena left
    // on its old decay.
    std::atomic<int> first_error{0};
    // Only the calling thread walks inline, so this needs no synchronization.
    size_t inline_walked = 0;
    bool logged_fallback = false;
    auto set_one = [&](unsigned i) {
        std::string name = fmt::format("arena.{}.{}_decay_ms", i, dirty ? "dirty" : "muzzy");
        const int err = je_mallctl(name.c_str(), nullptr, nullptr, &decay_ms, sizeof(decay_ms));
        if (err == 0) {
            updated.fetch_add(1);
            return;
        }
        if (err == EFAULT) {
            // The arena has not been created yet. Under `percpu_arena` arenas are created
            // lazily, and such an arena picks up the default written above.
            absent.fetch_add(1);
            return;
        }
        int none = 0;
        first_error.compare_exchange_strong(none, err);
        LOG(WARNING) << "failed to set " << name << " to " << decay_ms << ": " << std::strerror(err);
    };

    // One task per arena rather than one per thread: an arena's share of the work is whatever it
    // has dirty, which is wildly uneven -- a single arena has held 97% of it -- so letting the
    // pool hand out arenas as threads free up balances what a fixed split cannot.
    //
    // A submission that is refused runs inline instead. The pool can refuse because it is shut
    // down or because it could not start a thread, and the second is most likely exactly here:
    // this runs only when the machine is out of memory. Falling back to the calling thread keeps
    // a failure to parallelize from becoming a failure to reclaim.
    std::unique_ptr<ThreadPoolToken> token;
    if (pool != nullptr) {
        token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    }
    for (unsigned i = 0; i < narenas; i++) {
        if (token == nullptr) {
            set_one(i);
            inline_walked++;
            continue;
        }
        Status st = token->submit_func([&set_one, i]() { set_one(i); });
        if (!st.ok()) {
            if (!logged_fallback) {
                // Once per call: a pool that refuses one arena usually refuses the rest, and the
                // count of arenas walked inline is in the summary below anyway.
                LOG(WARNING) << "arena walk fell back to the calling thread at arena " << i << ": " << st;
                logged_fallback = true;
            }
            set_one(i);
            inline_walked++;
        }
    }
    if (token != nullptr) {
        token->wait();
    }

    // WARNING only for a decay of 0, matching apply_decay_ms(): that is the walk that purges
    // every arena's backlog in these threads, so the duration is both how long the last arena
    // went without backpressure and a stall worth seeing on its own. Every other value restarts
    // the decay backlog and returns in milliseconds, which is routine.
    const size_t failed = narenas - updated.load() - absent.load();
    LOG_AT_LEVEL(decay_ms == 0 ? google::GLOG_WARNING : google::GLOG_INFO)
            << "set jemalloc " << option << " to " << decay_ms << " on " << updated.load() << " of " << narenas
            << " arenas (" << absent.load() << " not created yet, " << failed << " failed, " << inline_walked
            << " inline), took " << watch.elapsed_time() / 1000 << "us";
    if (const int err = first_error.load(); err != 0) {
        return mallctl_failed(fmt::format("arena.<i>.{}", option), err);
    }
    return Status::OK();
}

// The decay the arenas are currently running with, so the watchdog can scale from it and put it
// back. Read from the options applied so far rather than from opt.dirty_decay_ms, which is a
// read-only snapshot of what the process started with: dirty_decay_ms is one of the three
// options a running BE may change, and a watchdog scaling from the startup value would tighten
// to something looser than what the arenas actually run with, then "restore" a value the
// operator had already replaced.
uint64_t decay_write_generation() {
    return g_decay_write_generation.load(std::memory_order_relaxed);
}

std::optional<ssize_t> configured_decay_ms(bool dirty) {
    const JemallocOptions applied = JemallocConfUpdater::instance().applied_options();
    const auto it = applied.find(dirty ? kDirtyDecayMs : kMuzzyDecayMs);
    if (it == applied.end()) {
        return std::nullopt;
    }
    StatusOr<ssize_t> parsed = parse_decay_ms(it->first, it->second);
    if (!parsed.ok()) {
        return std::nullopt;
    }
    return parsed.value();
}

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

std::string serialize_jemalloc_conf(const JemallocOptions& options) {
    std::vector<std::string> parts;
    parts.reserve(options.size());
    for (const auto& [name, value] : options) {
        parts.emplace_back(fmt::format("{}:{}", name, value));
    }
    return JoinStrings(parts, ",");
}

StatusOr<std::string> jemalloc_conf_with_prof_active(std::string_view conf, bool active) {
    ASSIGN_OR_RETURN(JemallocOptions options, parse_jemalloc_conf(conf));
    // The option is inserted when `conf` does not carry it. That is not a claim about how the
    // process started -- `prof_active` is one of the options jemalloc lets us change at runtime,
    // and jemalloc defaults it to false, so its absence means profiling is armed but idle, which
    // is exactly the state a caller wants to leave. --jemalloc_debug reaches it: it starts the BE
    // with `junk:true,tcache:false,prof:true`, no `prof_active` in sight. Whether profiling is
    // armed at all is apply_prof_active()'s call, which reads opt.prof instead of looking for a
    // string in the config.
    options[kProfActive] = active ? "true" : "false";
    return serialize_jemalloc_conf(options);
}

Status set_prof_active_via_config(bool active) {
    ASSIGN_OR_RETURN(std::string new_conf, jemalloc_conf_with_prof_active(config::jemalloc_conf.value(), active));
    // Going through the registry rather than applying directly keeps one code path: the hook
    // runs JemallocConfUpdater::update(), which is also what an operator editing
    // information_schema.be_configs reaches, and it rolls the config value back on failure.
    return ConfigUpdateRegistry::instance()->update_config(kJemallocConfName, new_conf);
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

Status JemallocConfUpdater::update(std::string_view new_conf) {
    ASSIGN_OR_RETURN(JemallocOptions new_options, parse_jemalloc_conf(new_conf));

    std::lock_guard guard(_mutex);

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
        g_decay_write_generation.fetch_add(1, std::memory_order_relaxed);
    }
    if (muzzy_decay_ms.has_value()) {
        RETURN_IF_ERROR(apply_decay_ms(kMuzzyDecayMs, false, *muzzy_decay_ms));
        _applied[kMuzzyDecayMs] = changed.at(kMuzzyDecayMs);
        g_decay_write_generation.fetch_add(1, std::memory_order_relaxed);
    }
    if (prof_active.has_value()) {
        RETURN_IF_ERROR(apply_prof_active(*prof_active));
        _applied[kProfActive] = changed.at(kProfActive);
    }
    _applied = std::move(new_options);
    return Status::OK();
}

} // namespace starrocks
