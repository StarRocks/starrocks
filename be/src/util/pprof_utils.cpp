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

#include "util/pprof_utils.h"

#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream> // IWYU pragma: keep
#include <memory>

#include "agent/utils.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"

namespace starrocks {
namespace config {
extern std::string pprof_profile_dir;
}

Status PprofUtils::get_perf_cmd(std::string* cmd) {
    AgentUtils util;
    std::string perf_cmd = "perf";
    std::string msg;
    bool rc = util.exec_cmd(perf_cmd + " --version", &msg);
    if (!rc) {
        return Status::NotSupported("perf: command not found in system PATH");
    }
    *cmd = perf_cmd;
    return Status::OK();
}

Status PprofUtils::generate_flamegraph(int32_t sample_seconds,
                                       const std::string& flame_graph_tool_dir, bool return_file,
                                       std::string* svg_file_or_content) {
    std::string perf_cmd;
    RETURN_IF_ERROR(PprofUtils::get_perf_cmd(&perf_cmd));

    const std::string stackcollapse_perf_pl = flame_graph_tool_dir + "/stackcollapse-perf.pl";
    const std::string flamegraph_pl = flame_graph_tool_dir + "/flamegraph.pl";

    if (!FileSystem::Default()->path_exists(stackcollapse_perf_pl).ok() ||
        !FileSystem::Default()->path_exists(flamegraph_pl).ok()) {
        return Status::InternalError("Missing stackcollapse-perf.pl or flamegraph.pl in FlameGraph");
    }

    // Generate unique temp file names safely
    std::string tmp_file = fmt::format("{}/cpu_perf.{}.{}", config::pprof_profile_dir, getpid(), rand());

    std::string perf_record_cmd = fmt::format("{} record -m 2 -g -p {} -o {} -- sleep {}",
                                              perf_cmd, getpid(), tmp_file, sample_seconds);

    AgentUtils util;
    std::string cmd_output;
    LOG(INFO) << "Executing command: " << perf_record_cmd;
    if (!util.exec_cmd(perf_record_cmd, &cmd_output)) {
        static_cast<void>(FileSystem::Default()->delete_file(tmp_file));
        return Status::InternalError("Failed to execute perf command: " + cmd_output);
    }

    std::string res_content;
    if (return_file) {
        std::string graph_file = fmt::format("{}/flamegraph.{}.{}.svg", config::pprof_profile_dir, getpid(), rand());
        std::string gen_cmd = fmt::format("{} script -i {} | {} | {} > {}",
                                          perf_cmd, tmp_file, stackcollapse_perf_pl, flamegraph_pl, graph_file);
        LOG(INFO) << "Executing command: " << gen_cmd;
        if (!util.exec_cmd(gen_cmd, &res_content)) {
            static_cast<void>(FileSystem::Default()->delete_file(tmp_file));
            static_cast<void>(FileSystem::Default()->delete_file(graph_file));
            return Status::InternalError("Failed to execute perf script command: " + res_content);
        }
        *svg_file_or_content = graph_file;
    } else {
        std::string gen_cmd = fmt::format("{} script -i {} | {} | {}",
                                          perf_cmd, tmp_file, stackcollapse_perf_pl, flamegraph_pl);
        LOG(INFO) << "Executing command: " << gen_cmd;
        if (!util.exec_cmd(gen_cmd, &res_content, false)) {
            static_cast<void>(FileSystem::Default()->delete_file(tmp_file));
            return Status::InternalError("Failed to execute perf script command: " + res_content);
        }
        *svg_file_or_content = res_content;
    }

    // Cleanup temp file
    static_cast<void>(FileSystem::Default()->delete_file(tmp_file));
    return Status::OK();
}

} // namespace starrocks

