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

#include "util/memory_lock.h"

#include <link.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "util/starrocks_metrics.h"

static inline uintptr_t page_align_down(uintptr_t addr, size_t page) {
    return addr & ~(page - 1);
}
static inline uintptr_t page_align_up(uintptr_t addr, size_t page) {
    return (addr + page - 1) & ~(page - 1);
}

static void lock_segment(const char* module_name, void* start, void* end) {
    size_t page_size = sysconf(_SC_PAGESIZE);
    uintptr_t seg_start = page_align_down((uintptr_t)start, page_size);
    uintptr_t seg_end = page_align_up((uintptr_t)end, page_size);
    size_t seg_len = seg_end - seg_start;

    if (mlock((void*)seg_start, seg_len) != 0) {
        LOG(WARNING) << "mlock failed for " << module_name << " " << seg_start << "-" << seg_end << " (" << seg_len
                     << " bytes): " << strerror(errno);
    } else {
        VLOG_FILE << "mlock success for " << module_name << " " << seg_start << "-" << seg_end << " (" << seg_len
                  << " bytes)";
        starrocks::StarRocksMetrics::instance()->exec_runtime_memory_size.increment(seg_len);
    }
}

static int module_name_matches(const char* dlpi_name, const std::vector<std::string>& module_list) {
    if (module_list.empty()) return 0;

    VLOG_FILE << "find module name: " << dlpi_name;

    if (!dlpi_name || dlpi_name[0] == '\0') {
        for (const auto& module_name : module_list) {
            if (module_name == "main") return 1;
        }
        return 0;
    }
    for (const auto& module_name : module_list) {
        if (strstr(dlpi_name, module_name.c_str()) != nullptr) return 1;
    }

    return 0;
}

static int phdr_callback(struct dl_phdr_info* info, size_t size, void* data) {
    const std::vector<std::string>* module_list = (const std::vector<std::string>*)data;

    if (!module_name_matches(info->dlpi_name, *module_list)) return 0;

    for (int i = 0; i < info->dlpi_phnum; i++) {
        const ElfW(Phdr)* ph = &info->dlpi_phdr[i];
        if (ph->p_type == PT_LOAD && (ph->p_flags & PF_X)) {
            void* seg_start = (void*)(info->dlpi_addr + ph->p_vaddr);
            void* seg_end = (void*)(info->dlpi_addr + ph->p_vaddr + ph->p_memsz);
            lock_segment(info->dlpi_name, seg_start, seg_end);
        }
    }
    return 0;
}

namespace starrocks {
void mlock_modules(const std::vector<std::string>& module_list) {
    dl_iterate_phdr(phdr_callback, (void*)&module_list);
}

void mlock_modules() {
    std::vector<std::string> module_list = config::sys_mlock_modules;
    mlock_modules(module_list);
}

} // namespace starrocks