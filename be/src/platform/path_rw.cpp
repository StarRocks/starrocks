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

#include "platform/path_rw.h"

#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>

#include "common/logging.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"

namespace starrocks {

Status read_write_test_file(const std::string& test_file_path) {
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(test_file_path));

    if (fs->path_exists(test_file_path).ok()) {
        RETURN_IF_ERROR(fs->delete_file(test_file_path));
    }

    const size_t TEST_FILE_BUF_SIZE = 4096;
    const size_t DIRECT_IO_ALIGNMENT = 512;
    char* write_test_buff = nullptr;
    char* read_test_buff = nullptr;
    if (posix_memalign((void**)&write_test_buff, DIRECT_IO_ALIGNMENT, TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate write buffer memory. size=" << TEST_FILE_BUF_SIZE;
        return Status::Corruption("Fail to allocate write buffer memory");
    }
    std::unique_ptr<char, decltype(&std::free)> write_buff(write_test_buff, &std::free);
    if (posix_memalign((void**)&read_test_buff, DIRECT_IO_ALIGNMENT, TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate read buffer memory. size=" << TEST_FILE_BUF_SIZE;
        return Status::Corruption("Fail to allocate write buffer memory");
    }
    std::unique_ptr<char, decltype(&std::free)> read_buff(read_test_buff, &std::free);
    auto rand_seed = static_cast<uint32_t>(time(nullptr));
    for (size_t i = 0; i < TEST_FILE_BUF_SIZE; ++i) {
        int32_t tmp_value = rand_r(&rand_seed);
        write_test_buff[i] = static_cast<char>(tmp_value);
    }

    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(opts, test_file_path));
    RETURN_IF_ERROR(wf->append(Slice(write_buff.get(), TEST_FILE_BUF_SIZE)));
    RETURN_IF_ERROR(wf->close());

    ASSIGN_OR_RETURN(auto rf, fs->new_sequential_file(test_file_path));
    RETURN_IF_ERROR(rf->read_fully(read_buff.get(), TEST_FILE_BUF_SIZE));
    rf.reset();
    RETURN_IF_ERROR(fs->delete_file(test_file_path));

    if (memcmp(write_buff.get(), read_buff.get(), TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "the test file write_buf and read_buf not equal, [filename = " << test_file_path << "]";
        return Status::InternalError("test file write_buf and read_buf not equal");
    }
    return Status::OK();
}

bool check_datapath_rw(const std::string& path) {
    if (!fs::path_exist(path)) return false;
    std::string file_path = path + "/.read_write_test_file";
    try {
        Status res = read_write_test_file(file_path);
        return res.ok();
    } catch (...) {
        // do nothing
    }
    LOG(WARNING) << "error when try to read and write temp file under the data path and return "
                    "false. [path="
                 << path << "]";
    return false;
}

} // namespace starrocks
