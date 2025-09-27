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

#include "clucene_file_writer.h"

#include <utility>

#include "CLucene.h"
#include "runtime/exec_env.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/inverted/clucene/clucene_fs_directory.h"
#include "storage/storage_engine.h"
#include "storage/tablet_index.h"

namespace starrocks {

CLuceneFileWriter::CLuceneFileWriter(std::string index_path, std::shared_ptr<FileSystem> fs,
                                     const std::shared_ptr<WritableFileOptions>& opts)
        : _index_path(std::move(index_path)), _fs(std::move(fs)), _opts(opts), _local_fs(FileSystem::Default()) {
    _tmp_dir = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir();
    _index_to_dir.clear();
}

CLuceneFileWriter::~CLuceneFileWriter() {
    for (const auto& dir : _index_to_dir | std::views::values) {
        if (dir != nullptr) {
            if (const auto& d1 = std::dynamic_pointer_cast<StarRocksMergingDirectory>(dir)) {
                d1->deleteDirectory();
            }
        }
    }
}

StatusOr<std::shared_ptr<StarRocksMergingDirectory>> CLuceneFileWriter::open(const TabletIndex* index_meta) {
    const auto index_id = index_meta->index_id();
    if (_index_to_dir.contains(index_id)) {
        // NOTE: If a directory for this index already exists, what will happen if we return this existing directory?
        //       Maybe it will never happen.
        return Status::AlreadyExist(fmt::format("Temporary index directory for index {} already exists", index_id));
    }

    const auto& inverted_index_file_name = IndexDescriptor::get_inverted_index_file_name(_index_path);
    const auto& local_fs_index_path = IndexDescriptor::tmp_inverted_index_file_path(_tmp_dir, inverted_index_file_name);

    VLOG(10) << "Open temporary inverted index dir " << local_fs_index_path << " for index "
             << inverted_index_file_name;
    ASSIGN_OR_RETURN(auto dir, StarRocksFSDirectoryFactory::getDirectory(_local_fs, local_fs_index_path, _opts));
    _index_to_dir.emplace(index_id, std::move(dir));
    return dir;
}

Status CLuceneFileWriter::close() {
    if (_index_to_dir.empty()) {
        return Status::OK();
    }
    try {
        RETURN_IF_ERROR(write());
    } catch (CLuceneError& err) {
        return Status::InternalError(
                fmt::format("CLuceneError occur when close idx file {}, error msg: {}", _index_path, err.what()));
    }
    return Status::OK();
}

std::map<int64_t, std::vector<FileEntry>> CLuceneFileWriter::prepare_files() {
    std::map<int64_t, std::vector<FileEntry>> id_to_file_infos;
    for (const auto& [index_id, dir] : _index_to_dir) {
        std::vector<std::string> files;
        dir->list(&files);

        // Remove write.lock file
        std::erase(files, StarRocksMergingDirectory::WRITE_LOCK_FILE);

        std::vector<FileEntry> file_infos;
        file_infos.reserve(files.size());
        for (const auto& file : files) {
            file_infos.emplace_back(file, 0, dir->fileLength(file.c_str()));
        }
        id_to_file_infos.emplace(index_id, std::move(file_infos));
    }
    return id_to_file_infos;
}

int64_t CLuceneFileWriter::calculate_header_length(std::map<int64_t, std::vector<FileEntry>>& index_id_to_files) {
    int64_t header_length = sizeof(int32_t); // the number of indices
    for (const auto& files : index_id_to_files | std::views::values) {
        header_length += sizeof(int64_t); // index id
        header_length += sizeof(int32_t); // index file count
        for (const auto& file : files) {
            header_length += sizeof(int32_t);         // file name size
            header_length += file.file_name.length(); // file name
            header_length += sizeof(int64_t);         // file offset
            header_length += sizeof(int64_t);         // file size
        }
    }
    return header_length;
}

StatusOr<std::unique_ptr<lucene::store::IndexInput>> CLuceneFileWriter::open_input(
        const FileEntry& file, const std::shared_ptr<lucene::store::Directory>& directory) {
    CLuceneError err;
    lucene::store::IndexInput* tmp = nullptr;
    if (!directory->openInput(file.file_name.c_str(), tmp, err, BUFFER_SIZE)) {
        return Status::InternalError(
                fmt::format("CLuceneError occur when open index file {}, error msg: {}", file.file_name, err.what()));
    }
    std::unique_ptr<lucene::store::IndexInput> input(tmp);
    return std::move(input);
}

Status CLuceneFileWriter::copy_file(const FileEntry& file, const std::shared_ptr<lucene::store::Directory>& directory,
                                    const std::unique_ptr<lucene::store::IndexOutput>& output, uint8_t* buffer) {
    VLOG(10) << "copyFile " << file.file_name;

    ASSIGN_OR_RETURN(const auto input, open_input(file, directory));
    if (file.length != input->length()) {
        return Status::InternalError(
                fmt::format("File length {} does not match the original file length {}", input->length(), file.length));
    }

    const int64_t start_ptr = output->getFilePointer();
    int64_t remainder = file.length;

    while (remainder > 0) {
        const int64_t len = std::min({BUFFER_SIZE, file.length, remainder});
        input->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }

    if (remainder != 0) {
        return Status::InternalError(
                fmt::format("Non-zero remainder length {} after copying inverted index file({}, length: {}).",
                            remainder, file.file_name, file.length));
    }

    const int64_t end_ptr = output->getFilePointer();
    const int64_t diff = end_ptr - start_ptr;
    if (diff != file.length) {
        return Status::InternalError(
                fmt::format("Difference in the output file offsets {} does not match the original file length {}", diff,
                            file.length));
    }
    input->close();
    return Status::OK();
}

Status CLuceneFileWriter::copy_files(const std::map<int64_t, std::vector<FileEntry>>& id_to_files,
                                     const std::unique_ptr<lucene::store::IndexOutput>& output) {
    uint8_t buffer[BUFFER_SIZE];
    for (const auto& [index_id, files] : id_to_files) {
        if (!_index_to_dir.contains(index_id)) {
            return Status::InternalError(fmt::format("Index directory for index {} does not exist", index_id));
        }
        for (const auto& file : files) {
            RETURN_IF_ERROR(copy_file(file, _index_to_dir[index_id], output, buffer));
        }
    }
    return Status::OK();
}

Status CLuceneFileWriter::write_headers(int64_t offset, const std::unique_ptr<lucene::store::IndexOutput>& output,
                                        const std::map<int64_t, std::vector<FileEntry>>& files) {
    if (output == nullptr) {
        return Status::InternalError(fmt::format("Invalid output while writing inverted file for {}.", _index_path));
    }

    output->writeInt(files.size());
    for (const auto& [index_id, file_metas] : files) {
        output->writeLong(index_id);
        output->writeInt(file_metas.size());
        for (const auto& file_meta : file_metas) {
            output->writeInt(static_cast<int32_t>(file_meta.file_name.length()));
            output->writeBytes(reinterpret_cast<const uint8_t*>(file_meta.file_name.data()),
                               file_meta.file_name.length());
            output->writeLong(offset);
            output->writeLong(file_meta.length);
            offset += file_meta.length;
        }
    }
    return Status::OK();
}

Status CLuceneFileWriter::write() {
    try {
        auto files = prepare_files();
        const int64_t header_length = calculate_header_length(files);

        const auto& index_dir = IndexDescriptor::get_inverted_index_file_parent(_index_path);
        const auto& inverted_index_file_name = IndexDescriptor::get_inverted_index_file_name(_index_path);
        ASSIGN_OR_RETURN(auto out_dir, StarRocksFSDirectoryFactory::getDirectory(_fs.get(), index_dir, _opts));
        auto output =
                std::unique_ptr<lucene::store::IndexOutput>(out_dir->createOutput(inverted_index_file_name.c_str()));

        DeferOp op([&out_dir, &output] {
            if (output != nullptr) {
                output->close();
            }
            if (out_dir != nullptr) {
                out_dir->close();
            }
        });

        RETURN_IF_ERROR(write_headers(header_length, output, files));
        RETURN_IF_ERROR(copy_files(files, output));
    } catch (CLuceneError& err) {
        return Status::IOError(
                fmt::format("CLuceneError occur when write index file {}, error msg: {}", _index_path, err.what()));
    }
    return Status::OK();
}

} // namespace starrocks
