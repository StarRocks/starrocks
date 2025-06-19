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

#include <CLucene.h>

#include <utility>

#include "clucene_fs_directory.h"
#include "runtime/exec_env.h"
#include "storage/index/index_descriptor.h"
#include "storage/storage_engine.h"
#include "storage/tablet_index.h"

namespace starrocks {

CLuceneFileWriter::CLuceneFileWriter(std::shared_ptr<FileSystem> fs, std::string index_path,
                                     std::unique_ptr<WritableFile> file_writer, bool can_use_ram_dir)
        : _index_path(std::move(index_path)),
          _fs(std::move(fs)),
          _local_fs(FileSystem::Default()),
          _index_writer(std::move(file_writer)),
          _can_use_ram_dir(can_use_ram_dir) {
    _tmp_dir = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir();
}

CLuceneFileWriter::~CLuceneFileWriter() {
    for (const auto& dir : _indices_dirs | std::views::values) {
        if (dir != nullptr) {
            if (const auto& d1 = std::dynamic_pointer_cast<StarRocksFSDirectory>(dir)) {
                d1->deleteDirectory();
            }
        }
    }
}

Status CLuceneFileWriter::initialize(InvertedIndexDirectoryMap& indices_dirs) {
    _indices_dirs = std::move(indices_dirs);
    return Status::OK();
}

int64_t CLuceneFileWriter::get_index_file_total_size() const {
    DCHECK(_closed) << debug_string();
    return _total_file_size;
}

const FileSystem* CLuceneFileWriter::get_fs() const {
    return _fs.get();
}

void CLuceneFileWriter::set_file_writer_opts(const std::shared_ptr<WritableFileOptions>& opts) {
    _opts = opts;
}

Status CLuceneFileWriter::_insert_directory_into_map(int64_t index_id, std::shared_ptr<StarRocksFSDirectory> dir) {
    if (auto [it, inserted] = _indices_dirs.emplace(index_id, std::move(dir)); !inserted) {
        LOG(ERROR) << "CluceneFileWriter::open attempted to insert a duplicate key: " << index_id << ", "
                   << "Directories already in map: " << _indices_dirs.at(index_id)->toString();
        return Status::InternalError("CluceneFileWriter::open attempted to insert a duplicate dir");
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<StarRocksFSDirectory>> CLuceneFileWriter::open(const TabletIndex* index_meta) {
    const auto& inverted_index_file_name = IndexDescriptor::get_inverted_index_file_name(_index_path);
    const auto& local_fs_index_path = IndexDescriptor::tmp_inverted_index_file_path(_tmp_dir, inverted_index_file_name);
    VLOG(10) << "inverted_index_file_name is " << inverted_index_file_name << ", local_fs_index_path is "
             << local_fs_index_path;
    ASSIGN_OR_RETURN(auto dir,
                     StarRocksFSDirectoryFactory::getDirectory(_local_fs, local_fs_index_path, _can_use_ram_dir));
    RETURN_IF_ERROR(_insert_directory_into_map(index_meta->index_id(), dir));
    dir->set_file_writer_opts(_opts);
    return dir;
}

Status CLuceneFileWriter::delete_index(const TabletIndex* index_meta) {
    if (!index_meta) {
        return Status::InvalidArgument("Index metadata is null.");
    }

    // Check if the specified index exists
    const auto index_id = index_meta->index_id();
    const auto index_it = _indices_dirs.find(index_id);
    if (index_it == _indices_dirs.end()) {
        VLOG(1) << "No inverted index with id " << index_id << " found.";
        return Status::OK();
    }

    _indices_dirs.erase(index_it);
    return Status::OK();
}

int64_t CLuceneFileWriter::headerLength() {
    int64_t header_size = 0;
    header_size += sizeof(int32_t); // Account for the size of the number of indices

    for (const auto& dir : _indices_dirs | std::views::values) {
        header_size += sizeof(int64_t); // index id
        header_size += sizeof(int32_t); // index file count
        std::vector<std::string> files;
        dir->list(&files);

        for (const auto& file : files) {
            header_size += sizeof(int32_t); // file name size
            header_size += file.length();   // file name
            header_size += sizeof(int64_t); // file offset
            header_size += sizeof(int64_t); // file size
        }
    }
    return header_size;
}

Status CLuceneFileWriter::close() {
    DCHECK(!_closed) << debug_string();
    _closed = true;
    if (_indices_dirs.empty()) {
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

void CLuceneFileWriter::sort_files(std::vector<FileInfo>& file_infos) {
    auto file_priority = [](const std::string& filename) {
        for (const auto& [file, priority] : IndexDescriptor::index_file_info_map) {
            if (filename.find(file) != std::string::npos) {
                return priority;
            }
        }
        return std::numeric_limits<int32_t>::max(); // Other files
    };

    std::ranges::sort(file_infos, [&](const FileInfo& a, const FileInfo& b) {
        const int32_t& priority_a = file_priority(a.path);
        const int32_t& priority_b = file_priority(b.path);
        if (priority_a != priority_b) {
            return priority_a < priority_b;
        }
        return a.size < b.size;
    });
}

void CLuceneFileWriter::copy_file(const std::string& fileName, lucene::store::Directory* dir,
                                  lucene::store::IndexOutput* output, uint8_t* buffer, int64_t bufferLength) {
    VLOG(10) << "copyFile " << fileName;
    lucene::store::IndexInput* tmp = nullptr;
    CLuceneError err;
    if (!dir->openInput(fileName.c_str(), tmp, err, bufferLength)) {
        throw err;
    }

    std::unique_ptr<lucene::store::IndexInput> input(tmp);
    int64_t start_ptr = output->getFilePointer();
    int64_t length = input->length();
    int64_t remainder = length;
    int64_t chunk = bufferLength;

    while (remainder > 0) {
        int64_t len = std::min({chunk, length, remainder});
        input->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    if (remainder != 0) {
        std::ostringstream errMsg;
        errMsg << "Non-zero remainder length after copying: " << remainder << " (id: " << fileName
               << ", length: " << length << ", buffer size: " << chunk << ")";
        err.set(CL_ERR_IO, errMsg.str().c_str());
        throw err;
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    if (diff != length) {
        std::ostringstream errMsg;
        errMsg << "Difference in the output file offsets " << diff << " does not match the original file length "
               << length;
        err.set(CL_ERR_IO, errMsg.str().c_str());
        throw err;
    }
    input->close();
}

Status CLuceneFileWriter::write() {
    try {
        // Calculate header length and initialize offset
        int64_t current_offset = headerLength();
        // Prepare file metadata
        const auto& file_metadata = std::move(prepare_file_metadata(current_offset));

        // Create output stream
        ASSIGN_OR_RETURN(auto res, create_output_stream());
        auto out_dir = std::move(res).first;
        auto compound_file_output = std::move(res).second;
        DeferOp op([&out_dir, &compound_file_output] {
            if (compound_file_output != nullptr) {
                compound_file_output->close();
            }
            if (out_dir != nullptr) {
                out_dir->close();
            }
        });

        // Write version and number of indices
        write_indices_count(compound_file_output.get());

        // Write index headers and file metadata
        write_index_headers_and_metadata(compound_file_output.get(), file_metadata);

        // Copy file data
        copy_files_data(compound_file_output.get(), file_metadata);

        _total_file_size = compound_file_output->getFilePointer();
    } catch (CLuceneError& err) {
        return Status::IOError(
                fmt::format("CLuceneError occur when close idx file {}, error msg: {}", _index_path, err.what()));
    }
    return Status::OK();
}

// Helper function implementations
std::vector<FileInfo> CLuceneFileWriter::prepare_sorted_files(
        const std::shared_ptr<lucene::store::Directory>& directory) {
    std::vector<std::string> files;
    directory->list(&files);

    // Remove write.lock file
    std::erase(files, StarRocksFSDirectory::WRITE_LOCK_FILE);

    std::vector<FileInfo> sorted_files;
    for (const auto& file : files) {
        FileInfo file_info;
        file_info.path = file;
        file_info.size = directory->fileLength(file.c_str());
        sorted_files.push_back(std::move(file_info));
    }

    // Sort the files
    sort_files(sorted_files);
    return sorted_files;
}

StatusOr<std::pair<std::shared_ptr<lucene::store::Directory>, std::unique_ptr<lucene::store::IndexOutput>>>
CLuceneFileWriter::create_output_stream() const {
    const auto& index_dir = IndexDescriptor::get_inverted_index_file_parent(_index_path);
    VLOG(10) << "create output stream for " << index_dir;
    ASSIGN_OR_RETURN(auto out_dir, StarRocksFSDirectoryFactory::getDirectory(_fs.get(), index_dir));
    out_dir->set_file_writer_opts(_opts);

    DCHECK(_index_writer != nullptr) << "inverted index file writer is nullptr";
    auto compound_file_output =
            std::unique_ptr<lucene::store::IndexOutput>(out_dir->createOutputV2(_index_writer.get()));

    return std::make_pair(std::move(out_dir), std::move(compound_file_output));
}

void CLuceneFileWriter::write_indices_count(lucene::store::IndexOutput* output) const {
    VLOG(10) << "write indices count for " << _index_path;
    // Write the number of indices
    const auto num_indices = static_cast<uint32_t>(_indices_dirs.size());
    output->writeInt(num_indices);
}

std::vector<FileMetadata> CLuceneFileWriter::prepare_file_metadata(int64_t& current_offset) {
    std::vector<FileMetadata> file_metadata;
    std::vector<FileMetadata> meta_files;
    std::vector<FileMetadata> normal_files;
    for (const auto& [index_id, dir] : _indices_dirs) {
        VLOG(10) << "prepare sorted files for " << std::dynamic_pointer_cast<StarRocksFSDirectory>(dir)->getDirName();
        for (const auto& sorted_files = prepare_sorted_files(dir); const auto& file : sorted_files) {
            VLOG(10) << "check file " << file.path;
            bool is_meta = false;

            for (const auto& file_name : IndexDescriptor::index_file_info_map | std::views::keys) {
                if (file.path.find(file_name) != std::string::npos) {
                    meta_files.emplace_back(index_id, file.path, 0, file.size.value_or(0), dir.get());
                    is_meta = true;
                    break;
                }
            }

            if (!is_meta) {
                normal_files.emplace_back(index_id, file.path, 0, file.size.value_or(0), dir.get());
            }
        }
    }

    file_metadata.reserve(meta_files.size() + normal_files.size());

    // meta file
    for (auto& entry : meta_files) {
        entry.offset = current_offset;
        file_metadata.emplace_back(std::move(entry));
        current_offset += entry.length;
    }
    // normal file
    for (auto& entry : normal_files) {
        entry.offset = current_offset;
        file_metadata.emplace_back(std::move(entry));
        current_offset += entry.length;
    }
    return file_metadata;
}

void CLuceneFileWriter::write_index_headers_and_metadata(lucene::store::IndexOutput* output,
                                                         const std::vector<FileMetadata>& file_metadata) {
    // Group files by index_id and index_suffix
    std::map<int64_t, std::vector<FileMetadata>> indices;

    for (const auto& meta : file_metadata) {
        indices[meta.index_id].push_back(meta);
    }

    for (const auto& [index_id, files] : indices) {
        // Write the index ID and the number of files
        output->writeLong(index_id);
        output->writeInt(static_cast<int32_t>(files.size()));

        // Write file metadata
        for (const auto& file : files) {
            output->writeInt(static_cast<int32_t>(file.filename.length()));
            output->writeBytes(reinterpret_cast<const uint8_t*>(file.filename.data()), file.filename.length());
            output->writeLong(file.offset);
            output->writeLong(file.length);
        }
    }
}

void CLuceneFileWriter::copy_files_data(lucene::store::IndexOutput* output,
                                        const std::vector<FileMetadata>& file_metadata) {
    constexpr int64_t buffer_length = 16384;
    uint8_t buffer[buffer_length];

    for (const auto& meta : file_metadata) {
        copy_file(meta.filename, meta.directory, output, buffer, buffer_length);
    }
}

std::string CLuceneFileWriter::debug_string() const {
    std::stringstream indices_dirs;
    for (const auto& [index, dir] : _indices_dirs) {
        indices_dirs << "index id is: " << index << " , index dir is: " << dir->toString();
    }
    return fmt::format(
            "inverted index file writer debug string: index path prefix is: {}, closed is: {}, total file size is: {}, "
            "index dirs is: {}",
            _index_path, _closed, _total_file_size, indices_dirs.str());
}

} // namespace starrocks
