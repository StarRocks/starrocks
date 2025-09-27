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

#include "clucene_file_reader.h"

#include "clucene_compound_reader.h"
#include "clucene_fs_directory.h"
#include "storage/index/index_descriptor.h"
#include "storage/tablet_index.h"

namespace starrocks {

CLuceneFileReader::CLuceneFileReader(std::shared_ptr<FileSystem> fs, std::string index_file_path)
        : _fs(std::move(fs)), _index_file_path(std::move(index_file_path)) {}

CLuceneFileReader::~CLuceneFileReader() {
    if (_stream != nullptr) {
        try {
            _stream->close();
            _stream.reset();
        } catch (CLuceneError& e) {
            LOG(WARNING) << "CLuceneError occur when close idx file " << _index_file_path << ", reason: " << e.what();
        }
    }
}

Status CLuceneFileReader::init() {
    try {
        VLOG(10) << "CLuceneFileReader init from " << _index_file_path;
        _stream = std::make_unique<StarRocksIndexInput>(_fs.get(), _index_file_path, BUFFER_SIZE);

        // 3. read file
        const int32_t num_indices = _stream->readInt(); // Read number of indices
        for (int32_t i = 0; i < num_indices; ++i) {
            const int64_t index_id = _stream->readLong(); // Read index ID
            const int32_t num_files = _stream->readInt(); // Read number of files in the index

            std::map<std::string, FileEntry> entries;
            for (int32_t j = 0; j < num_files; ++j) {
                const int32_t file_name_length = _stream->readInt();

                std::string file_name;
                file_name.reserve(file_name_length);
                _stream->readBytes(reinterpret_cast<uint8_t*>(file_name.data()), file_name_length);

                const int64_t offset = _stream->readLong();
                const int64_t length = _stream->readLong();

                entries.emplace(file_name, FileEntry{file_name, offset, length});
            }
            _index_to_entries.emplace(index_id, std::move(entries));
        }
    } catch (CLuceneError& err) {
        return Status::IOError(
                fmt::format("CLuceneError occur when init idx file {}, reason: {}", _index_file_path, err.what()));
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<StarRocksMergedDirectory>> CLuceneFileReader::open(
        const std::shared_ptr<TabletIndex>& index_meta) const {
    auto index_id = index_meta->index_id();
    if (!_index_to_entries.contains(index_id)) {
        return Status::InternalError(
                fmt::format("No index with id {} found in inverted file {}", index_id, _index_file_path));
    }
    const auto& entries = _index_to_entries.at(index_id);
    return std::make_unique<StarRocksMergedDirectory>(_stream, index_id, entries);
}

} // namespace starrocks
