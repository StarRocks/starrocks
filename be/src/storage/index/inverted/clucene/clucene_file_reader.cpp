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

Status CLuceneFileReader::init(int32_t read_buffer_size) {
    std::unique_lock lock(_mutex); // Lock for writing
    if (!_inited) {
        _read_buffer_size = read_buffer_size;
        RETURN_IF_ERROR(_init_from(read_buffer_size));
        _inited = true;
    }
    return Status::OK();
}

Status CLuceneFileReader::_init_from(int32_t read_buffer_size) {
    try {
        VLOG(10) << "CLuceneFileReader init from " << _index_file_path;
        ASSIGN_OR_RETURN(const auto tmp, StarRocksIndexInput::open(_fs.get(), _index_file_path, read_buffer_size));
        _stream = std::unique_ptr<lucene::store::IndexInput>(tmp);
        _stream->setIndexFile(true);

        // 3. read file
        const int32_t num_indices = _stream->readInt(); // Read number of indices
        ReaderFileEntry* entry = nullptr;

        for (int32_t i = 0; i < num_indices; ++i) {
            int64_t indexId = _stream->readLong();       // Read index ID
            const int32_t numFiles = _stream->readInt(); // Read number of files in the index

            // true, true means it will deconstruct key and value
            auto fileEntries = std::make_unique<EntriesType>(true, true);
            for (int32_t j = 0; j < numFiles; ++j) {
                entry = _CLNEW ReaderFileEntry();

                const int32_t file_name_length = _stream->readInt();

                auto aid = std::make_unique<char[]>(file_name_length + 1);
                _stream->readBytes(reinterpret_cast<uint8_t*>(aid.get()), file_name_length);
                aid[file_name_length] = '\0';
                entry->file_name = std::string(aid.get(), file_name_length);
                entry->offset = _stream->readLong();
                entry->length = _stream->readLong();

                fileEntries->put(aid.release(), entry);
            }
            _indices_entries.emplace(indexId, std::move(fileEntries));
        }
    } catch (CLuceneError& err) {
        std::stringstream ss;
        ss << "CLuceneError occur when init idx file " << _index_file_path << ", error msg: " << err.what();
        if (_stream != nullptr) {
            try {
                _stream->close();
                _stream.reset();
            } catch (CLuceneError& e) {
                ss << "\nSuppressed by: when close index file " << _index_file_path << ", error msg: " << e.what();
            }
        }
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

StatusOr<InvertedIndexDirectoryMap> CLuceneFileReader::get_all_directories() {
    InvertedIndexDirectoryMap res;
    std::shared_lock lock(_mutex); // Lock for reading
    for (const auto& index_id : _indices_entries | std::views::keys) {
        VLOG(10) << "get directory for index_id:" << index_id;
        ASSIGN_OR_RETURN(auto reader, _open(index_id));
        res.emplace(index_id, std::move(reader));
    }
    return res;
}

StatusOr<std::unique_ptr<CLuceneCompoundReader>> CLuceneFileReader::_open(const int64_t& index_id) const {
    std::shared_lock lock(_mutex); // Lock for reading
    if (!_inited) {
        return Status::InternalError(fmt::format(
                "CLuceneError occur when open idx file, file reader for {} is not initialized", _index_file_path));
    }

    // Check if the specified index exists
    const auto& index_it = _indices_entries.find(index_id);
    if (index_it == _indices_entries.end()) {
        std::ostringstream errMsg;
        errMsg << "No index with id " << index_id << " found";
        return Status::InternalError(
                fmt::format("CLuceneError occur when open idx file {}, error msg: {}", _index_file_path, errMsg.str()));
    }
    // Need to clone resource here, because index searcher cache need it.
    // If initialized, we can assume that _stream cannot be nullptr.
    return std::make_unique<CLuceneCompoundReader>(_stream->clone(), index_it->second.get(), _read_buffer_size);
}

StatusOr<std::unique_ptr<CLuceneCompoundReader>> CLuceneFileReader::open(const int64_t& index_id) const {
    return _open(index_id);
}

StatusOr<std::unique_ptr<CLuceneCompoundReader>> CLuceneFileReader::open(
        const std::shared_ptr<TabletIndex>& index_meta) const {
    const auto index_id = index_meta->index_id();
    return _open(index_id);
}

std::string CLuceneFileReader::get_index_file_path(const TabletIndex* index_meta) const {
    return _index_file_path;
}

Status CLuceneFileReader::index_file_exist(const TabletIndex* index_meta, bool* res) const {
    std::shared_lock lock(_mutex); // Lock for reading
    if (!_inited) {
        *res = false;
        return Status::InternalError(fmt::format("idx file {} is not opened", _index_file_path));
    }
    // Check if the specified index exists
    *res = _indices_entries.contains(index_meta->index_id());
    return Status::OK();
}

Status CLuceneFileReader::has_null(const TabletIndex* index_meta, bool* res) const {
    std::shared_lock lock(_mutex); // Lock for reading
    if (!_inited) {
        *res = false;
        return Status::InternalError(fmt::format("idx file {} is not opened", _index_file_path));
    }
    // Check if the specified index exists
    if (const auto& index_it = _indices_entries.find(index_meta->index_id()); index_it == _indices_entries.end()) {
        *res = false;
    } else {
        const auto* entries = index_it->second.get();
        const ReaderFileEntry* e = entries->get(IndexDescriptor::get_temporary_null_bitmap_file_name().data());
        if (e == nullptr) {
            *res = false;
            return Status::OK();
        }
        // roaring bitmap cookie header size is 5
        if (e->length <= 5) {
            *res = false;
        } else {
            *res = true;
        }
    }
    return Status::OK();
}

void CLuceneFileReader::debug_file_entries() {
    std::shared_lock lock(_mutex); // Lock for reading
    for (auto& [index_id, index_entries] : _indices_entries) {
        LOG(INFO) << "index_id:" << index_id;
        for (const auto& val : *index_entries | std::views::values) {
            const ReaderFileEntry* file_entry = val;
            LOG(INFO) << "file entry name:" << file_entry->file_name << " length:" << file_entry->length
                      << " offset:" << file_entry->offset;
        }
    }
}

} // namespace starrocks
