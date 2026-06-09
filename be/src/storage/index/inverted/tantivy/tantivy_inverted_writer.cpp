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

#include "storage/index/inverted/tantivy/tantivy_inverted_writer.h"

#include <tantivy_binding.h>

#include <filesystem>
#include <vector>

#include "common/config.h"
#include "storage/index/compound_index_common.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "util/slice.h"

namespace starrocks {

namespace tb = ::starrocks::tantivy_binding;

TantivyInvertedWriter::TantivyInvertedWriter(std::string field_name, std::string temp_dir, std::string tokenizer,
                                             int64_t index_id)
        : _field_name(std::move(field_name)),
          _temp_dir(std::move(temp_dir)),
          _tokenizer(std::move(tokenizer)),
          _index_id(index_id) {}

Status TantivyInvertedWriter::create(const TypeInfoPtr& typeinfo, const std::string& field_name,
                                     const std::string& path, TabletIndex* tablet_index,
                                     std::unique_ptr<InvertedWriter>* res) {
    auto parser_str = get_parser_string_from_properties(tablet_index->index_properties());
    std::string tokenizer;
    if (parser_str == INVERTED_INDEX_PARSER_ENGLISH || parser_str == INVERTED_INDEX_PARSER_STANDARD) {
        tokenizer = "english";
    } else if (parser_str == INVERTED_INDEX_PARSER_CHINESE) {
        tokenizer = "cjk";
    } else if (parser_str == INVERTED_INDEX_PARSER_JIEBA) {
        tokenizer = "jieba";
    } else if (parser_str == INVERTED_INDEX_PARSER_NONE) {
        tokenizer = "raw";
    } else {
        return Status::NotSupported("tantivy: unsupported parser '" + parser_str + "'");
    }

    int64_t index_id = tablet_index->index_id();
    auto writer = std::unique_ptr<TantivyInvertedWriter>(
            new TantivyInvertedWriter(field_name, path, std::move(tokenizer), index_id));
    *res = std::move(writer);
    return Status::OK();
}

Status TantivyInvertedWriter::init() {
    std::error_code ec;
    if (std::filesystem::exists(_temp_dir, ec)) {
        return Status::AlreadyExist("tantivy: temp dir already exists, refusing to overwrite: " + _temp_dir);
    }
    std::filesystem::create_directories(_temp_dir, ec);
    if (ec) {
        return Status::IOError("tantivy: failed to create temp dir '" + _temp_dir + "': " + ec.message());
    }

    tb::RustResult r = tb::tantivy_create_index_writer(_temp_dir.c_str(), _field_name.c_str(), _tokenizer.c_str());
    TantivyResultGuard guard(r);
    if (!r.success) {
        return tantivy_status_from_error(r);
    }
    _writer = TantivyWriterGuard(r.value.ptr);
    return Status::OK();
}

void TantivyInvertedWriter::add_values(const void* values, size_t count) {
    if (count == 0 || !_writer) return;

    const auto* slices = reinterpret_cast<const Slice*>(values);

    std::vector<tb::FFISlice> ffi_slices(count);
    for (size_t i = 0; i < count; ++i) {
        ffi_slices[i] = {reinterpret_cast<const uint8_t*>(slices[i].data), slices[i].size};
    }

    tb::RustResult r = tb::tantivy_index_add_strings_batch(_writer.get(), ffi_slices.data(), count);
    TantivyResultGuard guard(r);
    if (!r.success) {
        auto st = tantivy_status_from_error(r);
        LOG(WARNING) << "tantivy add_values failed for field '" << _field_name << "': " << st.message();
        if (_error_status.ok()) {
            _error_status = std::move(st);
        }
        return;
    }

    _rid += static_cast<uint32_t>(count);
    _estimated_size += count * 32;
}

void TantivyInvertedWriter::add_nulls(uint32_t count) {
    if (count == 0 || !_writer) return;

    std::vector<tb::FFISlice> ffi_slices(count, {nullptr, 0});
    tb::RustResult r = tb::tantivy_index_add_strings_batch(_writer.get(), ffi_slices.data(), count);
    TantivyResultGuard guard(r);
    if (!r.success) {
        auto st = tantivy_status_from_error(r);
        LOG(WARNING) << "tantivy add_nulls failed for field '" << _field_name << "': " << st.message();
        if (_error_status.ok()) {
            _error_status = std::move(st);
        }
        return;
    }

    _null_bitmap.addRange(_rid, static_cast<uint64_t>(_rid) + count);
    _rid += count;
}

Status TantivyInvertedWriter::finish(WritableFile* /*wfile*/, ColumnMetaPB* /*meta*/) {
    // Unreachable: ColumnWriter dispatches via need_compound() == true to
    // finish_compound. If we land here, the dispatch contract is broken.
    return Status::InternalError("tantivy writer: finish() must not be called when need_compound() == true");
}

StatusOr<CompoundIndexEntry> TantivyInvertedWriter::finish_compound(ColumnMetaPB* /*meta*/) {
    if (!_writer) {
        return Status::InternalError("tantivy: writer not initialized");
    }

    if (!_error_status.ok() && !config::tantivy_ignore_write_error) {
        return _error_status;
    }

    {
        tb::RustResult r = tb::tantivy_commit_index(_writer.get());
        TantivyResultGuard guard(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
    }
    _writer.reset();

    // Serialize null bitmap to a file in the temp dir.
    if (!_null_bitmap.isEmpty()) {
        auto nbm_size = _null_bitmap.getSizeInBytes();
        std::vector<char> buf(nbm_size);
        _null_bitmap.write(buf.data());
        std::string nbm_path = _temp_dir + "/_starrocks_null_bitmap";
        std::ofstream ofs(nbm_path, std::ios::binary);
        if (!ofs.good()) {
            return Status::IOError("tantivy: failed to create null bitmap file");
        }
        ofs.write(buf.data(), static_cast<std::streamsize>(nbm_size));
        ofs.close();
    }

    // Enumerate all files in the temp dir.
    CompoundIndexEntry entry;
    entry.kind = CompoundIndexKind::INVERTED_TANTIVY;
    entry.index_id = _index_id;
    entry.suffix = "";

    std::error_code ec;
    for (const auto& de : std::filesystem::directory_iterator(_temp_dir, ec)) {
        if (!de.is_regular_file()) continue;
        CompoundFileRef ref;
        ref.name = de.path().filename().string();
        ref.local_path = de.path().string();
        entry.files.push_back(std::move(ref));
    }
    if (ec) {
        return Status::IOError("tantivy: failed to enumerate temp dir: " + ec.message());
    }
    if (entry.files.empty()) {
        return Status::InternalError("tantivy: commit produced no files");
    }

    // Hand off temp dir lifetime to the compound packing flow.
    _packed = true;
    return entry;
}

TantivyInvertedWriter::~TantivyInvertedWriter() noexcept {
    if (_packed) {
        return;
    }
    if (_temp_dir.empty()) {
        return;
    }
    try {
        std::error_code ec;
        std::filesystem::remove_all(_temp_dir, ec);
        if (ec) {
            LOG(WARNING) << "tantivy: failed to cleanup temp dir " << _temp_dir << ": " << ec.message();
        }
    } catch (...) {
        // Destructors must never throw; swallow any unexpected exception
        // from path ops or logging machinery.
    }
}

uint64_t TantivyInvertedWriter::size() const {
    return _estimated_size;
}

} // namespace starrocks
