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

#include "storage/index/inverted/tantivy/tantivy_inverted_reader.h"

#include <fmt/format.h>
#include <tantivy_binding.h>

#include <algorithm>
#include <boost/algorithm/string/trim.hpp>
#include <fstream>
#include <iterator>
#include <limits>
#include <vector>

#include "common/config.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/index/compound_index_common.h"
#include "storage/index/compound_index_file_reader.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/rowset/options.h"
#include "util/slice.h"

// FFI callback for the direct-to-bitmap query variants: Rust streams matched BE
// row ids here in blocks and we add them straight into the roaring result,
// avoiding a Vec<u32> round-trip. The return value tells Rust how many ids were
// accepted, allowing the limited scorer to stop when a shared budget is empty.
extern "C" size_t sr_tantivy_append_rowids(void* ctx, const uint32_t* ids, size_t len) {
    reinterpret_cast<roaring::Roaring*>(ctx)->addMany(len, ids);
    return len;
}

struct TantivyLimitedBitmapSink {
    roaring::Roaring* bitmap;
    std::atomic<int64_t>* remaining;
};

extern "C" size_t sr_tantivy_append_rowids_limited(void* ctx, const uint32_t* ids, size_t len) {
    auto* sink = reinterpret_cast<TantivyLimitedBitmapSink*>(ctx);
    int64_t remaining = sink->remaining->load(std::memory_order_relaxed);
    while (remaining > 0) {
        const size_t accepted = std::min<size_t>(len, static_cast<size_t>(remaining));
        if (sink->remaining->compare_exchange_weak(remaining, remaining - accepted, std::memory_order_relaxed)) {
            sink->bitmap->addMany(accepted, ids);
            return accepted;
        }
    }
    return 0;
}

namespace starrocks {

namespace tb = ::starrocks::tantivy_binding;

namespace {
StatusOr<TantivyCanonicalQuery> canonicalize_query(void* analyzer, InvertedIndexQueryType query_type,
                                                   const void* query_value);

bool is_resident_metadata_file(std::string_view name) {
    return name == "meta.json" || name == ".managed.json" || name.ends_with(".term") ||
           name.ends_with(".fieldnorm") || name.ends_with(".fast") || name.ends_with(".lock");
}

void append_mtime(FileSystem* fs, const std::string& path, std::string* version) {
    auto mtime = fs->get_file_modified_time(path);
    if (mtime.ok()) {
        *version += ":mtime=" + std::to_string(mtime.value());
    }
}

std::shared_ptr<TantivyReaderResource> make_reader_resource(MemTracker* allocation_tracker) {
    if (allocation_tracker == nullptr) {
        return std::make_shared<TantivyReaderResource>();
    }
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(allocation_tracker);
    return std::shared_ptr<TantivyReaderResource>(
            new TantivyReaderResource(), [allocation_tracker](TantivyReaderResource* resource) {
                SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(allocation_tracker);
                delete resource;
            });
}
}

TantivyInvertedReader::TantivyInvertedReader(std::string path, uint32_t index_id, std::string field_name,
                                             std::string analyzer_definition, std::string analyzer_digest)
        : InvertedReader(std::move(path), index_id),
          _field_name(std::move(field_name)),
          _analyzer_definition(std::move(analyzer_definition)),
          _analyzer_digest(std::move(analyzer_digest)) {
    _identity.storage_mode = TantivyStorageMode::LOCAL_DIR;
    _identity.canonical_path = _index_path;
    _identity.index_id = _index_id;
    _identity.field_name = _field_name;
    _identity.tokenizer_name = _analyzer_definition;
    _identity.analyzer_digest = _analyzer_digest;
}

Status TantivyInvertedReader::create(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index,
                                     LogicalType field_type, std::unique_ptr<InvertedReader>* res) {
    std::string field_name;
    if (!tablet_index->col_unique_ids().empty()) {
        field_name = std::to_string(tablet_index->col_unique_ids()[0]);
    } else {
        field_name = "content";
    }

    ASSIGN_OR_RETURN(auto analyzer_definition, get_tantivy_analyzer_definition(tablet_index->index_properties()));
    auto analyzer_digest = get_tantivy_analyzer_digest(tablet_index->index_properties());

    uint32_t index_id = static_cast<uint32_t>(tablet_index->index_id());
    auto reader = std::make_unique<TantivyInvertedReader>(path, index_id, std::move(field_name),
                                                          std::move(analyzer_definition), std::move(analyzer_digest));
    tb::RustResult analyzer_result =
            tb::tantivy_create_analyzer(reader->_analyzer_definition.c_str(), reader->_analyzer_digest.c_str());
    TantivyResultGuard analyzer_result_guard(analyzer_result);
    RETURN_IF_ERROR(tantivy_status_from_error(analyzer_result));
    reader->_analyzer = TantivyAnalyzerGuard(analyzer_result.value.ptr);

    const std::string meta_path = path + "/meta.json";
    auto* fs = FileSystem::Default();
    if (fs->path_exists(meta_path).ok()) {
        ASSIGN_OR_RETURN(auto meta_size, fs->get_file_size(meta_path));
        reader->_identity.file_size = meta_size;
        reader->_identity.object_version = "meta.json:size=" + std::to_string(meta_size);
        append_mtime(fs, meta_path, &reader->_identity.object_version);
    }

    *res = std::move(reader);
    return Status::OK();
}

Status TantivyInvertedReader::open_compound(TantivyInvertedReader* reader, FileSystem* fs, const std::string& bin_path,
                                            int64_t index_id, const std::string& column_name,
                                            uint64_t encryption_meta_hash) {
    auto exists = fs->path_exists(bin_path);
    if (!exists.ok()) {
        return exists;
    }

    ASSIGN_OR_RETURN(auto compound_file, CompoundIndexFileReader::open(bin_path, fs));
    auto layout_or = compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, index_id);
    if (!layout_or.ok()) {
        if (layout_or.status().is_not_found()) {
            return Status::Corruption(fmt::format("compound .idx is missing Tantivy index {}: {}", index_id, bin_path));
        }
        return layout_or.status();
    }

    reader->set_field_name(column_name);
    reader->_is_compound = true;
    reader->_compound_fs = fs;
    reader->_identity.storage_mode = TantivyStorageMode::COMPOUND;
    reader->_identity.canonical_path = bin_path;
    reader->_identity.index_id = index_id;
    reader->_identity.field_name = column_name;
    reader->_identity.tokenizer_name = reader->_analyzer_definition;
    reader->_identity.compound_format_version = COMPOUND_BIN_VERSION;
    reader->_identity.index_suffix = layout_or.value().suffix;
    reader->_identity.encryption_meta_hash = encryption_meta_hash;
    ASSIGN_OR_RETURN(reader->_identity.file_size, fs->get_file_size(bin_path));
    reader->_identity.object_version = "compound:size=" + std::to_string(reader->_identity.file_size);
    append_mtime(fs, bin_path, &reader->_identity.object_version);
    return Status::OK();
}

Status TantivyInvertedReader::load(const IndexReadOptions& /*opt*/, void* /*meta*/) {
    return Status::OK();
}

Status TantivyInvertedReader::load_compound(std::unique_ptr<RandomAccessFile> ra_file,
                                            const std::string& file_table_json) {
    auto resource = std::make_shared<TantivyReaderResource>();
    resource->identity = _identity;
    resource->null_bitmap = _preset_null_bitmap;
    resource->ra_file = std::move(ra_file);
    if (auto* cache_manager = ExecEnv::GetInstance()->tantivy_cache_manager(); cache_manager != nullptr) {
        resource->read_buffer_pool = cache_manager->read_buffer_pool();
    }
    tb::RustResult r = tb::tantivy_open_compound_reader(
            resource->ra_file.get(), resource->read_buffer_pool.get(), false, file_table_json.c_str(), "{}",
            _field_name.c_str(), _analyzer_definition.c_str(), _analyzer_digest.c_str());
    TantivyResultGuard guard(r);
    RETURN_IF_ERROR(tantivy_status_from_error(r));

    resource->reader = TantivyReaderGuard(r.value.ptr);
    tb::TantivyReaderResourceUsage usage{};
    if (tb::tantivy_index_reader_resource_usage(resource->reader.get(), &usage)) {
        resource->estimated_bytes = usage.estimated_bytes + resource->null_bitmap.getSizeInBytes();
        resource->materialized_bytes = usage.materialized_bytes;
        resource->resident_bytes = usage.resident_bytes;
        resource->resident_read_count = usage.resident_read_count;
        resource->resident_read_bytes = usage.resident_read_bytes;
        resource->resident_directory = usage.resident_directory;
        resource->fd_charge = usage.fd_charge;
    }
    _direct_resource = std::move(resource);
    _is_compound = true;
    return Status::OK();
}

Status TantivyInvertedReader::new_iterator(const std::shared_ptr<TabletIndex> index_meta,
                                           InvertedIndexIterator** iterator, const IndexReadOptions& index_opt) {
    RETURN_IF_ERROR(load(index_opt, nullptr));
    *iterator = new InvertedIndexIterator(index_meta, this, index_opt.stats,
                                          {.enable_tantivy_reader_cache = index_opt.enable_tantivy_reader_cache,
                                           .enable_tantivy_query_cache = index_opt.enable_tantivy_query_cache});
    return Status::OK();
}

StatusOr<std::shared_ptr<TantivyReaderResource>> TantivyInvertedReader::_get_resource(bool enable_cache) {
    if (_direct_resource != nullptr) {
        return _direct_resource;
    }
    auto* cache_manager = ExecEnv::GetInstance()->tantivy_cache_manager();
    if (!enable_cache || !config::enable_tantivy_reader_cache || cache_manager == nullptr) {
        if (cache_manager != nullptr) {
            cache_manager->reader_cache()->record_bypass();
        }
        return _open_resource(false, nullptr);
    }
    auto* allocation_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    return cache_manager->reader_cache()->get_or_load(_identity, [this, allocation_tracker] {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(allocation_tracker);
        return _open_resource(true, allocation_tracker);
    });
}

StatusOr<std::shared_ptr<TantivyReaderResource>> TantivyInvertedReader::_open_resource(
        bool allow_resident_directory, MemTracker* allocation_tracker) const {
    return _is_compound ? _open_compound_resource(allow_resident_directory, allocation_tracker)
                        : _open_local_resource(allocation_tracker);
}

StatusOr<std::shared_ptr<TantivyReaderResource>> TantivyInvertedReader::_open_local_resource(
        MemTracker* allocation_tracker) const {
    auto resource = make_reader_resource(allocation_tracker);
    resource->identity = _identity;
    if (!_analyzer_digest.empty()) {
        std::ifstream manifest(_index_path + "/_starrocks_analyzer_manifest", std::ios::binary);
        if (!manifest.good()) {
            return Status::Corruption("tantivy analyzer manifest is missing for digest " + _analyzer_digest);
        }
        std::string actual((std::istreambuf_iterator<char>(manifest)), std::istreambuf_iterator<char>());
        boost::algorithm::trim(actual);
        if (actual != _analyzer_digest) {
            return Status::Corruption(
                    fmt::format("tantivy analyzer manifest mismatch: metadata={}, index={}", _analyzer_digest, actual));
        }
    }
    tb::RustResult r = tb::tantivy_load_index_reader(_index_path.c_str(), _field_name.c_str(),
                                                     _analyzer_definition.c_str(), _analyzer_digest.c_str());
    TantivyResultGuard guard(r);
    RETURN_IF_ERROR(tantivy_status_from_error(r));
    resource->reader = TantivyReaderGuard(r.value.ptr);

    std::string nbm_path = _index_path + "/_starrocks_null_bitmap";
    std::ifstream ifs(nbm_path, std::ios::binary | std::ios::ate);
    if (ifs.good()) {
        auto fsize = ifs.tellg();
        if (fsize > 0) {
            ifs.seekg(0, std::ios::beg);
            std::vector<char> buf(fsize);
            ifs.read(buf.data(), fsize);
            resource->null_bitmap = roaring::Roaring::read(buf.data());
        }
    }
    tb::TantivyReaderResourceUsage usage{};
    if (tb::tantivy_index_reader_resource_usage(resource->reader.get(), &usage)) {
        resource->estimated_bytes = usage.estimated_bytes + resource->null_bitmap.getSizeInBytes();
        resource->fd_charge = usage.fd_charge;
    }
    return resource;
}

StatusOr<std::shared_ptr<TantivyReaderResource>> TantivyInvertedReader::_open_compound_resource(
        bool allow_resident_directory, MemTracker* allocation_tracker) const {
    if (_compound_fs == nullptr) {
        return Status::InternalError("tantivy compound filesystem is not configured");
    }
    ASSIGN_OR_RETURN(auto compound_file, CompoundIndexFileReader::open(_identity.canonical_path, _compound_fs));
    auto layout_or = compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, _identity.index_id);
    if (!layout_or.ok()) {
        if (layout_or.status().is_not_found()) {
            return Status::Corruption(fmt::format("compound .idx is missing Tantivy index {}: {}", _identity.index_id,
                                                  _identity.canonical_path));
        }
        return layout_or.status();
    }
    auto layout = std::move(layout_or).value();
    std::string file_table_json = "{";
    size_t file_count = 0;
    size_t logical_file_bytes = 0;
    size_t null_bitmap_file_bytes = 0;
    for (const auto& file : layout.files) {
        if (file.name == "_starrocks_null_bitmap") {
            null_bitmap_file_bytes = static_cast<size_t>(file.length);
            continue;
        }
        if (file_count++ > 0) file_table_json += ",";
        file_table_json +=
                fmt::format(R"("{}":{{"offset":{},"length":{}}})", file.name, file.offset, file.length);
        if (file.length > std::numeric_limits<size_t>::max() - logical_file_bytes) {
            logical_file_bytes = std::numeric_limits<size_t>::max();
        } else {
            logical_file_bytes += static_cast<size_t>(file.length);
        }
    }
    file_table_json += "}";
    std::string resident_file_table_json = "{}";

    auto resource = make_reader_resource(allocation_tracker);
    resource->identity = _identity;
    resource->identity.index_suffix = layout.suffix;
    bool use_resident_directory = false;
    if (auto* cache_manager = ExecEnv::GetInstance()->tantivy_cache_manager(); cache_manager != nullptr) {
        resource->read_buffer_pool = cache_manager->read_buffer_pool();
        auto would_admit_resident_bytes = [&](size_t resident_file_bytes) {
            size_t metadata_estimate =
                    file_table_json.size() + layout.files.size() * sizeof(CompoundFileEntry) + null_bitmap_file_bytes;
            const size_t reader_headroom = std::max<size_t>(1UL << 20, resident_file_bytes / 10);
            metadata_estimate = metadata_estimate > std::numeric_limits<size_t>::max() - reader_headroom
                                        ? std::numeric_limits<size_t>::max()
                                        : metadata_estimate + reader_headroom;
            const size_t admission_estimate =
                    resident_file_bytes > std::numeric_limits<size_t>::max() - metadata_estimate
                            ? std::numeric_limits<size_t>::max()
                            : resident_file_bytes + metadata_estimate;
            return cache_manager->reader_cache()->would_admit(resource->identity, admission_estimate);
        };

        bool resident_all_files = allow_resident_directory && would_admit_resident_bytes(logical_file_bytes);
        size_t resident_file_bytes = logical_file_bytes;
        if (allow_resident_directory && !resident_all_files) {
            resident_file_bytes = 0;
            for (const auto& file : layout.files) {
                if (file.name != "_starrocks_null_bitmap" && is_resident_metadata_file(file.name)) {
                    resident_file_bytes =
                            file.length > std::numeric_limits<size_t>::max() - resident_file_bytes
                                    ? std::numeric_limits<size_t>::max()
                                    : resident_file_bytes + static_cast<size_t>(file.length);
                }
            }
        }
        use_resident_directory = allow_resident_directory && resident_file_bytes > 0 &&
                                 would_admit_resident_bytes(resident_file_bytes);
        if (use_resident_directory) {
            resident_file_table_json = "{";
            size_t resident_file_count = 0;
            for (const auto& file : layout.files) {
                if (file.name == "_starrocks_null_bitmap" ||
                    (!resident_all_files && !is_resident_metadata_file(file.name))) {
                    continue;
                }
                if (resident_file_count++ > 0) resident_file_table_json += ",";
                resident_file_table_json +=
                        fmt::format(R"("{}":{{"offset":{},"length":{}}})", file.name, file.offset, file.length);
            }
            resident_file_table_json += "}";
        }
    }
    ASSIGN_OR_RETURN(resource->ra_file, _compound_fs->new_random_access_file(_identity.canonical_path));
    for (const auto& file : layout.files) {
        if (file.name == "_starrocks_null_bitmap" && file.length > 0) {
            std::vector<char> buffer(file.length);
            RETURN_IF_ERROR(resource->ra_file->read_at_fully(file.offset, buffer.data(), file.length));
            resource->null_bitmap = roaring::Roaring::read(buffer.data());
            break;
        }
    }

    if (!_analyzer_digest.empty()) {
        const auto manifest = std::find_if(layout.files.begin(), layout.files.end(), [](const auto& file) {
            return file.name == "_starrocks_analyzer_manifest";
        });
        if (manifest == layout.files.end()) {
            return Status::Corruption("tantivy analyzer manifest is missing for digest " + _analyzer_digest);
        }
        std::string actual(manifest->length, '\0');
        RETURN_IF_ERROR(resource->ra_file->read_at_fully(manifest->offset, actual.data(), manifest->length));
        boost::algorithm::trim(actual);
        if (actual != _analyzer_digest) {
            return Status::Corruption(fmt::format("tantivy analyzer manifest mismatch: metadata={}, index={}",
                                                  _analyzer_digest, actual));
        }
    }

    tb::RustResult r = tb::tantivy_open_compound_reader(
            resource->ra_file.get(), resource->read_buffer_pool.get(), use_resident_directory, file_table_json.c_str(),
            resident_file_table_json.c_str(), _field_name.c_str(), _analyzer_definition.c_str(),
            _analyzer_digest.c_str());
    TantivyResultGuard guard(r);
    RETURN_IF_ERROR(tantivy_status_from_error(r));
    resource->reader = TantivyReaderGuard(r.value.ptr);
    tb::TantivyReaderResourceUsage usage{};
    if (tb::tantivy_index_reader_resource_usage(resource->reader.get(), &usage)) {
        resource->estimated_bytes = usage.estimated_bytes + resource->null_bitmap.getSizeInBytes() +
                                    file_table_json.size() + layout.files.size() * sizeof(CompoundFileEntry);
        resource->materialized_bytes = usage.materialized_bytes;
        resource->resident_bytes = usage.resident_bytes;
        resource->resident_read_count = usage.resident_read_count;
        resource->resident_read_bytes = usage.resident_read_bytes;
        resource->resident_directory = usage.resident_directory;
        resource->fd_charge = usage.fd_charge;
    }
    return resource;
}

Status TantivyInvertedReader::query(OlapReaderStatistics* /*stats*/, const std::string& /*column_name*/,
                                    const void* query_value, InvertedIndexQueryType query_type,
                                    roaring::Roaring* bit_map, const InvertedIndexQueryOptions& options) {
    auto* cache_manager = ExecEnv::GetInstance()->tantivy_cache_manager();
    const bool use_query_cache =
            options.enable_tantivy_query_cache && config::enable_tantivy_query_cache && cache_manager != nullptr;
    if (!use_query_cache && cache_manager != nullptr) {
        cache_manager->query_cache()->record_bypass();
    }
    std::string cache_key;
    if (use_query_cache) {
        ASSIGN_OR_RETURN(auto canonical, canonicalize_query(_analyzer.get(), query_type, query_value));
        cache_key = canonical.encode_with(_identity);
        if (auto cached = cache_manager->query_cache()->lookup(cache_key); cached != nullptr) {
            *bit_map = *cached;
            return Status::OK();
        }
    }

    ASSIGN_OR_RETURN(auto resource, _get_resource(options.enable_tantivy_reader_cache));
    RETURN_IF_ERROR(
            _query_impl(resource->reader.get(), resource->null_bitmap, query_value, query_type, 0, nullptr, bit_map));
    if (use_query_cache) {
        cache_manager->query_cache()->maybe_insert(cache_key, *bit_map);
    }
    return Status::OK();
}

Status TantivyInvertedReader::query_limited(OlapReaderStatistics* /*stats*/, const std::string& /*column_name*/,
                                            const void* query_value, InvertedIndexQueryType query_type, int32_t limit,
                                            std::atomic<int64_t>* global_budget, roaring::Roaring* bit_map,
                                            const InvertedIndexQueryOptions& options) {
    if (auto* cache_manager = ExecEnv::GetInstance()->tantivy_cache_manager(); cache_manager != nullptr) {
        cache_manager->query_cache()->record_bypass();
    }
    if (limit <= 0 || (global_budget != nullptr && global_budget->load(std::memory_order_relaxed) <= 0)) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(auto resource, _get_resource(options.enable_tantivy_reader_cache));
    return _query_impl(resource->reader.get(), resource->null_bitmap, query_value, query_type, limit, global_budget,
                       bit_map);
}

Status TantivyInvertedReader::query_scored(OlapReaderStatistics* /*stats*/, const std::string& /*column_name*/,
                                           const void* query_value, InvertedIndexQueryType query_type, int32_t limit,
                                           float min_score, float max_score, roaring::Roaring* bit_map,
                                                        std::unordered_map<uint32_t, float>* row_to_score,
                                                        const InvertedIndexQueryOptions& options) {
    if (auto* cache_manager = ExecEnv::GetInstance()->tantivy_cache_manager(); cache_manager != nullptr) {
        cache_manager->query_cache()->record_bypass();
    }
    ASSIGN_OR_RETURN(auto resource, _get_resource(options.enable_tantivy_reader_cache));
    return _query_impl_scored(resource->reader.get(), query_value, query_type, limit, min_score, max_score, bit_map,
                              row_to_score);
}

namespace {

struct TokenizedTerms {
    std::vector<std::string> strs;
    std::vector<tb::FFISlice> slices;
    std::vector<uint32_t> positions;
};

StatusOr<TokenizedTerms> tokenize_query(void* analyzer, const std::string& text) {
    TokenizedTerms result;
    tb::RustTokenArray out{};
    tb::RustResult r = tb::tantivy_analyzer_tokenize_detail(analyzer, reinterpret_cast<const uint8_t*>(text.data()),
                                                            text.size(), &out);
    TantivyResultGuard rg(r);
    if (!r.success) {
        if (out.ptr) tb::tantivy_free_token_array(out);
        return tantivy_status_from_error(r);
    }
    result.strs.reserve(out.len);
    result.positions.reserve(out.len);
    for (size_t i = 0; i < out.len; ++i) {
        result.strs.emplace_back(out.ptr[i].term);
        result.positions.emplace_back(static_cast<uint32_t>(out.ptr[i].position));
    }
    tb::tantivy_free_token_array(out);
    result.slices.reserve(result.strs.size());
    for (const auto& t : result.strs) {
        result.slices.push_back({reinterpret_cast<const uint8_t*>(t.data()), t.size()});
    }
    return result;
}

TokenizedTerms use_tokenized_query(const TokenizedQueryValue& query) {
    TokenizedTerms result;
    result.strs = query.terms;
    result.slices.reserve(result.strs.size());
    for (const auto& term : result.strs) {
        result.slices.push_back({reinterpret_cast<const uint8_t*>(term.data()), term.size()});
    }
    return result;
}

StatusOr<TantivyCanonicalQuery> canonicalize_query(void* analyzer, InvertedIndexQueryType query_type,
                                                   const void* query_value) {
    TantivyCanonicalQuery canonical;
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        canonical.type = TantivyCanonicalQueryType::EQUAL;
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        canonical.raw_value.assign(slice->data, slice->size);
        break;
    }
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
    case InvertedIndexQueryType::MATCH_ALL_QUERY: {
        canonical.type = query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ? TantivyCanonicalQueryType::MATCH_ANY
                                                                               : TantivyCanonicalQueryType::MATCH_ALL;
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        ASSIGN_OR_RETURN(auto tokenized, tokenize_query(analyzer, std::string(slice->data, slice->size)));
        canonical.terms = std::move(tokenized.strs);
        break;
    }
    case InvertedIndexQueryType::MATCH_ANY_TERMS_QUERY:
    case InvertedIndexQueryType::MATCH_ALL_TERMS_QUERY: {
        canonical.type = query_type == InvertedIndexQueryType::MATCH_ANY_TERMS_QUERY
                                 ? TantivyCanonicalQueryType::MATCH_ANY
                                 : TantivyCanonicalQueryType::MATCH_ALL;
        canonical.terms = reinterpret_cast<const TokenizedQueryValue*>(query_value)->terms;
        break;
    }
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
        canonical.type = TantivyCanonicalQueryType::MATCH_PHRASE;
        const auto* phrase = reinterpret_cast<const PhraseQueryValue*>(query_value);
        ASSIGN_OR_RETURN(auto tokenized, tokenize_query(analyzer, std::string(phrase->text.data, phrase->text.size)));
        canonical.terms = std::move(tokenized.strs);
        canonical.raw_value.assign(phrase->text.data, phrase->text.size);
        canonical.slop = static_cast<uint32_t>(phrase->slop);
        break;
    }
    case InvertedIndexQueryType::MATCH_WILDCARD_QUERY: {
        canonical.type = TantivyCanonicalQueryType::MATCH_WILDCARD;
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        canonical.raw_value.assign(slice->data, slice->size);
        break;
    }
    default:
        return Status::NotSupported("tantivy: unsupported query type " + std::to_string(static_cast<int>(query_type)));
    }

    if (canonical.type == TantivyCanonicalQueryType::MATCH_ANY ||
        canonical.type == TantivyCanonicalQueryType::MATCH_ALL) {
        std::sort(canonical.terms.begin(), canonical.terms.end());
        canonical.terms.erase(std::unique(canonical.terms.begin(), canonical.terms.end()), canonical.terms.end());
    }
    return canonical;
}

} // namespace

Status TantivyInvertedReader::_query_impl(void* reader_handle, const roaring::Roaring& null_bitmap,
                                          const void* query_value, InvertedIndexQueryType query_type, int32_t limit,
                                          std::atomic<int64_t>* global_budget, roaring::Roaring* bit_map) {
    TantivyLimitedBitmapSink limited_sink{bit_map, global_budget};
    void* sink_ctx = global_budget != nullptr ? static_cast<void*>(&limited_sink) : static_cast<void*>(bit_map);
    tb::SetBitmapFn append = global_budget != nullptr ? sr_tantivy_append_rowids_limited : sr_tantivy_append_rowids;
    const size_t rust_limit = limit > 0 ? static_cast<size_t>(limit) : 0;
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        tb::RustResult r = tb::tantivy_term_query_bitmap(reader_handle, reinterpret_cast<const uint8_t*>(slice->data),
                                                         slice->size, rust_limit, sink_ctx, append);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_ANY_QUERY: {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        ASSIGN_OR_RETURN(auto terms, tokenize_query(_analyzer.get(), std::string(slice->data, slice->size)));
        if (terms.slices.empty()) return Status::OK();
        tb::RustResult r = tb::tantivy_match_query_bitmap(reader_handle, terms.slices.data(), terms.slices.size(),
                                                          rust_limit, sink_ctx, append);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_ANY_TERMS_QUERY: {
        const auto* query = reinterpret_cast<const TokenizedQueryValue*>(query_value);
        auto terms = use_tokenized_query(*query);
        if (terms.slices.empty()) return Status::OK();
        tb::RustResult r = tb::tantivy_match_query_bitmap(reader_handle, terms.slices.data(), terms.slices.size(),
                                                          rust_limit, sink_ctx, append);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_ALL_QUERY: {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        ASSIGN_OR_RETURN(auto terms, tokenize_query(_analyzer.get(), std::string(slice->data, slice->size)));
        if (terms.slices.empty()) return Status::OK();
        tb::RustResult r = tb::tantivy_match_all_query_bitmap(reader_handle, terms.slices.data(), terms.slices.size(),
                                                              config::tantivy_match_all_bitmap_min_df_ratio, rust_limit,
                                                              sink_ctx, append);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_ALL_TERMS_QUERY: {
        const auto* query = reinterpret_cast<const TokenizedQueryValue*>(query_value);
        auto terms = use_tokenized_query(*query);
        if (terms.slices.empty()) return Status::OK();
        tb::RustResult r = tb::tantivy_match_all_query_bitmap(reader_handle, terms.slices.data(), terms.slices.size(),
                                                              config::tantivy_match_all_bitmap_min_df_ratio, rust_limit,
                                                              sink_ctx, append);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
        const auto* pqv = reinterpret_cast<const PhraseQueryValue*>(query_value);
        ASSIGN_OR_RETURN(auto terms, tokenize_query(_analyzer.get(), std::string(pqv->text.data, pqv->text.size)));
        if (terms.slices.empty()) return Status::OK();
        tb::RustResult r = tb::tantivy_phrase_match_query_bitmap(
                reader_handle, terms.slices.data(), terms.slices.size(), terms.positions.data(),
                static_cast<uint32_t>(pqv->slop), rust_limit, sink_ctx, append);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_WILDCARD_QUERY: {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        tb::RustResult r =
                tb::tantivy_wildcard_query_bitmap(reader_handle, reinterpret_cast<const uint8_t*>(slice->data),
                                                  slice->size, rust_limit, sink_ctx, append);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        *bit_map -= null_bitmap;
        return Status::OK();
    }
    default:
        return Status::NotSupported("tantivy: unsupported query type " + std::to_string(static_cast<int>(query_type)));
    }
}

Status TantivyInvertedReader::_query_impl_scored(void* reader_handle, const void* query_value,
                                                 InvertedIndexQueryType query_type, int32_t limit, float min_score,
                                                 float max_score, roaring::Roaring* bit_map,
                                                 std::unordered_map<uint32_t, float>* row_to_score) {
    TokenizedTerms terms;
    if (query_type == InvertedIndexQueryType::MATCH_ANY_TERMS_QUERY ||
        query_type == InvertedIndexQueryType::MATCH_ALL_TERMS_QUERY) {
        terms = use_tokenized_query(*reinterpret_cast<const TokenizedQueryValue*>(query_value));
    } else {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        ASSIGN_OR_RETURN(terms, tokenize_query(_analyzer.get(), std::string(slice->data, slice->size)));
    }
    if (terms.slices.empty()) return Status::OK();

    // limit > 0 pushes the SQL LIMIT into tantivy's TopDocs (top-k pruning);
    // 0 means score every hit (e.g. ORDER BY score() ASC). min/max_score gate the
    // hits to the inclusive [min, max] BM25 range inside tantivy (WHERE score()>c).
    const uint64_t topk = limit > 0 ? static_cast<uint64_t>(limit) : 0;
    tb::RustU32Array ids{};
    tb::RustF32Array scores{};
    TantivyU32ArrayGuard id_guard(ids);
    TantivyF32ArrayGuard score_guard(scores);
    tb::RustResult r{};
    switch (query_type) {
    case InvertedIndexQueryType::MATCH_ANY_QUERY:
    case InvertedIndexQueryType::MATCH_ANY_TERMS_QUERY:
        r = tb::tantivy_match_query_scored(reader_handle, terms.slices.data(), terms.slices.size(), topk, min_score,
                                           max_score, &ids, &scores);
        break;
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
    case InvertedIndexQueryType::MATCH_ALL_TERMS_QUERY:
        r = tb::tantivy_match_all_query_scored(reader_handle, terms.slices.data(), terms.slices.size(), topk, min_score,
                                               max_score, &ids, &scores);
        break;
    default:
        return Status::NotSupported("tantivy: scored query only supports MATCH_ANY/MATCH_ALL, got " +
                                    std::to_string(static_cast<int>(query_type)));
    }
    TantivyResultGuard rg(r);
    RETURN_IF_ERROR(tantivy_status_from_error(r));
    // Two parallel arrays: ids[i] matched with BM25 score scores[i].
    bit_map->addMany(ids.len, ids.ptr);
    row_to_score->reserve(row_to_score->size() + ids.len);
    for (size_t i = 0; i < ids.len; ++i) {
        (*row_to_score)[ids.ptr[i]] = scores.ptr[i];
    }
    return Status::OK();
}

Status TantivyInvertedReader::query_null(OlapReaderStatistics* /*stats*/, const std::string& /*column_name*/,
                                         roaring::Roaring* bit_map, const InvertedIndexQueryOptions& options) {
    if (auto* cache_manager = ExecEnv::GetInstance()->tantivy_cache_manager(); cache_manager != nullptr) {
        cache_manager->query_cache()->record_bypass();
    }
    ASSIGN_OR_RETURN(auto resource, _get_resource(options.enable_tantivy_reader_cache));
    *bit_map = resource->null_bitmap;
    return Status::OK();
}

InvertedIndexReaderType TantivyInvertedReader::get_inverted_index_reader_type() {
    return InvertedIndexReaderType::TEXT;
}

} // namespace starrocks
