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

#include <fstream>
#include <vector>

#include "storage/index/compound_index_common.h"
#include "storage/index/compound_index_file_reader.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/rowset/options.h"
#include "util/slice.h"

namespace starrocks {

namespace tb = ::starrocks::tantivy_binding;

TantivyInvertedReader::TantivyInvertedReader(std::string path, uint32_t index_id, std::string field_name,
                                             std::string tokenizer_name)
        : InvertedReader(std::move(path), index_id),
          _field_name(std::move(field_name)),
          _tokenizer_name(std::move(tokenizer_name)) {}

Status TantivyInvertedReader::create(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index,
                                     LogicalType field_type, std::unique_ptr<InvertedReader>* res) {
    std::string field_name;
    if (!tablet_index->col_unique_ids().empty()) {
        field_name = std::to_string(tablet_index->col_unique_ids()[0]);
    } else {
        field_name = "content";
    }

    auto parser_str = get_parser_string_from_properties(tablet_index->index_properties());
    std::string tokenizer_name;
    if (parser_str == INVERTED_INDEX_PARSER_ENGLISH || parser_str == INVERTED_INDEX_PARSER_STANDARD) {
        tokenizer_name = "english";
    } else if (parser_str == INVERTED_INDEX_PARSER_CHINESE) {
        tokenizer_name = "cjk";
    } else if (parser_str == INVERTED_INDEX_PARSER_JIEBA) {
        tokenizer_name = "jieba";
    } else if (parser_str == INVERTED_INDEX_PARSER_NONE) {
        tokenizer_name = "raw";
    } else {
        tokenizer_name = "english";
    }

    uint32_t index_id = static_cast<uint32_t>(tablet_index->index_id());
    auto reader =
            std::make_unique<TantivyInvertedReader>(path, index_id, std::move(field_name), std::move(tokenizer_name));
    *res = std::move(reader);
    return Status::OK();
}

Status TantivyInvertedReader::open_compound(TantivyInvertedReader* reader, FileSystem* fs, const std::string& bin_path,
                                            int64_t index_id, const std::string& column_name) {
    auto exists = fs->path_exists(bin_path);
    if (!exists.ok()) {
        return Status::NotFound("compound .idx not present: " + bin_path);
    }

    ASSIGN_OR_RETURN(auto compound_file, CompoundIndexFileReader::open(bin_path, fs));
    ASSIGN_OR_RETURN(auto layout, compound_file->find_index(CompoundIndexKind::INVERTED_TANTIVY, index_id));

    std::string file_table_json = "{";
    for (size_t i = 0; i < layout.files.size(); ++i) {
        if (i > 0) file_table_json += ",";
        file_table_json += fmt::format(R"("{}":{{"offset":{},"length":{}}})", layout.files[i].name,
                                       layout.files[i].offset, layout.files[i].length);
    }
    file_table_json += "}";

    ASSIGN_OR_RETURN(auto ra_file, fs->new_random_access_file(bin_path));

    reader->set_field_name(column_name);

    // Extract null bitmap sidecar if present.
    for (const auto& fe : layout.files) {
        if (fe.name == "_starrocks_null_bitmap" && fe.length > 0) {
            std::vector<char> nbuf(fe.length);
            RETURN_IF_ERROR(ra_file->read_at_fully(fe.offset, nbuf.data(), fe.length));
            reader->set_null_bitmap(roaring::Roaring::read(nbuf.data()));
            break;
        }
    }

    LOG(INFO) << "tantivy compound open: bin_path=" << bin_path << " index_id=" << index_id
              << " file_table=" << file_table_json;
    return reader->load_compound(std::move(ra_file), file_table_json);
}

Status TantivyInvertedReader::load(const IndexReadOptions& /*opt*/, void* /*meta*/) {
    if (_loaded) return Status::OK();

    tb::RustResult r = tb::tantivy_load_index_reader(_index_path.c_str(), _field_name.c_str(), _tokenizer_name.c_str());
    TantivyResultGuard guard(r);
    RETURN_IF_ERROR(tantivy_status_from_error(r));

    _reader = TantivyReaderGuard(r.value.ptr);
    _loaded = true;
    _is_compound = false;

    // Try loading null bitmap if present.
    std::string nbm_path = _index_path + "/_starrocks_null_bitmap";
    std::ifstream ifs(nbm_path, std::ios::binary | std::ios::ate);
    if (ifs.good()) {
        auto fsize = ifs.tellg();
        if (fsize > 0) {
            ifs.seekg(0, std::ios::beg);
            std::vector<char> buf(fsize);
            ifs.read(buf.data(), fsize);
            _null_bitmap = roaring::Roaring::read(buf.data());
        }
    }

    return Status::OK();
}

Status TantivyInvertedReader::load_compound(std::unique_ptr<RandomAccessFile> ra_file,
                                            const std::string& file_table_json) {
    if (_loaded) return Status::OK();

    _compound_ra_file = std::move(ra_file);
    tb::RustResult r = tb::tantivy_open_compound_reader(_compound_ra_file.get(), file_table_json.c_str(),
                                                        _field_name.c_str(), _tokenizer_name.c_str());
    TantivyResultGuard guard(r);
    RETURN_IF_ERROR(tantivy_status_from_error(r));

    _compound_reader = TantivyCompoundReaderGuard(r.value.ptr);
    _loaded = true;
    _is_compound = true;

    return Status::OK();
}

Status TantivyInvertedReader::new_iterator(const std::shared_ptr<TabletIndex> index_meta,
                                           InvertedIndexIterator** iterator, const IndexReadOptions& index_opt) {
    RETURN_IF_ERROR(load(index_opt, nullptr));
    *iterator = new InvertedIndexIterator(index_meta, this, index_opt.stats);
    return Status::OK();
}

Status TantivyInvertedReader::query(OlapReaderStatistics* /*stats*/, const std::string& /*column_name*/,
                                    const void* query_value, InvertedIndexQueryType query_type,
                                    roaring::Roaring* bit_map) {
    void* handle = _is_compound ? _compound_reader.get() : _reader.get();
    if (handle == nullptr) {
        return Status::InternalError(_is_compound ? "tantivy compound reader not loaded" : "tantivy reader not loaded");
    }
    return _query_impl(handle, query_value, query_type, bit_map);
}

Status TantivyInvertedReader::query_scored(OlapReaderStatistics* /*stats*/, const std::string& /*column_name*/,
                                           const void* query_value, InvertedIndexQueryType query_type, int32_t limit,
                                           float min_score, float max_score, roaring::Roaring* bit_map,
                                           std::unordered_map<uint32_t, float>* row_to_score) {
    void* handle = _is_compound ? _compound_reader.get() : _reader.get();
    if (handle == nullptr) {
        return Status::InternalError(_is_compound ? "tantivy compound reader not loaded" : "tantivy reader not loaded");
    }
    return _query_impl_scored(handle, query_value, query_type, limit, min_score, max_score, bit_map, row_to_score);
}

namespace {

struct TokenizedTerms {
    std::vector<std::string> strs;
    std::vector<tb::FFISlice> slices;
};

StatusOr<TokenizedTerms> tokenize_query(const std::string& tokenizer_name, const std::string& text) {
    TokenizedTerms result;
    tb::RustStringArray out{};
    tb::RustResult r = tb::tantivy_tokenize(tokenizer_name.c_str(), reinterpret_cast<const uint8_t*>(text.data()),
                                            text.size(), &out);
    TantivyResultGuard rg(r);
    if (!r.success) {
        if (out.ptr) tb::tantivy_free_string_array(out);
        return tantivy_status_from_error(r);
    }
    result.strs.reserve(out.len);
    for (size_t i = 0; i < out.len; ++i) {
        result.strs.emplace_back(out.ptr[i]);
    }
    tb::tantivy_free_string_array(out);
    result.slices.reserve(result.strs.size());
    for (const auto& t : result.strs) {
        result.slices.push_back({reinterpret_cast<const uint8_t*>(t.data()), t.size()});
    }
    return result;
}

} // namespace

Status TantivyInvertedReader::_query_impl(void* reader_handle, const void* query_value,
                                          InvertedIndexQueryType query_type, roaring::Roaring* bit_map) {
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        tb::RustU32Array out{};
        TantivyU32ArrayGuard arr_guard(out);
        tb::RustResult r =
                tb::tantivy_term_query(reader_handle, reinterpret_cast<const uint8_t*>(slice->data), slice->size, &out);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        bit_map->addMany(out.len, out.ptr);
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_ANY_QUERY: {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        ASSIGN_OR_RETURN(auto terms, tokenize_query(_tokenizer_name, std::string(slice->data, slice->size)));
        if (terms.slices.empty()) return Status::OK();
        tb::RustU32Array out{};
        TantivyU32ArrayGuard arr_guard(out);
        tb::RustResult r = tb::tantivy_match_query(reader_handle, terms.slices.data(), terms.slices.size(), &out);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        bit_map->addMany(out.len, out.ptr);
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_ALL_QUERY: {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        ASSIGN_OR_RETURN(auto terms, tokenize_query(_tokenizer_name, std::string(slice->data, slice->size)));
        if (terms.slices.empty()) return Status::OK();
        tb::RustU32Array out{};
        TantivyU32ArrayGuard arr_guard(out);
        tb::RustResult r = tb::tantivy_match_all_query(reader_handle, terms.slices.data(), terms.slices.size(), &out);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        bit_map->addMany(out.len, out.ptr);
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
        const auto* pqv = reinterpret_cast<const PhraseQueryValue*>(query_value);
        ASSIGN_OR_RETURN(auto terms, tokenize_query(_tokenizer_name, std::string(pqv->text.data, pqv->text.size)));
        if (terms.slices.empty()) return Status::OK();
        tb::RustU32Array out{};
        TantivyU32ArrayGuard arr_guard(out);
        tb::RustResult r = tb::tantivy_phrase_match_query(reader_handle, terms.slices.data(), terms.slices.size(),
                                                          static_cast<uint32_t>(pqv->slop), &out);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        bit_map->addMany(out.len, out.ptr);
        return Status::OK();
    }
    case InvertedIndexQueryType::MATCH_WILDCARD_QUERY: {
        const auto* slice = reinterpret_cast<const Slice*>(query_value);
        tb::RustU32Array out{};
        TantivyU32ArrayGuard arr_guard(out);
        tb::RustResult r = tb::tantivy_wildcard_query(reader_handle, reinterpret_cast<const uint8_t*>(slice->data),
                                                      slice->size, &out);
        TantivyResultGuard rg(r);
        RETURN_IF_ERROR(tantivy_status_from_error(r));
        bit_map->addMany(out.len, out.ptr);
        *bit_map -= _null_bitmap;
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
    const auto* slice = reinterpret_cast<const Slice*>(query_value);
    ASSIGN_OR_RETURN(auto terms, tokenize_query(_tokenizer_name, std::string(slice->data, slice->size)));
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
        r = tb::tantivy_match_query_scored(reader_handle, terms.slices.data(), terms.slices.size(), topk, min_score,
                                           max_score, &ids, &scores);
        break;
    case InvertedIndexQueryType::MATCH_ALL_QUERY:
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
                                         roaring::Roaring* bit_map) {
    *bit_map = _null_bitmap;
    return Status::OK();
}

InvertedIndexReaderType TantivyInvertedReader::get_inverted_index_reader_type() {
    return InvertedIndexReaderType::TEXT;
}

} // namespace starrocks
