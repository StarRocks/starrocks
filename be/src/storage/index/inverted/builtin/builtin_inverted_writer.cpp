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

#include "storage/index/inverted/builtin/builtin_inverted_writer.h"

#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/Misc.h>
#include <fmt/format.h>

#include <algorithm>
#include <boost/locale/encoding_utf.hpp>
#include <functional>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/index/inverted/builtin/block_posting_writer.h"
#include "storage/index/inverted/builtin/builtin_simple_analyzer.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_writer.h"
#include "storage/tablet_index.h"
#include "storage/types.h"
#include "types/logical_type.h"
#include "types/storage_type_traits.h"

namespace starrocks {

namespace {
// Write a fixed-width u32 IndexedColumn (ordinal-indexed) of `n` values, value i = get(i).
// Used for the doc_freq column (one df per term) and the doc_len column (one length per row).
Status write_u32_indexed_column(WritableFile* wfile, IndexedColumnMetaPB* meta,
                                const std::function<uint32_t(size_t)>& get, size_t n) {
    TypeInfoPtr typeinfo = get_type_info(TYPE_INT);
    IndexedColumnWriterOptions options;
    options.write_ordinal_index = true; // random access by ordinal (term ordinal / rowid)
    options.write_value_index = false;
    // INT's default encoding is BIT_SHUFFLE (ordinal-readable, unlike VARCHAR's DICT default).
    options.encoding = EncodingInfo::get_default_encoding(typeinfo->type(), false);
    options.compression = CompressionTypePB::ZSTD; // small Zipfian integers; read rarely (Phase-1)
    IndexedColumnWriter writer(options, typeinfo, wfile);
    RETURN_IF_ERROR(writer.init());
    for (size_t i = 0; i < n; ++i) {
        int32_t v = static_cast<int32_t>(get(i));
        RETURN_IF_ERROR(writer.add(&v));
    }
    return writer.finish(meta);
}
} // namespace

template <LogicalType field_type>
class BuiltinInvertedWriterImpl : public BuiltinInvertedWriter {
public:
    using CppType = StorageCppType<field_type>;

    explicit BuiltinInvertedWriterImpl(std::unique_ptr<BitmapIndexWriter>& writer, const TabletIndex* inverted_index)
            : _builtin_writer(std::move(writer)) {
        static_assert(field_type == TYPE_CHAR || field_type == TYPE_VARCHAR);
        _parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(inverted_index->index_properties()));
        DCHECK(_parser_type != InvertedIndexParserType::PARSER_UNKNOWN);
        _lower_case = get_lower_case_from_properties(inverted_index->index_properties());
        _index_options = get_index_options_from_properties(inverted_index->index_properties());
    }

    Status init() override;

    void add_values(const void* values, size_t count) override;

    void add_nulls(uint32_t count) override {
        _builtin_writer->add_nulls(count);
        if (capture_freqs()) {
            // Null rows contribute no tokens: doc_len 0, and the rowid is skipped in postings.
            _doc_len.resize(_doc_len.size() + count, 0);
            _freq_capture_bytes += static_cast<uint64_t>(count) * sizeof(uint32_t);
        }
    }

    Status finish(WritableFile* wfile, ColumnMetaPB* meta) override;

    uint64_t size() const override { return _builtin_writer->size() + _freq_capture_bytes; }

private:
    // Tokenize one row exactly the way the presence bitmap is fed, invoking `on_token(Slice)` per
    // token. This is the single tokenization site shared by both index_options: DOCS only feeds the
    // bitmap, DOCS_AND_FREQS additionally accumulates tf/doc_len from the same token stream, so the
    // captured stats can never drift from the dictionary. PARSER_NONE / PARSER_ENGLISH stay
    // zero-copy (the Slice points into caller-owned memory); only the CLucene path allocates a
    // per-token string, where the encoding conversion makes a copy unavoidable.
    template <class OnToken>
    void for_each_token(const Slice& value, OnToken&& on_token) {
        if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
            on_token(value);
        } else if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            std::string mutable_text(value.data, value.size);
            std::vector<SliceToken> tokens;
            _builtin_analyzer->tokenize(mutable_text.data(), mutable_text.length(), tokens);
            for (const auto& token : tokens) {
                on_token(token.text);
            }
        } else {
            std::wstring tchar = boost::locale::conv::utf_to_utf<TCHAR>(value.data, value.data + value.size);
            _char_string_reader->init(tchar.c_str(), tchar.size(), false);
            auto stream = _analyzer->reusableTokenStream(L"", _char_string_reader.get());
            lucene::analysis::Token token;
            while (stream->next(&token)) {
                if (token.termLength() != 0) {
                    std::string str = boost::locale::conv::utf_to_utf<char>(token.termBuffer(),
                                                                            token.termBuffer() + token.termLength());
                    on_token(Slice(str));
                }
            }
        }
    }

    // Persist the captured freqs/norms side data. Called from finish() only on the DOCS_AND_FREQS
    // path, after the presence bitmap is written. finish_freqs() writes the block posting (docid+tf) +
    // doc_freq column; finish_norms() writes the per-row doc_len column + segment sum_len.
    Status finish_freqs(WritableFile* wfile, PostingIndexPB* posting);
    Status finish_norms(WritableFile* wfile, NormsPB* norms);

    std::unique_ptr<BitmapIndexWriter> _builtin_writer;
    std::unique_ptr<lucene::analysis::Analyzer> _analyzer{};
    std::unique_ptr<lucene::util::StringReader> _char_string_reader{};

    std::unique_ptr<SimpleAnalyzer> _builtin_analyzer{};

    InvertedIndexParserType _parser_type;
    bool _lower_case = true;

    // Index options level (default DOCS). freqs/norms are captured iff the level is at least
    // DOCS_AND_FREQS; keeping the level (not a bool) lets future levels add their own `>=` gates.
    InvertedIndexOptions _index_options = InvertedIndexOptions::DOCS;
    bool capture_freqs() const { return _index_options >= InvertedIndexOptions::DOCS_AND_FREQS; }

    // freqs/norms (>= DOCS_AND_FREQS) capture state; untouched on the default DOCS path.
    struct PostingList {
        std::vector<uint32_t> docids;
        std::vector<uint32_t> tfs;
    };
    // term -> postings. Unordered: finish() materializes the terms into a vector and sorts them by
    // Slice::compare, so the container's own iteration order is never relied on -- an unordered_map
    // avoids the red-black tree node overhead of std::map for the (potentially large) distinct-term set.
    std::unordered_map<std::string, PostingList> _freq_index;
    std::vector<uint32_t> _doc_len; // per row (token count)
    uint64_t _sum_len = 0;
    // Rough heap estimate of the freq-capture state (_freq_index + _doc_len), maintained incrementally
    // so size() reflects it for the segment-flush estimator (which otherwise sees only the bitmap).
    uint64_t _freq_capture_bytes = 0;
};

template <LogicalType field_type>
Status BuiltinInvertedWriterImpl<field_type>::init() {
    // init tokenizer relative context
    _char_string_reader = std::make_unique<lucene::util::StringReader>(L"");
    if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
        _analyzer = std::make_unique<lucene::analysis::standard::StandardAnalyzer>();
    } else if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
        _builtin_analyzer = std::make_unique<SimpleAnalyzer>(_lower_case);
    } else if (_parser_type == InvertedIndexParserType::PARSER_CHINESE) {
        auto chinese_analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
        chinese_analyzer->setLanguage(L"cjk");
        _analyzer.reset(chinese_analyzer);
    }
    return Status::OK();
}

template <LogicalType field_type>
void BuiltinInvertedWriterImpl<field_type>::add_values(const void* values, size_t count) {
    const Slice* val = static_cast<const Slice*>(values);
    for (size_t i = 0; i < count; ++i) {
        if (!capture_freqs()) {
            // DOCS: only write docs (feed the presence bitmap). Byte-identical to the historical path.
            for_each_token(*val, [&](const Slice& tk) { _builtin_writer->add_value_with_current_rowid((void*)&tk); });
        } else {
            // DOCS_AND_FREQS: write docs AND freqs -- feed the presence bitmap while capturing this
            // row's tf per term plus doc_len, and accumulate sum_len, all from one token pass.
            // Postings reuse the presence-bitmap writer's row counter (advanced by incre_rowid()
            // below) rather than keeping a second one here that could drift from it.
            const uint32_t rid = _builtin_writer->current_rowid();
            std::unordered_map<std::string, uint32_t> tf;
            uint32_t doc_len = 0;
            for_each_token(*val, [&](const Slice& tk) {
                _builtin_writer->add_value_with_current_rowid((void*)&tk);
                tf[std::string(tk.data, tk.size)]++;
                ++doc_len;
            });
            for (const auto& [term, f] : tf) {
                PostingList& pl = _freq_index[term];
                if (pl.docids.empty()) {
                    // First posting of a new distinct term: account key + map node + PostingList
                    // (rough; only feeds the flush size estimator, see _freq_capture_bytes / size()).
                    _freq_capture_bytes += term.size() + sizeof(PostingList) + 64;
                }
                pl.docids.push_back(rid);
                pl.tfs.push_back(f);
                _freq_capture_bytes += 2 * sizeof(uint32_t); // one (docid, tf) posting
            }
            _doc_len.push_back(doc_len);
            _freq_capture_bytes += sizeof(uint32_t);
            _sum_len += doc_len;
        }
        ++val;
        _builtin_writer->incre_rowid();
    }
}

template <LogicalType field_type>
Status BuiltinInvertedWriterImpl<field_type>::finish(WritableFile* wfile, ColumnMetaPB* meta) {
    ColumnIndexMetaPB* index_meta = meta->add_indexes();
    index_meta->set_type(BUILTIN_INVERTED_INDEX);
    BuiltinInvertedIndexPB* inverted_index_meta = index_meta->mutable_builtin_inverted_index();
    BitmapIndexPB* bitmap_index_meta = inverted_index_meta->mutable_bitmap_index();
    RETURN_IF_ERROR(_builtin_writer->finish(wfile, bitmap_index_meta));
    if (!capture_freqs()) {
        return Status::OK();
    }
    // Persist the actual level. The two ladders are kept numerically identical (asserted here), so a
    // new level added to InvertedIndexOptions must also be added to the proto enum.
    static_assert(static_cast<int>(InvertedIndexOptions::DOCS) == BuiltinInvertedIndexPB::DOCS);
    static_assert(static_cast<int>(InvertedIndexOptions::DOCS_AND_FREQS) == BuiltinInvertedIndexPB::DOCS_AND_FREQS);
    inverted_index_meta->set_index_options(static_cast<BuiltinInvertedIndexPB::IndexOptions>(_index_options));
    RETURN_IF_ERROR(finish_freqs(wfile, inverted_index_meta->mutable_posting()));
    RETURN_IF_ERROR(finish_norms(wfile, inverted_index_meta->mutable_norms()));
    return Status::OK();
}

// Write the per-term block posting (docid + term frequency) and the doc_freq column into `posting`.
// Terms are ordered by unsigned byte comparison so posting/doc_freq ordinal i matches the presence-
// bitmap dictionary ordinal i (the dict is sorted with the same comparator).
template <LogicalType field_type>
Status BuiltinInvertedWriterImpl<field_type>::finish_freqs(WritableFile* wfile, PostingIndexPB* posting) {
    std::vector<const std::pair<const std::string, PostingList>*> ordered;
    ordered.reserve(_freq_index.size());
    for (const auto& kv : _freq_index) {
        ordered.push_back(&kv);
    }
    std::sort(ordered.begin(), ordered.end(),
              [](const auto* a, const auto* b) { return Slice(a->first).compare(Slice(b->first)) < 0; });

    BlockPostingWriter pw(wfile);
    RETURN_IF_ERROR(pw.init());
    for (uint32_t ord = 0; ord < ordered.size(); ++ord) {
        const PostingList& pl = ordered[ord]->second;
        pw.start_term(ord);
        for (size_t j = 0; j < pl.docids.size(); ++j) {
            pw.add(pl.docids[j], pl.tfs[j], _doc_len[pl.docids[j]]);
        }
        RETURN_IF_ERROR(pw.finish_term());
    }
    RETURN_IF_ERROR(pw.finish(posting->mutable_posting_block_column(), posting->mutable_posting_index_column()));

    // doc_freq: df per term (= postings size = number of docs containing the term), dict-ordinal aligned.
    return write_u32_indexed_column(
            wfile, posting->mutable_doc_freq_column(),
            [&](size_t i) { return static_cast<uint32_t>(ordered[i]->second.docids.size()); }, ordered.size());
}

// Write the per-row document-length column and the segment-wide sum_len into `norms`.
template <LogicalType field_type>
Status BuiltinInvertedWriterImpl<field_type>::finish_norms(WritableFile* wfile, NormsPB* norms) {
    RETURN_IF_ERROR(write_u32_indexed_column(
            wfile, norms->mutable_doc_len_column(), [&](size_t i) { return _doc_len[i]; }, _doc_len.size()));
    norms->set_sum_len(_sum_len);
    return Status::OK();
}

Status BuiltinInvertedWriter::create(const TypeInfoPtr& typeinfo, TabletIndex* tablet_index,
                                     std::unique_ptr<InvertedWriter>* res) {
    auto gram_num = get_gram_num_from_properties(tablet_index->index_properties());

    std::unique_ptr<BitmapIndexWriter> writer;
    RETURN_IF_ERROR(BitmapIndexWriter::create(typeinfo, &writer, gram_num));
    writer->set_dictionary_compression(CompressionTypePB::ZSTD);

    LogicalType type = typeinfo->type();
    switch (type) {
    case LogicalType::TYPE_CHAR: {
        *res = std::make_unique<BuiltinInvertedWriterImpl<LogicalType::TYPE_CHAR>>(writer, tablet_index);
        break;
    }
    case LogicalType::TYPE_VARCHAR: {
        *res = std::make_unique<BuiltinInvertedWriterImpl<LogicalType::TYPE_VARCHAR>>(writer, tablet_index);
        break;
    }
    default:
        return Status::NotSupported(
                strings::Substitute("Unsupported type for inverted index: $0", type_to_string_v2(type)));
    }
    return Status::OK();
}

} // namespace starrocks
