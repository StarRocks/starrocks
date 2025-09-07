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

#include <vector>

#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/Misc.h>
#include <fmt/format.h>

#include <boost/locale/encoding_utf.hpp>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "storage/index/inverted/builtin/builtin_simple_analyzer.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/rowset/bitmap_index_writer.h" 
#include "storage/tablet_index.h"
#include "storage/type_traits.h"
#include "types/logical_type.h"
#include "util/threadpool.h"

namespace starrocks {

struct BuiltinInvertedWriterContext {
    lucene::analysis::Analyzer* analyzer;
    lucene::util::StringReader* char_string_reader;
    SimpleAnalyzer* builtin_analyzer;
    BitmapIndexWriter* writer;
};

template <LogicalType field_type>
class BuiltinInvertedWriterImpl : public BuiltinInvertedWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    explicit BuiltinInvertedWriterImpl(const TypeInfoPtr& typeinfo, const TabletIndex* inverted_index)
        : _typeinfo(typeinfo) {
        static_assert(field_type == TYPE_CHAR || field_type == TYPE_VARCHAR);
        _parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(inverted_index->index_properties()));
        // must in tokenize mode
        DCHECK(!(_parser_type == InvertedIndexParserType::PARSER_UNKNOWN ||
                 _parser_type == InvertedIndexParserType::PARSER_UNKNOWN));

        _dop = config::builtin_inverted_writer_dop > 1 ? config::builtin_inverted_writer_dop : 1;

        _builtin_writers.resize(_dop);
        _analyzers.resize(_dop);
        _char_string_readers.resize(_dop);
        _builtin_analyzers.resize(_dop);
    }

    Status init() override;

    void add_values(const void* values, size_t count) override;

    void _do_add_values(const void* values, size_t count, const BuiltinInvertedWriterContext* context);

    void add_nulls(uint32_t count) override { CHECK(false); }

    Status finish(WritableFile* wfile, ColumnMetaPB* meta) override;

    uint64_t size() const override;

private:
    std::vector<std::unique_ptr<BitmapIndexWriter>> _builtin_writers;

    std::vector<std::unique_ptr<lucene::analysis::Analyzer>> _analyzers;
    std::vector<std::unique_ptr<lucene::util::StringReader>> _char_string_readers;

    std::vector<std::unique_ptr<SimpleAnalyzer>> _builtin_analyzers;

    InvertedIndexParserType _parser_type;
    TypeInfoPtr _typeinfo;

    std::unique_ptr<ThreadPool> _thread_pool;
    std::unique_ptr<ThreadPoolToken> _thread_pool_token;

    int _dop;
    size_t _rid = 0;
};

template <LogicalType field_type>
Status BuiltinInvertedWriterImpl<field_type>::init() {
    for (int i = 0; i < _dop; ++i) {
        std::unique_ptr<BitmapIndexWriter> writer;
        RETURN_IF_ERROR(BitmapIndexWriter::create(_typeinfo, &writer));
        _builtin_writers[i] = std::move(writer);
    }

    // init tokenizer relative context
    for (int i = 0; i < _dop; ++i) {
        _char_string_readers[i] = std::make_unique<lucene::util::StringReader>(L"");
        if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
            _analyzers[i] = std::make_unique<lucene::analysis::standard::StandardAnalyzer>();
        } else if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            _analyzers[i] = std::make_unique<lucene::analysis::SimpleAnalyzer>();
        } else if (_parser_type == InvertedIndexParserType::PARSER_CHINESE) {
            auto chinese_analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
            chinese_analyzer->setLanguage(L"cjk");
            _analyzers[i].reset(chinese_analyzer);
        } else if (_parser_type == InvertedIndexParserType::PARSER_BUILTIN_ENGLISH) {
            _builtin_analyzers[i] = std::make_unique<SimpleAnalyzer>();
        } else {
            // ANALYSER_NOT_SET, ANALYSER_NONE use default SimpleAnalyzer
            _analyzers[i] = std::make_unique<lucene::analysis::SimpleAnalyzer>();
        }
    }

    if (_dop > 1) {
        // build thread pool
        ThreadPoolBuilder builder("builtin_inverted_writer");
        builder.set_min_threads(_dop);
        builder.set_max_threads(_dop);
        builder.set_max_queue_size(std::numeric_limits<int>::max());
        builder.set_idle_timeout(MonoDelta::FromMilliseconds(60000));
        RETURN_IF_ERROR(builder.build(&_thread_pool));
        _thread_pool_token = _thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    }
    return Status::OK();
}

template <LogicalType field_type>
void BuiltinInvertedWriterImpl<field_type>::add_values(const void* values, size_t count) {
    if (count == 0) {
        return;
    }

    if (_dop == 1) {
        BuiltinInvertedWriterContext context;
        context.analyzer = _analyzers[0].get();
        context.char_string_reader = _char_string_readers[0].get();
        context.builtin_analyzer = _builtin_analyzers[0].get();
        context.writer = _builtin_writers[0].get();
        context.writer->reset_rowid(_rid);

        _do_add_values(values, count, &context);
        _rid += count;
        return;
    }

    int dop = std::min(_dop, static_cast<int>(count));
    int group_size = count / dop;
    for (int i = 0; i < dop; ++i) {
        int start = i * group_size;
        int end = (i + 1) * group_size;
        if (i == dop - 1) {
            end = count;
        }

        BuiltinInvertedWriterContext context;
        context.analyzer = _analyzers[i].get();
        context.char_string_reader = _char_string_readers[i].get();
        context.builtin_analyzer = _builtin_analyzers[i].get();
        context.writer = _builtin_writers[i].get();
        context.writer->reset_rowid(_rid + start);

        const void* val = static_cast<const CppType*>(values) + start;
        size_t cnt = end - start;

        auto st = _thread_pool_token->submit_func([this, val, cnt, context]() {
            _do_add_values(val, cnt, &context);
        });

        if (!st.ok()) {
            CHECK(false) << "submit_func failed";
        }
    }
    _thread_pool_token->wait();
    _rid += count;
}

template <LogicalType field_type>
void BuiltinInvertedWriterImpl<field_type>::_do_add_values(const void* values, size_t count, const BuiltinInvertedWriterContext* context) {
    auto* val = (Slice*)values;
    for (int i = 0; i < count; ++i) {
        const char* s = val->data;
        size_t size = val->size;
        if (_parser_type == InvertedIndexParserType::PARSER_BUILTIN_ENGLISH) {
            std::string mutable_text(val->data, val->size);
            std::vector<SliceToken> tokens;
            context->builtin_analyzer->tokenize(mutable_text.data(), mutable_text.length(), tokens);
            for (const auto& token : tokens) {
                context->writer->add_value_with_current_rowid((void*) &(token.text));
            }
        } else {
            // Data in Slice does not contained any null-terminated. Any api in Boost/std
            // which write the result into a given memory area does not fit in this case.
            // So we still use boost::locale::conv::utf_to_utf<TCHAR> to construct a new
            // wstring in every loop for the correctness.
            std::wstring tchar = boost::locale::conv::utf_to_utf<TCHAR>(s, s + size);
    
            context->char_string_reader->init(tchar.c_str(), tchar.size(), false);
            auto stream = context->analyzer->reusableTokenStream(L"", context->char_string_reader);
            lucene::analysis::Token token;
            while (stream->next(&token)) {
                if (token.termLength() != 0) {
                    std::string str = boost::locale::conv::utf_to_utf<char>(token.termBuffer(), token.termBuffer() + token.termLength());
                    Slice s(str);
                    context->writer->add_value_with_current_rowid((void*) &s);
                }
            }
        }

        ++val;
        context->writer->incre_rowid();
    }
}

template <LogicalType field_type>
Status BuiltinInvertedWriterImpl<field_type>::finish(WritableFile* wfile, ColumnMetaPB* meta) {
    std::unique_ptr<BitmapIndexWriter> merged_writer = std::move(_builtin_writers[0]);
    if (_dop > 1) {
        std::vector<std::unique_ptr<BitmapIndexWriter>> remain_writers;
        remain_writers.reserve(_dop - 1);
        for (int i = 1; i < _dop; ++i) {
            remain_writers.push_back(std::move(_builtin_writers[i]));
        }
        merged_writer->merge_index_from(std::move(remain_writers));
    }
    merged_writer->reset_rowid(_rid);
    
    ColumnIndexMetaPB* index_meta = meta->add_indexes();
    index_meta->set_type(BUILTIN_INVERTED_INDEX);
    BuiltinInvertedIndexPB* inverted_index_meta = index_meta->mutable_builtin_inverted_index();
    BitmapIndexPB* bitmap_index_meta = inverted_index_meta->mutable_bitmap_index();
    return merged_writer->finish(wfile, bitmap_index_meta);
}

template <LogicalType field_type>
uint64_t BuiltinInvertedWriterImpl<field_type>::size() const {
    uint64_t total_size = 0;
    for (int i = 0; i < _dop; ++i) {
        total_size += _builtin_writers[i]->size();
    }
    return total_size;
}

Status BuiltinInvertedWriter::create(const TypeInfoPtr& typeinfo, TabletIndex* tablet_index, std::unique_ptr<InvertedWriter>* res) {
    LogicalType type = typeinfo->type();
    switch (type) {
    case LogicalType::TYPE_CHAR: {
        *res = std::make_unique<BuiltinInvertedWriterImpl<LogicalType::TYPE_CHAR>>(typeinfo, tablet_index);
        break;
    }
    case LogicalType::TYPE_VARCHAR: {
        *res = std::make_unique<BuiltinInvertedWriterImpl<LogicalType::TYPE_VARCHAR>>(typeinfo, tablet_index);
        break;
    }
    default:
        return Status::NotSupported(
                strings::Substitute("Unsupported type for inverted index: $0", type_to_string_v2(type)));
    }
    return Status::OK();
}

} // namespace starrocks