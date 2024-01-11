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

#include "clucene_inverted_writer.h"

#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/Misc.h>
#include <fmt/format.h>

#include <boost/locale/encoding_utf.hpp>

#include "common/status.h"
#include "storage/inverted/index_descriptor.hpp"
#include "types/logical_type.h"
#include "util/faststring.h"

namespace starrocks {

#define FINALIZE_OUTPUT(x) \
    if (x != nullptr) {    \
        x->close();        \
        _CLDELETE(x);      \
    }
#define FINALLY_FINALIZE_OUTPUT(x) \
    try {                          \
        FINALIZE_OUTPUT(x)         \
    } catch (...) {                \
    }

template <LogicalType field_type>
class CLuceneInvertedWriterImpl : public CLuceneInvertedWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    explicit CLuceneInvertedWriterImpl(const std::string& field_name, const std::string& directory,
                                       const TabletIndex* inverted_index)
            : _directory(std::move(directory)), _inverted_index(inverted_index) {
        _parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_inverted_index->index_properties()));
        _field_name = std::wstring(field_name.begin(), field_name.end());
    }

    uint64_t size() const override { return _index_writer->numRamDocs(); }

    uint64_t estimate_buffer_size() const override { return _index_writer->ramSizeInBytes(); }

    uint64_t total_mem_footprint() const override { return _index_writer->ramSizeInBytes(); }

    Status init() override {
        try {
            if constexpr (is_string_type(field_type)) {
                return init_fulltext_index();
            }
            return Status::InvertedIndexFileNotFound("Field type not supported");
        } catch (CLuceneError e) {
            LOG(WARNING) << "Inverted index writer init error occurred: " << e.what();
            return Status::InvertedIndexCluceneError("Inverted index writer init error occurred");
        }
    }

    Status init_fulltext_index() {
        bool create = true;

        LOG(INFO) << "inverted index path: " << _directory;

        if (lucene::index::IndexReader::indexExists(_directory.c_str())) {
            create = false;
            if (lucene::index::IndexReader::isLocked(_directory.c_str())) {
                LOG(WARNING) << ("Lucene Index was locked... unlocking it.\n");
                lucene::index::IndexReader::unlock(_directory.c_str());
            }
        }

        _char_string_reader = std::make_unique<lucene::util::StringReader>(L"");

        _doc = std::make_unique<lucene::document::Document>();

        if (_parser_type == InvertedIndexParserType::PARSER_STANDARD) {
            _analyzer = std::make_unique<lucene::analysis::standard::StandardAnalyzer>();
        } else if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH) {
            _analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer>();
        } else if (_parser_type == InvertedIndexParserType::PARSER_CHINESE) {
            auto chinese_analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
            chinese_analyzer->setLanguage(L"cjk");
            _analyzer.reset(chinese_analyzer);
        } else {
            // ANALYSER_NOT_SET, ANALYSER_NONE use default SimpleAnalyzer
            _analyzer = std::make_unique<lucene::analysis::SimpleAnalyzer>();
        }
        _index_writer = std::make_unique<lucene::index::IndexWriter>(_directory.c_str(), _analyzer.get(), create);
        _index_writer->setMaxBufferedDocs(MAX_BUFFER_DOCS);
        _index_writer->setRAMBufferSizeMB(RAMBufferSizeMB);
        _index_writer->setMaxFieldLength(MAX_FIELD_LEN);
        _index_writer->setMergeFactor(MERGE_FACTOR);
        _index_writer->setUseCompoundFile(false);
        _doc->clear();

        int field_config = int(lucene::document::Field::STORE_NO);
        if (_parser_type == InvertedIndexParserType::PARSER_NONE) {
            field_config |= int(lucene::document::Field::INDEX_UNTOKENIZED);
        } else {
            field_config |= int(lucene::document::Field::INDEX_TOKENIZED);
        }
        _field = new lucene::document::Field(_field_name.c_str(), field_config);
        _doc->add(*_field);
        return Status::OK();
    }

    void add_values(const void* values, size_t count) override {
        if constexpr (is_string_type(field_type)) {
            if (_field == nullptr || _index_writer == nullptr) {
                LOG(ERROR) << "field or index writer is null in inverted index writer.";
            }
            auto* _val = (Slice*)values;
            for (int i = 0; i < count; ++i) {
                new_fulltext_field(_val->data, _val->size);
                _index_writer->addDocument(_doc.get());
                ++_val;
                _rid++;
            }
        } else {
            LOG(ERROR) << "Inverted not supported type: " << field_type;
        }
    }

    void new_fulltext_field(const char* field_value_data, size_t field_value_size) {
        if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH ||
            _parser_type == InvertedIndexParserType::PARSER_CHINESE) {
            new_char_token_stream(field_value_data, field_value_size, _field);
        } else {
            new_field_value(field_value_data, field_value_size, _field);
        }
    }

    void new_char_token_stream(const char* s, size_t len, lucene::document::Field* field) {
        auto tchar = boost::locale::conv::utf_to_utf<TCHAR>(s, s + len);
        _char_string_reader->init(tchar.c_str(), len, true);
        auto stream = _analyzer->reusableTokenStream(field->name(), _char_string_reader.get());
        field->setValue(stream);
    }

    void new_field_value(const char* s, size_t len, lucene::document::Field* field) {
        auto field_value = boost::locale::conv::utf_to_utf<TCHAR>(s, s + len);
        field->setValue(field_value.data(), true);
    }

    void add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
        if constexpr (is_string_type(field_type)) {
            if (_field == nullptr || _index_writer == nullptr) {
                LOG(ERROR) << "field or index writer is null in inverted index writer.";
            }

            for (int i = 0; i < count; ++i) {
                new_fulltext_field(empty_value.c_str(), 0);
                _index_writer->addDocument(_doc.get());
            }
        }
    }

    void write_null_bitmap(lucene::store::IndexOutput* null_bitmap_out, lucene::store::Directory* dir) {
        // write null_bitmap file
        _null_bitmap.runOptimize();
        size_t size = _null_bitmap.getSizeInBytes(false);
        if (size > 0) {
            null_bitmap_out = dir->createOutput(IndexDescriptor::get_temporary_null_bitmap_file_name().c_str());
            faststring buf;
            buf.resize(size);
            _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap_out->writeBytes(reinterpret_cast<uint8_t*>(buf.data()), size);
            FINALIZE_OUTPUT(null_bitmap_out)
        }
    }

    void close() {
        if (_index_writer) {
            _index_writer->close();
        }
    }

    Status finish() override {
        lucene::store::Directory* dir = nullptr;
        lucene::store::IndexOutput* null_bitmap_out = nullptr;
        lucene::store::IndexOutput* data_out = nullptr;
        lucene::store::IndexOutput* index_out = nullptr;
        lucene::store::IndexOutput* meta_out = nullptr;
        try {
            // write string type values
            if constexpr (is_string_type(field_type)) {
                dir = _index_writer->getDirectory();
                write_null_bitmap(null_bitmap_out, dir);
                close();
            }
        } catch (CLuceneError& e) {
            FINALLY_FINALIZE_OUTPUT(null_bitmap_out)
            FINALLY_FINALIZE_OUTPUT(meta_out)
            FINALLY_FINALIZE_OUTPUT(data_out)
            FINALLY_FINALIZE_OUTPUT(index_out)
            FINALLY_FINALIZE_OUTPUT(dir)
            LOG(WARNING) << "Inverted index writer finish error occurred: " << e.what();
            return Status::InvertedIndexCluceneError("Inverted index writer finish error occurred");
        }

        return Status::OK();
    }

private:
    rowid_t _rid = 0;
    roaring::Roaring _null_bitmap;

    std::unique_ptr<lucene::document::Document> _doc{};
    lucene::document::Field* _field{};
    std::unique_ptr<lucene::index::IndexWriter> _index_writer{};
    std::unique_ptr<lucene::analysis::Analyzer> _analyzer{};
    std::unique_ptr<lucene::util::StringReader> _char_string_reader{};
    std::string _directory;
    const TabletIndex* _index_meta;
    InvertedIndexParserType _parser_type;
    std::wstring _field_name;
    const TabletIndex* _inverted_index;
};

Status CLuceneInvertedWriter::create(const TypeInfoPtr& typeinfo, const std::string& field_name,
                                     const std::string& directory, TabletIndex* tablet_index,
                                     std::unique_ptr<InvertedWriter>* res) {
    LogicalType type = typeinfo->type();
    if (type == LogicalType::TYPE_ARRAY) {
        type = typeinfo->type();
    }

    switch (type) {
    case LogicalType::TYPE_CHAR: {
        *res = std::make_unique<CLuceneInvertedWriterImpl<LogicalType::TYPE_CHAR>>(field_name, directory, tablet_index);
        break;
    }
    case LogicalType::TYPE_VARCHAR: {
        *res = std::make_unique<CLuceneInvertedWriterImpl<LogicalType::TYPE_VARCHAR>>(field_name, directory,
                                                                                      tablet_index);
        break;
    }
    default:
        return Status::NotSupported(
                strings::Substitute("Unsupported type for inverted index: $0", type_to_string_v2(type)));
    }
    return Status::OK();
}

} // namespace starrocks