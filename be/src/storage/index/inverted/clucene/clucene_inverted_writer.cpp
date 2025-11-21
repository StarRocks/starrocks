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

#include "storage/index/inverted/clucene/clucene_inverted_writer.h"

#include <utility>

#include "clucene_file_manager.h"
#include "common/status.h"
#include "fs/fs.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/inverted/clucene/clucene_file_writer.h"
#include "storage/index/inverted/clucene/clucene_fs_directory.h"
#include "storage/index/inverted/inverted_index_analyzer.h"
#include "storage/index/inverted/inverted_index_context.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/rowset/common.h"
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

    explicit CLuceneInvertedWriterImpl(const std::string& field_name,
                                       std::shared_ptr<CLuceneFileWriter> index_file_writer,
                                       const TabletIndex* inverted_index)
            : _index_file_writer(std::move(index_file_writer)),
              _field_name(std::wstring(field_name.begin(), field_name.end())),
              _inverted_index(inverted_index) {
        _parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(_inverted_index->index_properties()));
    }

    ~CLuceneInvertedWriterImpl() override {
        if (_index_writer != nullptr) {
            close_on_error();
        }
    }

    uint64_t size() const override { return _index_writer->numRamDocs(); }

    uint64_t estimate_buffer_size() const override { return _index_writer->ramSizeInBytes(); }

    uint64_t total_mem_footprint() const override { return _index_writer->ramSizeInBytes(); }

    Status init() override {
        try {
            if constexpr (is_string_type(field_type)) {
                return init_fulltext_index();
            }
            return Status::NotFound("Field type not supported");
        } catch (CLuceneError& e) {
            LOG(WARNING) << "Inverted index writer init error occurred: " << e.what();
            return Status::InternalError("Inverted index writer init error occurred");
        }
    }

    Status init_fulltext_index() {
        RETURN_IF_ERROR(init_inverted_index_context());
        RETURN_IF_ERROR(open_index_directory());
        RETURN_IF_ERROR(create_char_string_reader());

        ASSIGN_OR_RETURN(_analyzer, InvertedIndexAnalyzer::create_analyzer(_parser_type));
        if (_analyzer == nullptr) {
            return Status::InternalError(fmt::format("Inverted index analyzer could not be created for parser type: {}",
                                                     inverted_index_parser_type_to_string(_parser_type)));
        }

        RETURN_IF_ERROR(create_index_writer());

        _doc = std::make_unique<lucene::document::Document>();
        _doc->clear();

        RETURN_IF_ERROR(create_field());
        _doc->add(*_field);

        return Status::OK();
    }

    void add_values(const void* values, size_t count) override {
        if constexpr (is_string_type(field_type)) {
            auto* _val = (Slice*)values;
            for (int i = 0; i < count; ++i) {
                const char* s = _val->data;
                size_t size = _val->size;

                if (!_val->empty()) {
                    if (_parser_type == InvertedIndexParserType::PARSER_ENGLISH ||
                        _parser_type == InvertedIndexParserType::PARSER_CHINESE ||
                        _parser_type == InvertedIndexParserType::PARSER_STANDARD) {
                        _char_string_reader->init(s, size, false);
                        auto stream = _analyzer->reusableTokenStream(_field->name(), _char_string_reader.get());
                        _field->setValue(stream);
                    } else {
                        _field->setValue(const_cast<char*>(s), size);
                    }
                    _index_writer->addDocument(_doc.get());
                } else {
                    _index_writer->addNullDocument(_doc.get());
                }
                ++_val;
                _rid++;
            }
        } else {
            LOG(ERROR) << "Inverted not supported type: " << field_type;
        }
    }

    void add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
        if constexpr (is_string_type(field_type)) {
            for (int i = 0; i < count; ++i) {
                _index_writer->addNullDocument(_doc.get());
            }
        }
    }

    void write_null_bitmap(lucene::store::IndexOutput* null_bitmap_out) {
        // write null_bitmap file
        _null_bitmap.runOptimize();
        size_t size = _null_bitmap.getSizeInBytes(false);
        if (size > 0) {
            faststring buf;
            buf.resize(size);
            _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap_out->writeBytes(buf.data(), size);
        }
    }

    void close_on_error() const {
        try {
            if (_index_writer) {
                _index_writer->close();
            }
        } catch (CLuceneError& e) {
            LOG(ERROR) << "InvertedIndexWriter close_on_error failure: " << e.what();
        }
    }

    Status finish() override {
        if (_dir != nullptr) {
            try {
                // write string type values
                if constexpr (is_string_type(field_type)) {
                    auto null_bitmap_out = std::unique_ptr<lucene::store::IndexOutput>(
                            _dir->createOutput(IndexDescriptor::get_temporary_null_bitmap_file_name().data()));
                    write_null_bitmap(null_bitmap_out.get());
                } else {
                    return Status::NotSupported(
                            fmt::format("Unsupported field type {}", logical_type_to_string(field_type)));
                }
            } catch (CLuceneError& e) {
                return Status::InternalError(
                        fmt::format("Inverted index writer finish error occurred, msg: {}", e.what()));
            }
            return Status::OK();
        }
        return Status::InternalError("Inverted index writer finish error occurred: dir is nullptr");
    }

private:
    Status create_index_writer() {
        bool create_index = true;
        bool close_dir_on_shutdown = true;
        _index_writer = std::make_unique<lucene::index::IndexWriter>(_dir.get(), _analyzer.get(), create_index,
                                                                     close_dir_on_shutdown);
        _index_writer->setMaxBufferedDocs(-1);
        _index_writer->setRAMBufferSizeMB(RAMBufferSizeMB);
        _index_writer->setMaxFieldLength(MAX_FIELD_LEN);
        _index_writer->setMergeFactor(MERGE_FACTOR);
        _index_writer->setUseCompoundFile(false);
        _index_writer->setEnableCorrectTermWrite(true);
        return Status::OK();
    }

    Status init_inverted_index_context() {
        _inverted_index_ctx = std::make_shared<InvertedIndexCtx>();
        _inverted_index_ctx->setParserType(_parser_type);
        return Status::OK();
    }

    Status open_index_directory() {
        ASSIGN_OR_RETURN(_dir, _index_file_writer->open(_inverted_index));
        return Status::OK();
    }

    Status create_char_string_reader() {
        try {
            _char_string_reader = std::make_unique<lucene::util::SStringReader<char>>();
            return Status::OK();
        } catch (CLuceneError& e) {
            return Status::InternalError(fmt::format("inverted index create string reader failed: {}", e.what()));
        }
    }

    Status create_field() {
        int field_config = static_cast<int>(lucene::document::Field::STORE_NO) |
                           static_cast<int>(lucene::document::Field::INDEX_NONORMS);
        field_config |= _parser_type == InvertedIndexParserType::PARSER_NONE
                                ? static_cast<int>(lucene::document::Field::INDEX_UNTOKENIZED)
                                : static_cast<int>(lucene::document::Field::INDEX_TOKENIZED);
        _field = new lucene::document::Field(_field_name.c_str(), field_config);
        _field->setOmitTermFreqAndPositions(
                get_omit_term_freq_and_position_from_properties(_inverted_index->index_properties()) ==
                INVERTED_INDEX_OMIT_TERM_FREQ_AND_POSITION_YES);
        return Status::OK();
    }

    rowid_t _rid = 0;
    roaring::Roaring _null_bitmap;

    std::unique_ptr<lucene::document::Document> _doc{};
    lucene::document::Field* _field{};
    std::unique_ptr<lucene::index::IndexWriter> _index_writer{};
    std::unique_ptr<lucene::analysis::Analyzer> _analyzer{};
    std::unique_ptr<lucene::util::Reader> _char_string_reader{};

    InvertedIndexParserType _parser_type;

    std::shared_ptr<CLuceneFileWriter> _index_file_writer;

    std::wstring _field_name;
    const TabletIndex* _inverted_index;
    std::shared_ptr<InvertedIndexCtx> _inverted_index_ctx;

    std::shared_ptr<StarRocksFSDirectory> _dir = nullptr;
};

CLuceneInvertedWriter::CLuceneInvertedWriter() = default;

CLuceneInvertedWriter::~CLuceneInvertedWriter() = default;

Status CLuceneInvertedWriter::create(const TypeInfoPtr& typeinfo, const std::string& field_name,
                                     const std::string& directory, TabletIndex* tablet_index,
                                     std::unique_ptr<InvertedWriter>* res) {
    LogicalType type = typeinfo->type();
    if (type == LogicalType::TYPE_ARRAY) {
        type = typeinfo->type();
    }

    ASSIGN_OR_RETURN(auto index_file_writer,
                     CLuceneFileManager::getInstance().get_or_create_clucene_file_writer(directory));

    switch (type) {
    case LogicalType::TYPE_CHAR: {
        *res = std::make_unique<CLuceneInvertedWriterImpl<LogicalType::TYPE_CHAR>>(field_name, index_file_writer,
                                                                                   tablet_index);
        break;
    }
    case LogicalType::TYPE_VARCHAR: {
        *res = std::make_unique<CLuceneInvertedWriterImpl<LogicalType::TYPE_VARCHAR>>(field_name, index_file_writer,
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