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
#include <fmt/format.h>

#include <vector>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/tokenizer/builtin_simple_analyzer.h"
#include "storage/index/inverted/tokenizer/chinese_tokenizer.h"
#include "storage/index/inverted/tokenizer/standard_tokenizer.h"
#include "storage/index/inverted/tokenizer/tokenizer.h"
#include "storage/index/inverted/tokenizer/tokenizer_factory.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/tablet_index.h"
#include "storage/type_traits.h"
#include "types/logical_type.h"

namespace starrocks {

template <LogicalType field_type>
class BuiltinInvertedWriterImpl : public BuiltinInvertedWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    explicit BuiltinInvertedWriterImpl(std::unique_ptr<BitmapIndexWriter>& writer, const TabletIndex* inverted_index)
            : _builtin_writer(std::move(writer)) {
        static_assert(field_type == TYPE_CHAR || field_type == TYPE_VARCHAR);
        _parser_type = get_inverted_index_parser_type_from_string(
                get_parser_string_from_properties(inverted_index->index_properties()));
        DCHECK(_parser_type != InvertedIndexParserType::PARSER_UNKNOWN);
    }

    Status init() override;

    void add_values(const void* values, size_t count) override;

    void add_nulls(uint32_t count) override { _builtin_writer->add_nulls(count); }

    Status finish(WritableFile* wfile, ColumnMetaPB* meta) override;

    uint64_t size() const override { return _builtin_writer->size(); }

private:
    std::unique_ptr<Tokenizer> _tokenizer;
    std::unique_ptr<BitmapIndexWriter> _builtin_writer;

    InvertedIndexParserType _parser_type;
};

template <LogicalType field_type>
Status BuiltinInvertedWriterImpl<field_type>::init() {
    _tokenizer = TokenizerFactory::create(_parser_type);
    if (_tokenizer == nullptr) {
        return Status::InternalError(fmt::format("create tokenizer failed, parser type: {}",
                                                 inverted_index_parser_type_to_string(_parser_type)));
    }
    return Status::OK();
}

template <LogicalType field_type>
void BuiltinInvertedWriterImpl<field_type>::add_values(const void* values, size_t count) {
    const Slice* val = static_cast<const Slice*>(values);
    for (int i = 0; i < count; ++i) {
        const auto st = _tokenizer->tokenize(val);
        if (!st.ok()) {
            LOG(WARNING) << "tokenize text failed, text: " << val->to_string() << ", error: " << st.status();
            continue;
        }
        for (std::vector<SliceToken> tokens = std::move(st).value(); const auto& token : tokens) {
            _builtin_writer->add_value_with_current_rowid(&token.text);
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
    return _builtin_writer->finish(wfile, bitmap_index_meta);
}

Status BuiltinInvertedWriter::create(const TypeInfoPtr& typeinfo, TabletIndex* tablet_index,
                                     std::unique_ptr<InvertedWriter>* res) {
    std::unique_ptr<BitmapIndexWriter> writer;
    RETURN_IF_ERROR(BitmapIndexWriter::create(typeinfo, &writer));
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