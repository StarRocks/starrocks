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

#include "exec/dictionary_cache_writer.h"

#include "exec/tablet_info.h"
#include "serde/protobuf_serde.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/block_compression.h"

namespace starrocks {

class RefreshDictionaryCacheTask final : public Runnable {
public:
    RefreshDictionaryCacheTask(DictionaryCacheWriter* writer, ChunkUniquePtr& chunk)
            : _writer(writer), _chunk(std::move(chunk)) {}

    ~RefreshDictionaryCacheTask() override = default;

    void run() override { _writer->sync_dictionary_cache(_chunk.get()); }

private:
    DictionaryCacheWriter* _writer;
    ChunkUniquePtr _chunk;
};

DictionaryCacheWriter::DictionaryCacheWriter(const TDictionaryCacheSink& t_dictionary_cache_sink, RuntimeState* state,
                                             starrocks::pipeline::FragmentContext* fragment_ctx)
        : _t_dictionary_cache_sink(t_dictionary_cache_sink), _state(state), _fragment_ctx(fragment_ctx) {}

Status DictionaryCacheWriter::prepare() {
    if (_is_prepared.load(std::memory_order_acquire)) {
        return Status::OK();
    }

    const auto& nodes = _t_dictionary_cache_sink.nodes;
    if (nodes.size() == 0) {
        auto st = Status::RuntimeError("no avaiable BE nodes for refreshing dictionary cache");
        return st;
    }

    _is_prepared.store(true, std::memory_order_release);

    return Status::OK();
}

Status DictionaryCacheWriter::append_chunk(ChunkPtr chunk, std::atomic_bool* terminate_flag /*finish or cancel*/) {
    if (chunk != nullptr) {
        if (chunk->num_rows() == 0) {
            return Status::OK();
        }

        RETURN_IF_ERROR(DictionaryCacheWriter::ChunkUtil::check_chunk_has_null(*chunk.get()));

        if (_buffer_chunk == nullptr) {
            chunk->reset_slot_id_to_index();
            for (size_t i = 0; i < chunk->num_columns(); i++) {
                chunk->set_slot_id_to_index(i + 1, i);
            }
            _buffer_chunk = chunk->clone_empty_with_slot();
        }
        DCHECK(_buffer_chunk != nullptr);
        _buffer_chunk->append(*chunk);
    }

    if (_buffer_chunk != nullptr &&
        (_buffer_chunk->container_memory_usage() > kChunkBufferLimit || chunk == nullptr /* end of appending*/)) {
        DCHECK(_immutable_buffer_chunk == nullptr);
        _immutable_buffer_chunk.swap(_buffer_chunk);
    }

    if (_immutable_buffer_chunk != nullptr) {
        RETURN_IF_ERROR(_submit());
        DCHECK(_buffer_chunk == nullptr && _immutable_buffer_chunk == nullptr);
    }

    if (terminate_flag != nullptr) {
        terminate_flag->store(true, std::memory_order_release);
    }

    return Status::OK();
}

Status DictionaryCacheWriter::_submit() {
    _num_pending_tasks.fetch_add(1, std::memory_order_release);
    std::shared_ptr<Runnable> task(std::make_shared<RefreshDictionaryCacheTask>(this, _immutable_buffer_chunk));
    auto st = ExecEnv::GetInstance()->dictionary_cache_pool()->submit(task);
    if (!st.ok()) {
        _num_pending_tasks.fetch_sub(1, std::memory_order_release);
        DCHECK(_num_pending_tasks.load(std::memory_order_acquire) >= 0);
        LOG(WARNING) << "refresh dictionary cache task submit failed, " << st.message();
    }
    return st;
}

void DictionaryCacheWriter::sync_dictionary_cache(const Chunk* chunk) {
    Status st = Status::OK();

    // construct closures
    std::vector<RefCountClosure<PProcessDictionaryCacheResult>*> closures;
    closures.resize(_t_dictionary_cache_sink.nodes.size());
    for (auto& closure : closures) {
        closure = new RefCountClosure<PProcessDictionaryCacheResult>();
        closure->ref();
    }

    DeferOp op([&]() {
        _num_pending_tasks.fetch_sub(1, std::memory_order_release);
        DCHECK(_num_pending_tasks.load(std::memory_order_acquire) >= 0);

        for (auto& closure : closures) {
            if (closure->unref()) {
                delete closure;
            }
        }
    });

    auto schema = std::make_shared<OlapTableSchemaParam>();
    (void)schema->init(_t_dictionary_cache_sink.schema);
    auto pschema = std::make_unique<POlapTableSchemaParam>();
    schema->to_protobuf(pschema.get());

    while (true) {
        std::unique_ptr<ChunkPB> pchunk = std::make_unique<ChunkPB>();
        st = ChunkUtil::compress_and_serialize_chunk(chunk, pchunk.get());
        if (!st.ok()) {
            break;
        }

        st = _send_request(pchunk.get(), pschema.get(), closures);
        if (!st.ok()) {
            break;
        }

        st = _wait_response(closures);
        break;
    }

    if (!st.ok()) {
        std::stringstream ss;
        ss << "RPC failed when refreshing dictionary cache, " << st.message();
        LOG(WARNING) << ss.str();
        // manually cancel fragment context, because sync_dictionary_cache is not
        // in pipeline driver executor thread
        _fragment_ctx->cancel(Status::InternalError(ss.str()));
    }
}

Status DictionaryCacheWriter::_send_request(ChunkPB* pchunk, POlapTableSchemaParam* pschema,
                                            std::vector<RefCountClosure<PProcessDictionaryCacheResult>*>& closures) {
    const auto& nodes = _t_dictionary_cache_sink.nodes;
    DCHECK(closures.size() == nodes.size());

    for (size_t i = 0; i < nodes.size(); ++i) {
        PProcessDictionaryCacheRequest request;
        request.set_allocated_chunk(pchunk);
        request.set_dict_id(_t_dictionary_cache_sink.dictionary_id);
        request.set_txn_id(_t_dictionary_cache_sink.txn_id);
        request.set_allocated_schema(pschema);
        request.set_memory_limit(_t_dictionary_cache_sink.memory_limit);
        request.set_key_size(_t_dictionary_cache_sink.key_size);
        request.set_type(PProcessDictionaryCacheRequestType::REFRESH);

        auto& closure = closures[i];
        closure->ref();
        closure->cntl.set_timeout_ms(config::dictionary_cache_refresh_timeout_ms);
        closure->cntl.ignore_eovercrowded();

        auto res = HttpBrpcStubCache::getInstance()->get_http_stub(nodes[i]);
        if (!res.ok()) {
            request.release_chunk();
            request.release_schema();
            LOG(WARNING) << "create brpc stub failed, " << res.status().message();
            return res.status();
        }
        res.value()->process_dictionary_cache(&closure->cntl, &request, &closure->result, closure);

        request.release_chunk();
        request.release_schema();
    }
    return Status::OK();
}

Status DictionaryCacheWriter::_wait_response(std::vector<RefCountClosure<PProcessDictionaryCacheResult>*>& closures) {
    Status st = Status::OK();
    for (size_t i = 0; i < closures.size(); ++i) {
        auto& closure = closures[i];
        closure->join();
        if (closure->cntl.Failed()) {
            st = Status::InternalError(closure->cntl.ErrorText());
            LOG(WARNING) << "Failed to send rpc "
                         << " err=" << st;
            return st;
        }
        st = closure->result.status();
        if (!st.ok()) {
            LOG(WARNING) << "RPC failed "
                         << " err=" << st;
            return st;
        }
    }
    return st;
}

Status DictionaryCacheWriter::set_finishing() {
    // trigger the last request and set finish
    return append_chunk(nullptr, &_is_finished);
}

bool DictionaryCacheWriter::need_input() {
    return _num_pending_tasks.load(std::memory_order_acquire) < kMaxMemoryUsageLimit / kChunkBufferLimit;
}

bool DictionaryCacheWriter::is_finished() {
    return (_is_finished.load(std::memory_order_acquire) || _is_cancelled.load(std::memory_order_acquire)) &&
           _num_pending_tasks.load(std::memory_order_acquire) == 0;
}

Status DictionaryCacheWriter::cancel() {
    // trigger the last request and set cancel
    return append_chunk(nullptr, &_is_cancelled);
}

Status DictionaryCacheWriter::ChunkUtil::compress_and_serialize_chunk(const Chunk* src, ChunkPB* dst) {
    VLOG_ROW << "serializing " << src->num_rows() << " rows";

    {
        StatusOr<ChunkPB> res = Status::OK();
        TRY_CATCH_BAD_ALLOC(res = serde::ProtobufChunkSerde::serialize(*src));
        if (!res.ok()) {
            LOG(WARNING) << "serialize chunk failed when refreshing dictionary cache, " << res.status().message();
            return res.status();
        }
        res->Swap(dst);
    }
    DCHECK(dst->has_uncompressed_size());
    DCHECK_EQ(dst->uncompressed_size(), dst->data().size());

    size_t uncompressed_size = dst->uncompressed_size();

    BlockCompressionCodec* compress_codec;
    raw::RawString compression_scratch;

    // use ZSTD as default compression method for chunkPB serialization, not configurable
    // ZSTD usually is a best compression method in practice
    (void)get_block_compression_codec(CompressionTypePB::ZSTD,
                                      const_cast<const BlockCompressionCodec**>(&compress_codec));

    DCHECK(compress_codec != nullptr);

    // try compress the ChunkPB data
    if (uncompressed_size > 0) {
        // must be true for ZSTD
        DCHECK(use_compression_pool(compress_codec->type()));

        Slice compressed_slice;
        Slice input(dst->data());
        auto st = compress_codec->compress(input, &compressed_slice, true, uncompressed_size, nullptr,
                                           &compression_scratch);
        if (!st.ok()) {
            LOG(WARNING) << "compress chunk failed when refreshing dictionary cache, " << st.message();
            return st;
        }

        double compress_ratio = (static_cast<double>(uncompressed_size)) / compression_scratch.size();
        if (LIKELY(compress_ratio > config::rpc_compress_ratio_threshold)) {
            VLOG_ROW << "uncompressed size: " << uncompressed_size
                     << ", compressed size: " << compression_scratch.size();

            dst->mutable_data()->swap(reinterpret_cast<std::string&>(compression_scratch));
            dst->set_compress_type(CompressionTypePB::ZSTD);
        }
    }

    return Status::OK();
}

Status DictionaryCacheWriter::ChunkUtil::uncompress_and_deserialize_chunk(const ChunkPB& pchunk, Chunk& chunk,
                                                                          faststring* uncompressed_buffer,
                                                                          const OlapTableSchemaParam* schema) {
    // build chunk meta
    auto row_desc = std::make_unique<RowDescriptor>(schema->tuple_desc(), false);
    StatusOr<serde::ProtobufChunkMeta> res = serde::build_protobuf_chunk_meta(*row_desc, pchunk);
    if (!res.ok()) {
        return res.status();
    }
    auto chunk_meta = std::move(res).value();

    // uncompress and deserialize
    if (pchunk.compress_type() == CompressionTypePB::NO_COMPRESSION) {
        TRY_CATCH_BAD_ALLOC({
            serde::ProtobufChunkDeserializer des(chunk_meta);
            StatusOr<Chunk> res = des.deserialize(pchunk.data());
            if (!res.ok()) return res.status();
            chunk = std::move(res).value();
        });
    } else {
        size_t uncompressed_size = 0;
        {
            const BlockCompressionCodec* codec = nullptr;
            (void)get_block_compression_codec(CompressionTypePB::ZSTD, &codec);
            uncompressed_size = pchunk.uncompressed_size();
            TRY_CATCH_BAD_ALLOC(uncompressed_buffer->resize(uncompressed_size));
            Slice output{uncompressed_buffer->data(), uncompressed_size};
            RETURN_IF_ERROR(codec->decompress(pchunk.data(), &output));
        }
        {
            TRY_CATCH_BAD_ALLOC({
                std::string_view buff(reinterpret_cast<const char*>(uncompressed_buffer->data()), uncompressed_size);
                serde::ProtobufChunkDeserializer des(chunk_meta);
                StatusOr<Chunk> res = Status::OK();
                TRY_CATCH_BAD_ALLOC(res = des.deserialize(buff));
                if (!res.ok()) return res.status();
                chunk = std::move(res).value();
            });
        }
    }
    return Status::OK();
}

Status DictionaryCacheWriter::ChunkUtil::check_chunk_has_null(const Chunk& chunk) {
    for (const auto& column : chunk.columns()) {
        if (column->has_null()) {
            return Status::InternalError("chunk has column with null value");
        }
    }
    return Status::OK();
}

} // namespace starrocks
