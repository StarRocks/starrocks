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

#include "connector/starrocks/starrocks_connector.h"

#include <arrow/compute/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/server.h>
#include <brpc/controller.h>
#include <fmt/format.h>

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/statusor.h"
#include "base/uid_util.h"
#include "base/utility/arrow_utils.h"
#include "column/arrow/arrow_to_starrocks_converter.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/brpc/brpc_stub_cache.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/serde/protobuf_chunk_serde.h"
#include "runtime/service_contexts.h"

namespace starrocks::connector {

namespace {

#define RETURN_IF_ARROW_ERROR(expr)    \
    do {                               \
        auto status = to_status(expr); \
        if (!status.ok()) {            \
            return status;             \
        }                              \
    } while (0)

#define ARROW_ASSIGN_OR_RETURN(lhs, rhs) ARROW_ASSIGN_OR_RETURN_IMPL(VARNAME_LINENUM(value_or_err), lhs, rhs)

#define ARROW_ASSIGN_OR_RETURN_IMPL(varname, lhs, rhs) \
    auto&& varname = (rhs);                            \
    RETURN_IF_ARROW_ERROR(varname.status());           \
    lhs = std::move(varname).ValueOrDie();

static StatusOr<std::vector<const TStarRocksRemoteScanOutput*>> order_remote_scan_outputs(
        const std::vector<TStarRocksRemoteScanOutput>& outputs, bool require_chunk_meta) {
    if (outputs.empty()) {
        return Status::InvalidArgument("starrocks remote scan outputs is empty");
    }

    std::vector<const TStarRocksRemoteScanOutput*> ordered(outputs.size(), nullptr);
    std::unordered_set<SlotId> local_slot_ids;
    local_slot_ids.reserve(outputs.size());
    for (const auto& output : outputs) {
        if (!output.__isset.output_index) {
            return Status::InvalidArgument("starrocks remote scan output missing output_index");
        }
        if (output.output_index < 0 || output.output_index >= static_cast<int32_t>(outputs.size())) {
            return Status::InvalidArgument(
                    fmt::format("starrocks remote scan output_index {} out of range", output.output_index));
        }
        if (ordered[output.output_index] != nullptr) {
            return Status::InvalidArgument(
                    fmt::format("duplicate starrocks remote scan output_index {}", output.output_index));
        }
        if (!output.__isset.local_slot_id) {
            return Status::InvalidArgument("starrocks remote scan output missing local_slot_id");
        }
        if (!local_slot_ids.emplace(output.local_slot_id).second) {
            return Status::InvalidArgument(
                    fmt::format("duplicate starrocks remote scan local_slot_id {}", output.local_slot_id));
        }
        if (require_chunk_meta && !output.__isset.actual_wire_type) {
            return Status::InvalidArgument("starrocks remote scan output missing actual_wire_type");
        }
        if (require_chunk_meta && !output.__isset.nullable) {
            return Status::InvalidArgument("starrocks remote scan output missing nullable");
        }
        if (require_chunk_meta && !output.__isset.is_const) {
            return Status::InvalidArgument("starrocks remote scan output missing is_const");
        }
        ordered[output.output_index] = &output;
    }

    for (size_t i = 0; i < ordered.size(); ++i) {
        if (ordered[i] == nullptr) {
            return Status::InvalidArgument(fmt::format("missing starrocks remote scan output_index {}", i));
        }
    }
    return ordered;
}

static Status convert_arrow_array_to_column(RuntimeState* state, SlotDescriptor* slot_desc,
                                            const std::shared_ptr<arrow::Array>& input_array, Column* column,
                                            size_t num_rows) {
    std::shared_ptr<arrow::Array> array = input_array;
    // Complex columns (ARRAY/MAP/STRUCT) need a recursive Arrow conversion plan (ConvertFuncTree
    // children / field_names) that this single-level converter does not build. The FE planner
    // already rejects complex columns for the arrow_flight transport (they go over brpc_chunk), so
    // this is a defensive guard: fail with a clear error instead of running an incomplete plan.
    if (slot_desc->type().is_complex_type()) {
        return Status::InternalError(
                fmt::format("remote arrow flight scan does not support complex column '{}' (type {}); "
                            "use the brpc_chunk transport",
                            slot_desc->col_name(), slot_desc->type().debug_string()));
    }
    if (array->type_id() == arrow::Type::DICTIONARY) {
        auto* dictionary_type = down_cast<const arrow::DictionaryType*>(array->type().get());
        auto decoded = arrow::compute::Cast(*array, dictionary_type->value_type());
        RETURN_IF_ARROW_ERROR(decoded.status());
        array = std::move(decoded).ValueOrDie();
    }

    ConvertFuncTree conv_func;
    conv_func.func = get_arrow_converter(array->type_id(), slot_desc->type().type, slot_desc->is_nullable(), false);
    if (conv_func.func == nullptr) {
        return illegal_converting_error(array->type()->name(), slot_desc->type().debug_string());
    }

    Filter chunk_filter(num_rows, 1);
    uint8_t* null_data = nullptr;
    Column* data_column = column;
    if (column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto* null_column = nullable_column->null_column_raw_ptr();
        size_t null_count = fill_null_column(array.get(), 0, num_rows, null_column, 0);
        nullable_column->set_has_null(null_count != 0);
        null_data = num_rows == 0 ? nullptr : &null_column->get_data().front();
        data_column = nullable_column->data_column_raw_ptr();
    } else if (array->null_count() > 0) {
        return Status::InternalError(
                fmt::format("remote arrow flight scan returned null for non-nullable slot '{}' (type {})",
                            slot_desc->col_name(), slot_desc->type().debug_string()));
    }

    ArrowConvertContext conv_ctx;
    if (state != nullptr) {
        conv_ctx.timezone = state->timezone();
    }
    conv_ctx.current_file = "remote_arrow_flight";
    conv_ctx.set_current_column(slot_desc->col_name(), slot_desc->type());
    RETURN_IF_ERROR(
            conv_func.func(array.get(), 0, num_rows, data_column, 0, null_data, &chunk_filter, &conv_ctx, &conv_func));
    if (column->is_nullable()) {
        down_cast<NullableColumn*>(column)->update_has_null();
    }
    return Status::OK();
}

static Status convert_record_batch_to_chunk(RuntimeState* state, const std::vector<SlotDescriptor*>& slots,
                                            const std::shared_ptr<arrow::RecordBatch>& record_batch, ChunkPtr* chunk) {
    if (record_batch == nullptr) {
        *chunk = nullptr;
        return Status::EndOfFile("remote arrow flight scan eof");
    }
    // The record batch comes off the wire from a remote cluster and is untrusted. Arrow's IPC reader
    // only checks each column's declared length, not that the value/offset buffers are physically large
    // enough, so a malformed or version-skewed peer could otherwise drive an out-of-bounds read in the
    // converters below. Fully validate before touching any buffer (the brpc transport already
    // bounds-checks every read in ColumnArraySerde; this gives the Arrow transport the same guarantee).
    RETURN_IF_ARROW_ERROR(record_batch->ValidateFull());
    if (record_batch->num_columns() < static_cast<int>(slots.size())) {
        return Status::InternalError("remote arrow flight record batch column count is less than tuple slots");
    }

    auto result = std::make_shared<Chunk>();
    size_t num_rows = record_batch->num_rows();
    for (size_t i = 0; i < slots.size(); ++i) {
        auto* slot = slots[i];
        auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        RETURN_IF_ERROR(convert_arrow_array_to_column(state, slot, record_batch->column(i), column.get(), num_rows));
        result->append_column(std::move(column), slot->id());
    }
    *chunk = std::move(result);
    return Status::OK();
}

} // namespace

StatusOr<serde::ProtobufChunkMeta> build_remote_scan_chunk_meta(
        const std::vector<TStarRocksRemoteScanOutput>& outputs) {
    ASSIGN_OR_RETURN(auto ordered_outputs, order_remote_scan_outputs(outputs, true));

    serde::ProtobufChunkMeta chunk_meta;
    chunk_meta.types.reserve(ordered_outputs.size());
    chunk_meta.is_nulls.reserve(ordered_outputs.size());
    chunk_meta.is_consts.reserve(ordered_outputs.size());
    for (size_t i = 0; i < ordered_outputs.size(); ++i) {
        const auto* output = ordered_outputs[i];
        chunk_meta.types.emplace_back(TypeDescriptor::from_thrift(output->actual_wire_type));
        chunk_meta.is_nulls.emplace_back(output->nullable);
        chunk_meta.is_consts.emplace_back(output->is_const);
        chunk_meta.slot_id_to_index[output->local_slot_id] = static_cast<int>(i);
    }
    return chunk_meta;
}

class RemoteScanClient {
public:
    RemoteScanClient(TNetworkAddress remote_be, std::string scan_token, std::vector<SlotDescriptor*> slots,
                     std::vector<TStarRocksRemoteScanOutput> remote_outputs)
            : _remote_be(std::move(remote_be)),
              _scan_token(std::move(scan_token)),
              _slots(std::move(slots)),
              _remote_outputs(std::move(remote_outputs)) {}
    virtual ~RemoteScanClient() = default;

    virtual Status open(RuntimeState* state) = 0;
    virtual void close(RuntimeState* state) {}
    virtual Status get_next(RuntimeState* state, ChunkPtr* chunk) = 0;

protected:
    TNetworkAddress _remote_be;
    std::string _scan_token;
    std::vector<SlotDescriptor*> _slots;
    std::vector<TStarRocksRemoteScanOutput> _remote_outputs;
};

class BrpcChunkRemoteScanClient final : public RemoteScanClient {
public:
    using RemoteScanClient::RemoteScanClient;

    Status open(RuntimeState* state) override {
        auto* query_execution_services = state->query_execution_services();
        _stub = query_execution_services->rpc->brpc_stub_cache->get_stub(_remote_be);
        if (_stub == nullptr) {
            return Status::InternalError(
                    fmt::format("connect remote scan BE {}:{} failed", _remote_be.hostname, _remote_be.port));
        }
        if (!_remote_outputs.empty()) {
            ASSIGN_OR_RETURN(_chunk_meta, build_remote_scan_chunk_meta(_remote_outputs));
            if (_chunk_meta.types.size() != _slots.size()) {
                return Status::InvalidArgument(
                        fmt::format("starrocks remote scan output size {} does not match local slot size {}",
                                    _chunk_meta.types.size(), _slots.size()));
            }
            return Status::OK();
        }
        _chunk_meta.types.reserve(_slots.size());
        _chunk_meta.is_nulls.reserve(_slots.size());
        _chunk_meta.is_consts.reserve(_slots.size());
        for (size_t i = 0; i < _slots.size(); ++i) {
            auto* slot = _slots[i];
            _chunk_meta.types.emplace_back(slot->type());
            _chunk_meta.is_nulls.emplace_back(slot->is_nullable());
            _chunk_meta.is_consts.emplace_back(false);
            _chunk_meta.slot_id_to_index[slot->id()] = static_cast<int>(i);
        }
        return Status::OK();
    }

    Status get_next(RuntimeState* state, ChunkPtr* chunk) override {
        PFetchRemoteScanChunkRequest request;
        request.set_scan_token(_scan_token);
        request.set_packet_seq(_packet_seq++);
        PFetchRemoteScanChunkResult result;
        brpc::Controller cntl;
        cntl.set_timeout_ms(state->query_options().query_timeout * 1000);
        _stub->fetch_remote_scan_chunk(&cntl, &request, &result, nullptr);
        if (cntl.Failed()) {
            return Status::InternalError(fmt::format("fetch remote scan chunk rpc failed: {}", cntl.ErrorText()));
        }
        Status status(result.status());
        RETURN_IF_ERROR(status);
        if (result.eos()) {
            return Status::EndOfFile("remote brpc chunk scan eof");
        }
        if (!result.has_chunk()) {
            return Status::InternalError("remote brpc chunk scan response missing ChunkPB");
        }
        if (!result.chunk().has_data()) {
            return Status::InternalError("remote brpc chunk scan response missing ChunkPB data");
        }
        serde::ProtobufChunkDeserializer deserializer(_chunk_meta, &result.chunk());
        ASSIGN_OR_RETURN(auto decoded, deserializer.deserialize(result.chunk().data()));
        *chunk = std::make_shared<Chunk>(std::move(decoded));
        return Status::OK();
    }

private:
    std::shared_ptr<PInternalService_RecoverableStub> _stub;
    serde::ProtobufChunkMeta _chunk_meta;
    int64_t _packet_seq = 0;
};

class ArrowFlightRemoteScanClient final : public RemoteScanClient {
public:
    using RemoteScanClient::RemoteScanClient;

    Status open(RuntimeState* state) override {
        using namespace arrow::flight;
        ARROW_ASSIGN_OR_RETURN(auto location, Location::ForGrpcTcp(_remote_be.hostname, _remote_be.port));
        ARROW_ASSIGN_OR_RETURN(_client, FlightClient::Connect(location));
        ARROW_ASSIGN_OR_RETURN(auto serialized_ticket, sql::CreateStatementQueryTicket("remote_scan:" + _scan_token));
        Ticket ticket(serialized_ticket);
        ARROW_ASSIGN_OR_RETURN(_reader, _client->DoGet(ticket));
        return Status::OK();
    }

    Status get_next(RuntimeState* state, ChunkPtr* chunk) override {
        arrow::flight::FlightStreamChunk stream_chunk;
        ARROW_ASSIGN_OR_RETURN(stream_chunk, _reader->Next());
        if (stream_chunk.data == nullptr) {
            return Status::EndOfFile("remote arrow flight scan eof");
        }
        return convert_record_batch_to_chunk(state, _slots, stream_chunk.data, chunk);
    }

private:
    std::unique_ptr<arrow::flight::FlightClient> _client;
    std::unique_ptr<arrow::flight::FlightStreamReader> _reader;
};

DataSourceProviderPtr StarRocksConnector::create_data_source_provider(ConnectorScanNode*,
                                                                      const TPlanNode& plan_node) const {
    return std::make_unique<StarRocksDataSourceProvider>(plan_node);
}

StarRocksDataSourceProvider::StarRocksDataSourceProvider(const TPlanNode& plan_node) {
    if (plan_node.__isset.starrocks_scan_node) {
        _starrocks_scan_node = plan_node.starrocks_scan_node;
    }
}

DataSourcePtr StarRocksDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<StarRocksDataSource>(this, scan_range);
}

const TupleDescriptor* StarRocksDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_starrocks_scan_node.tuple_id);
}

StatusOr<std::vector<SlotDescriptor*>> StarRocksDataSourceProvider::output_slots(RuntimeState* state) const {
    auto* tuple_desc = const_cast<TupleDescriptor*>(tuple_descriptor(state));
    if (tuple_desc == nullptr) {
        return Status::InternalError("failed to get tuple descriptor for starrocks scan");
    }

    // The FE always ships a non-empty remote_outputs for a real scan range: a synthetic
    // __sr_row_marker output guarantees at least one entry (even for COUNT(*)), and output_slots is
    // only reached when there is a real remote stream (open() short-circuits the EMPTYSET case
    // before calling this). A node that reaches here without remote_outputs is malformed, so fail
    // loudly instead of guessing a column order from names or the raw tuple.
    if (!_starrocks_scan_node.__isset.remote_outputs || _starrocks_scan_node.remote_outputs.empty()) {
        return Status::InternalError("starrocks scan node missing remote_outputs");
    }

    const auto& tuple_slots = tuple_desc->slots();
    ASSIGN_OR_RETURN(auto ordered_outputs, order_remote_scan_outputs(_starrocks_scan_node.remote_outputs, false));
    std::unordered_map<SlotId, SlotDescriptor*> slots_by_id;
    slots_by_id.reserve(tuple_slots.size());
    for (auto* slot : tuple_slots) {
        slots_by_id.emplace(slot->id(), slot);
    }

    std::vector<SlotDescriptor*> slots;
    slots.reserve(ordered_outputs.size());
    for (const auto* output : ordered_outputs) {
        auto iter = slots_by_id.find(output->local_slot_id);
        if (iter == slots_by_id.end()) {
            return Status::InternalError(fmt::format("starrocks scan local slot not found: {}", output->local_slot_id));
        }
        slots.emplace_back(iter->second);
    }
    return slots;
}

const std::vector<TStarRocksRemoteScanOutput>& StarRocksDataSourceProvider::remote_outputs() const {
    return _starrocks_scan_node.remote_outputs;
}

StarRocksDataSource::StarRocksDataSource(const StarRocksDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {
    if (scan_range.__isset.starrocks_scan_range) {
        _scan_range = scan_range.starrocks_scan_range;
        _has_scan_range = true;
    }
}

StarRocksDataSource::~StarRocksDataSource() = default;

std::string StarRocksDataSource::name() const {
    return "StarRocksDataSource";
}

Status StarRocksDataSource::open(RuntimeState* state) {
    if (!_has_scan_range) {
        // EMPTYSET plan: the remote optimizer collapsed the scan (empty table, contradictory
        // predicate, or a fully constant-folded plan), so the FE handed us a fragment with
        // no scan ranges. There is no remote stream to open; get_next will see _client ==
        // null and return EOS on the first call.
        return Status::OK();
    }
    _tuple_desc = const_cast<TupleDescriptor*>(_provider->tuple_descriptor(state));
    if (_tuple_desc == nullptr) {
        return Status::InternalError("failed to get tuple descriptor for starrocks scan");
    }
    ASSIGN_OR_RETURN(auto output_slots, _provider->output_slots(state));
    if (!_scan_range.__isset.scan_token || _scan_range.scan_token.empty()) {
        return Status::InvalidArgument("missing starrocks remote scan token");
    }
    if (!_scan_range.__isset.remote_be) {
        return Status::InvalidArgument("missing starrocks remote BE endpoint");
    }
    auto transport =
            _scan_range.__isset.transport ? _scan_range.transport : TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT;
    // Do NOT log scan_token: it is a bearer credential that can be replayed to fetch remote scan
    // results until it expires. Log only the query id and endpoint/transport.
    LOG(INFO) << "starrocks remote scan open: query_id=" << print_id(state->query_id())
              << " remote_be=" << _scan_range.remote_be.hostname << ":" << _scan_range.remote_be.port << " transport="
              << (transport == TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT ? "arrow_flight" : "brpc_chunk");
    if (transport == TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT) {
        _client = std::make_unique<ArrowFlightRemoteScanClient>(_scan_range.remote_be, _scan_range.scan_token,
                                                                std::move(output_slots), _provider->remote_outputs());
    } else {
        _client = std::make_unique<BrpcChunkRemoteScanClient>(_scan_range.remote_be, _scan_range.scan_token,
                                                              std::move(output_slots), _provider->remote_outputs());
    }
    return _client->open(state);
}

void StarRocksDataSource::close(RuntimeState* state) {
    if (_client != nullptr) {
        _client->close(state);
    }
}

Status StarRocksDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if (_client == nullptr) {
        // EMPTYSET path: no remote stream was opened, return EOS immediately.
        return Status::EndOfFile("remote starrocks scan empty plan");
    }
    do {
        auto status = _client->get_next(state, chunk);
        if (status.is_end_of_file()) {
            return status;
        }
        RETURN_IF_ERROR(status);
        if (*chunk != nullptr && (*chunk)->num_rows() > 0) {
            // Count raw rows/bytes as received from the wire, BEFORE residual filtering, so the
            // scanner's time-slice accounting and ScanRows/ScanBytes keep advancing even when the
            // residual predicates discard entire chunks.
            _raw_rows_read += (*chunk)->num_rows();
            _bytes_read += (*chunk)->bytes_usage();
            if (!_conjunct_ctxs.empty()) {
                // Residual predicates that the local FE could not push down (deny-list hits like
                // subqueries, non-deterministic functions, UDFs, or complex shapes that did not
                // round-trip through the SQL serializer) must be evaluated on the chunks that
                // come back from the remote BE; otherwise the local query silently returns the
                // unfiltered remote result set.
                RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, chunk->get()));
            }
        }
    } while (*chunk != nullptr && (*chunk)->num_rows() == 0);

    if (*chunk != nullptr) {
        _num_rows_read += (*chunk)->num_rows();
    }
    return Status::OK();
}

#undef ARROW_ASSIGN_OR_RETURN_IMPL
#undef ARROW_ASSIGN_OR_RETURN
#undef RETURN_IF_ARROW_ERROR

} // namespace starrocks::connector
