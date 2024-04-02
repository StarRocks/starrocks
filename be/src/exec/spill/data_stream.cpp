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

#include "exec/spill/data_stream.h"

#include "common/status.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {
// spill output stream. output serialized chunk data to BlockManager and add handle to block group.
class BlockSpillOutputDataStream final : public SpillOutputDataStream {
public:
    BlockSpillOutputDataStream(Spiller* spiller, BlockGroup* block_group, BlockManager* block_manager)
            : _spiller(spiller), _block_group(block_group), _block_manager(block_manager) {}
    ~BlockSpillOutputDataStream() override = default;

    Status append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size) override;
    Status flush() override;

    bool is_remote() const override {
        if (_cur_block != nullptr) {
            return _cur_block->is_remote();
        }
        return false;
    }

private:
    // acquire block from block manager
    Status _prepare_block(RuntimeState* state, size_t write_size);
    BlockPtr _cur_block;

    Spiller* _spiller{};

    BlockGroup* _block_group{};
    BlockManager* _block_manager{};
};

Status BlockSpillOutputDataStream::_prepare_block(RuntimeState* state, size_t write_size) {
    if (_cur_block == nullptr || !_cur_block->preallocate(write_size)) {
        // flush current block firstly
        RETURN_IF_ERROR(flush());
        // TODO: add profile for acquire block
        spill::AcquireBlockOptions opts;
        opts.query_id = state->query_id();
        opts.fragment_instance_id = state->fragment_instance_id();
        opts.plan_node_id = _spiller->options().plan_node_id;
        opts.name = _spiller->options().name;
        opts.block_size = write_size;
        ASSIGN_OR_RETURN(auto block, _block_manager->acquire_block(opts));
        // update metrics
        auto block_count = GET_METRICS(block->is_remote(), _spiller->metrics(), block_count);
        COUNTER_UPDATE(block_count, 1);
        TRACE_SPILL_LOG << fmt::format("allocate block [{}]", block->debug_string());
        _cur_block = std::move(block);
    }

    return Status::OK();
}

Status BlockSpillOutputDataStream::append(RuntimeState* state, const std::vector<Slice>& data,
                                          size_t total_write_size) {
    // acquire block if current block is nullptr or full
    RETURN_IF_ERROR(_prepare_block(state, total_write_size));
    {
        auto write_io_timer = GET_METRICS(_cur_block->is_remote(), _spiller->metrics(), write_io_timer);
        SCOPED_TIMER(write_io_timer);
        TRACE_SPILL_LOG << fmt::format("append block[{}], size[{}]", _cur_block->debug_string(), total_write_size);
        RETURN_IF_ERROR(_cur_block->append(data));
    }
    return Status::OK();
}

Status BlockSpillOutputDataStream::flush() {
    if (_cur_block == nullptr) {
        return Status::OK();
    }
    {
        auto write_io_timer = GET_METRICS(_cur_block->is_remote(), _spiller->metrics(), write_io_timer);
        SCOPED_TIMER(write_io_timer);
        size_t block_size = _cur_block->size();
        RETURN_IF_ERROR(_cur_block->flush());
        auto flush_bytes = GET_METRICS(_cur_block->is_remote(), _spiller->metrics(), flush_bytes);
        COUNTER_UPDATE(flush_bytes, block_size);
        TRACE_SPILL_LOG << fmt::format("flush block[{}]", _cur_block->debug_string());
    }
    RETURN_IF_ERROR(_block_manager->release_block(_cur_block));
    _block_group->append(std::move(_cur_block));

    return Status::OK();
}

std::shared_ptr<SpillOutputDataStream> create_spill_output_stream(Spiller* spiller, BlockGroup* block_group,
                                                                  BlockManager* block_manager) {
    return std::make_shared<BlockSpillOutputDataStream>(spiller, block_group, block_manager);
}

} // namespace starrocks::spill