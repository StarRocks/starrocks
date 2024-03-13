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

#pragma once

#include "exec/dictionary_cache_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {

namespace pipeline {

class DictionaryCacheSinkOperator final : public Operator {
public:
    DictionaryCacheSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                TDictionaryCacheSink t_dictionary_cache_sink, FragmentContext* fragment_ctx)
            : Operator(factory, id, "dictionary_cache_sink", plan_node_id, false, driver_sequence),
              _t_dictionary_cache_sink(t_dictionary_cache_sink),
              _fragment_ctx(fragment_ctx) {}

    ~DictionaryCacheSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    bool pending_finish() const override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    std::unique_ptr<DictionaryCacheWriter> _writer = nullptr;
    TDictionaryCacheSink _t_dictionary_cache_sink;
    FragmentContext* _fragment_ctx = nullptr;
};

class DictionaryCacheSinkOperatorFactory final : public OperatorFactory {
public:
    DictionaryCacheSinkOperatorFactory(int32_t id, const TDictionaryCacheSink& t_dictionary_cache_sink,
                                       FragmentContext* fragment_ctx)
            : OperatorFactory(id, "dictionary_cache_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
              _t_dictionary_cache_sink(t_dictionary_cache_sink),
              _fragment_ctx(fragment_ctx) {}

    ~DictionaryCacheSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<DictionaryCacheSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                             _t_dictionary_cache_sink, _fragment_ctx);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    TDictionaryCacheSink _t_dictionary_cache_sink;

    FragmentContext* _fragment_ctx = nullptr;
};

} // namespace pipeline
} // namespace starrocks
