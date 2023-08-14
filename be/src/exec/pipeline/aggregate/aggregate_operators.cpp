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

// NOTE: this combined cpp file is used to avoid duplicated template instantiation of Aggregator

#include "aggregate_blocking_sink_operator.cpp"
#include "aggregate_blocking_source_operator.cpp"
#include "aggregate_distinct_blocking_sink_operator.cpp"
#include "aggregate_distinct_blocking_source_operator.cpp"
#include "aggregate_distinct_streaming_sink_operator.cpp"
#include "aggregate_distinct_streaming_source_operator.cpp"
#include "aggregate_streaming_sink_operator.cpp"
#include "aggregate_streaming_source_operator.cpp"
#include "sorted_aggregate_streaming_sink_operator.cpp"
#include "sorted_aggregate_streaming_source_operator.cpp"
#include "spillable_aggregate_blocking_sink_operator.cpp"
#include "spillable_aggregate_blocking_source_operator.cpp"
#include "spillable_aggregate_distinct_blocking_operator.cpp"