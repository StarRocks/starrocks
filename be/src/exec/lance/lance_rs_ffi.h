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

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowArray;
struct ArrowSchema;
typedef struct SrLanceReader SrLanceReader;

typedef struct SrLanceString {
    const char* data;
    size_t len;
} SrLanceString;

typedef struct SrLanceStringPair {
    SrLanceString key;
    SrLanceString value;
} SrLanceStringPair;

typedef struct SrLanceVectorOptions {
    SrLanceString vector_column;
    const SrLanceString* query_vector;
    size_t query_vector_len;
    int64_t limit_k;
    const SrLanceString* index_segment_uuids;
    size_t index_segment_uuid_count;
    int32_t nprobes;
    int32_t refine_factor;
    int32_t ef;
    int32_t query_parallelism;
} SrLanceVectorOptions;

int sr_lance_reader_open(SrLanceString dataset_uri, int32_t fragment_id, const SrLanceString* columns,
                         size_t column_count, int32_t batch_size, const SrLanceStringPair* storage_options,
                         size_t storage_option_count, const SrLanceVectorOptions* vector_options,
                         SrLanceReader** out_reader, char** error);

int sr_lance_reader_next(SrLanceReader* reader, struct ArrowArray* out_array, struct ArrowSchema* out_schema,
                         int64_t* out_rows, char** error);

void sr_lance_reader_close(SrLanceReader* reader);
void sr_lance_free_error(char* error);

#ifdef __cplusplus
}
#endif
