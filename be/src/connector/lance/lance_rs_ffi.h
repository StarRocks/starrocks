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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum { SR_LANCE_ERROR = -1, SR_LANCE_NEXT_EOF = 0, SR_LANCE_NEXT_BATCH = 1, SR_LANCE_NEXT_PENDING = 2 };

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

typedef struct SrLanceCancellation {
    bool (*check)(void* context);
    void* context;
} SrLanceCancellation;

// Input buffers are borrowed during open only. A handle must not be used concurrently.
// version=0 resolves latest once. All batches of this reader use that immutable version.
// Returns 1 on success, -1 on error. Error strings must be freed with sr_lance_free_error.
int sr_lance_reader_open(SrLanceString dataset_uri, uint64_t version, const SrLanceString* columns, size_t column_count,
                         int32_t batch_size, int32_t cloud_type, const SrLanceStringPair* properties,
                         size_t property_count, SrLanceCancellation cancellation, SrLanceReader** out_reader,
                         char** error);
// Returns 1=batch, 0=EOF, 2=pending (check cancellation and poll again), -1=error.
// Outputs must be zero-initialized. On success, caller owns both Arrow release callbacks.
int sr_lance_reader_next(SrLanceReader* reader, struct ArrowArray* out_array, struct ArrowSchema* out_schema,
                         char** error);
uint64_t sr_lance_reader_version(const SrLanceReader* reader);
// Closing a reader does not invalidate exported Arrow arrays. Null is accepted.
void sr_lance_reader_close(SrLanceReader* reader);
void sr_lance_free_error(char* error);

#ifdef __cplusplus
}
#endif
