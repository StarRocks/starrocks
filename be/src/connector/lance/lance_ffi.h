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

struct ArrowArray;
struct ArrowSchema;
typedef struct LanceReader LanceReader;
typedef struct LanceString {
    const char* data;
    size_t len;
} LanceString;
typedef struct LanceProperty {
    LanceString key;
    LanceString value;
} LanceProperty;

typedef struct LanceCancellation {
    bool (*check)(void* context);
    void* context;
} LanceCancellation;

// Input buffers are borrowed during open only. A handle must not be used concurrently.
// version=0 resolves latest once. All batches of this reader use that immutable version.
// Returns 1 on success, -1 on error. Error strings must be freed with sr_lance_free_error.
int sr_lance_open(LanceString uri, uint64_t version, const LanceString* columns, size_t column_count,
                  int32_t batch_size, int32_t cloud_type, const LanceProperty* properties, size_t property_count,
                  LanceCancellation cancellation, LanceReader** reader, char** error);
// Returns 1=batch, 0=EOF, 2=pending (check cancellation and poll again), -1=error.
// Outputs must be zero-initialized. On success, caller owns both Arrow release callbacks.
int sr_lance_next(LanceReader* reader, struct ArrowArray* array, struct ArrowSchema* schema, char** error);
uint64_t sr_lance_version(const LanceReader* reader);
// Closing a reader does not invalidate exported Arrow arrays. Null is accepted.
void sr_lance_close(LanceReader* reader);
void sr_lance_free_error(char* error);

#ifdef __cplusplus
}
#endif
