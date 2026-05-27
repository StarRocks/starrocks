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

package com.starrocks.alter.reshard.presplit;

/**
 * Opaque marker for per-call-site scan context; concrete implementations are
 * supplied by each integration point (INSERT-from-FILES, Broker Load, ...).
 * The sampler does not introspect it — it threads the context back to the
 * code path that builds the {@code FileScanNode} for the sampling sub-query.
 */
public interface ScanContext {
}
