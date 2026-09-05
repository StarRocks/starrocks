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

package com.starrocks.common.util;

import java.util.List;
import java.util.Map;

/**
 * Raw intermediate representation of a single {@link RuntimeProfile} node
 * and its subtree, used by {@link ProfileSerializer} during serialisation and
 * deserialisation.
 *
 * <p>Carries exactly what the formatters need to produce a profile string —
 * no merge-time fields (strategy, min/max, display_threshold) are included.</p>
 *
 * <p>Counters are stored in BFS order (parent before child) so that
 * {@link RuntimeProfile#addCounter} can be called unconditionally during
 * deserialisation — every parent counter is guaranteed to exist before its
 * children are inserted.</p>
 *
 * @param name        profile node name ({@link RuntimeProfile#getName()})
 * @param counters    display-only counter snapshots, BFS order
 * @param infoStrings key-value metadata strings ({@link RuntimeProfile#getInfoStrings()})
 * @param children    child node snapshots, preserving original order
 */
record ProfileRawNode(
        String name,
        List<ProfileRawCounter> counters,
        Map<String, String> infoStrings,
        List<ProfileRawNode> children) {
}
