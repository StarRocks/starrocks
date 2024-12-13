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


package com.starrocks.lake.compaction;

import java.util.Collection;
import java.util.List;
<<<<<<< HEAD
=======
import java.util.Set;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import javax.validation.constraints.NotNull;

public interface Selector {
    @NotNull
<<<<<<< HEAD
    List<PartitionStatistics> select(@NotNull Collection<PartitionStatistics> statistics);
=======
    List<PartitionStatisticsSnapshot> select(@NotNull Collection<PartitionStatistics> statistics,
                                             @NotNull Set<Long> excludeTables);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
