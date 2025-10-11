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


package com.staros.schedule.select;

import java.util.List;
import java.util.Map;

public interface Selector {
    /**
     * Select n elements from given Map (WorkerId, Scores),  from the low end, (score value natural order)
     * @param scores worker scores. workerId -> score map
     * @param nSelect number of selections
     * @return n elements from given map by the Selector Policy
     */
    List<Long> selectHighEnd(Map<Long, Double> scores, int nSelect);

    /**
     * Select n elements from given Map (WorkerId, Scores), from the low end, (score value natural order)
     * @param scores worker scores. workerId -> score map
     * @param nSelect number of selections
     * @return n elements from given map by the Selector Policy
     */
    List<Long> selectLowEnd(Map<Long, Double> scores, int nSelect);
}
