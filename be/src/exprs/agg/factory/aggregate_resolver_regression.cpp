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

#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/ols.h"

namespace starrocks {

void AggregateFuncResolver::register_regression() {
    // register for ols
    add_aggregate_mapping<TYPE_JSON, OlsState>(std::string("ols_train"),
                                               std::vector{TYPE_DOUBLE, TYPE_ARRAY, TYPE_BOOLEAN}, false,
                                               std::make_shared<OlsAggregateFunction<true, false>>());

    add_aggregate_mapping<TYPE_VARCHAR, OlsState>(std::string("ols"),
                                                  std::vector{TYPE_DOUBLE, TYPE_ARRAY, TYPE_BOOLEAN}, false,
                                                  std::make_shared<OlsAggregateFunction<false, false>>());

    add_aggregate_mapping<TYPE_VARCHAR, OlsState>(std::string("ols"),
                                                  std::vector{TYPE_DOUBLE, TYPE_ARRAY, TYPE_BOOLEAN, TYPE_VARCHAR},
                                                  false, std::make_shared<OlsAggregateFunction<false, false>>());

    add_aggregate_mapping<TYPE_VARCHAR, OlsState>(
            std::string("ols"), std::vector{TYPE_DOUBLE, TYPE_ARRAY, TYPE_BOOLEAN, TYPE_VARCHAR, TYPE_JSON, TYPE_JSON},
            false, std::make_shared<OlsAggregateFunction<false, false>>());

    // register for ols
    add_aggregate_mapping<TYPE_JSON, OlsState>(std::string("wls_train"),
                                               std::vector{TYPE_DOUBLE, TYPE_ARRAY, TYPE_DOUBLE, TYPE_BOOLEAN}, false,
                                               std::make_shared<OlsAggregateFunction<true, true>>());

    add_aggregate_mapping<TYPE_VARCHAR, OlsState>(std::string("wls"),
                                                  std::vector{TYPE_DOUBLE, TYPE_ARRAY, TYPE_DOUBLE, TYPE_BOOLEAN},
                                                  false, std::make_shared<OlsAggregateFunction<false, true>>());

    add_aggregate_mapping<TYPE_VARCHAR, OlsState>(
            std::string("wls"), std::vector{TYPE_DOUBLE, TYPE_ARRAY, TYPE_DOUBLE, TYPE_BOOLEAN, TYPE_VARCHAR}, false,
            std::make_shared<OlsAggregateFunction<false, true>>());

    add_aggregate_mapping<TYPE_VARCHAR, OlsState>(
            std::string("wls"),
            std::vector{TYPE_DOUBLE, TYPE_ARRAY, TYPE_DOUBLE, TYPE_BOOLEAN, TYPE_VARCHAR, TYPE_JSON, TYPE_JSON}, false,
            std::make_shared<OlsAggregateFunction<false, true>>());
}

} // namespace starrocks
