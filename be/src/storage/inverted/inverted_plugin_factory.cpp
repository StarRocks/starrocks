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

#include "storage/inverted/inverted_plugin_factory.h"

#include "clucene/clucene_plugin.h"
#include "common/statusor.h"

namespace starrocks {
StatusOr<InvertedPlugin*> InvertedPluginFactory::get_plugin(InvertedImplementType imp_type) {
    switch (imp_type) {
    case InvertedImplementType::CLUCENE:
        return &CLucenePlugin::get_instance();
    default:
        return Status::InternalError("Invalid implement of inverted type");
    }
}

} // namespace starrocks