// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/key_coder.h"

namespace starrocks {

template <typename TraitsType>
KeyCoder::KeyCoder(TraitsType traits)
        : _full_encode_ascending(traits.full_encode_ascending),
          _encode_ascending(traits.encode_ascending),
          _decode_ascending(traits.decode_ascending),
          _full_encode_ascending_datum(traits.full_encode_ascending_datum),
          _encode_ascending_datum(traits.encode_ascending_datum) {}

// Helper class used to get KeyCoder
class KeyCoderResolver {
public:
    ~KeyCoderResolver() {
        for (KeyCoder* p : _coder_map) {
            delete p;
        }
    }

    static KeyCoderResolver* instance() {
        static KeyCoderResolver s_instance;
        return &s_instance;
    }

    KeyCoder* get_coder(LogicalType field_type) const { return _coder_map[field_type]; }

private:
    KeyCoderResolver() {
        add_mapping<TYPE_TINYINT>();
        add_mapping<TYPE_SMALLINT>();
        add_mapping<TYPE_INT>();
        add_mapping<TYPE_UNSIGNED_INT>();
        add_mapping<TYPE_BIGINT>();
        add_mapping<TYPE_UNSIGNED_BIGINT>();
        add_mapping<TYPE_LARGEINT>();
        add_mapping<TYPE_DATETIME_V1>();
        add_mapping<TYPE_DATETIME>();

        add_mapping<TYPE_DATE_V1>();
        add_mapping<TYPE_DATE>();
        add_mapping<TYPE_DECIMAL>();
        add_mapping<TYPE_DECIMALV2>();
        add_mapping<TYPE_CHAR>();
        add_mapping<TYPE_VARCHAR>();
        add_mapping<TYPE_BOOLEAN>();
    }

    template <LogicalType field_type>
    void add_mapping() {
        static_assert(field_type < TYPE_MAX_VALUE);
        _coder_map[field_type] = new KeyCoder(KeyCoderTraits<field_type>());
    }

    KeyCoder* _coder_map[TYPE_MAX_VALUE] = {nullptr};
};

const KeyCoder* get_key_coder(LogicalType type) {
    return KeyCoderResolver::instance()->get_coder(delegate_type(type));
}

} // namespace starrocks
