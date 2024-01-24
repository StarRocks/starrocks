#pragma once

#include <functional>
#include <ostream>
#include <string>
#include <type_traits>

#include "common/compiler_util.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "simdjson.h"
#include "types/constexpr.h"
#include "util/coding.h"
#include "velocypack/vpack.h"

namespace starrocks {

namespace vpack = arangodb::velocypack;

enum JsonType {
    JSON_BOOL = 0,
    JSON_NUMBER = 1,
    JSON_STRING = 2,
    JSON_NULL = 3,
    JSON_ARRAY = 4,
    JSON_OBJECT = 5,
};

// Maximum length of JSON string is 16MB
constexpr size_t kJSONLengthLimit = 16 << 20;

class JsonValue {
public:
    using VSlice = vpack::Slice;
    using VBuilder = vpack::Builder;

    JsonValue() = default;

    JsonValue(const JsonValue& rhs) : binary_(rhs.binary_.data(), rhs.binary_.size()) {}

    JsonValue(JsonValue&& rhs) noexcept : binary_(std::move(rhs.binary_)) {}

    JsonValue& operator=(const JsonValue& rhs) {
        if (this != &rhs) {
            binary_ = rhs.binary_;
        }
        return *this;
    }

    JsonValue& operator=(JsonValue&& rhs) noexcept {
        if (this != &rhs) {
            binary_ = std::move(rhs.binary_);
        }
        return *this;
    }

    // TODO(mofei) avoid copy data from slice ?
    explicit JsonValue(const Slice& src) { assign(src); }

    // TODO(mofei) avoid copy data from slice ?
    explicit JsonValue(const VSlice& slice) { assign(Slice(slice.start(), slice.byteSize())); }

    void assign(const Slice& src) { binary_.assign(src.get_data(), src.get_size()); }
    void assign(const vpack::Builder& b) { binary_.assign((const char*)b.data(), (size_t)b.size()); }

    ////////////////// builder  //////////////////////

    // construct a JsonValue from single sql type
    static JsonValue from_none();
    static JsonValue from_null();
    static JsonValue from_int(int64_t value);
    static JsonValue from_uint(uint64_t value);
    static JsonValue from_bool(bool value);
    static JsonValue from_double(double value);
    static JsonValue from_string(const Slice& value);

    template <class T>
    static StatusOr<JsonValue> from(T value) {
        if constexpr (std::is_same_v<T, bool>) {
            return JsonValue::from_bool(value);
        } else if constexpr (std::is_integral_v<T>) {
            return JsonValue::from_int(value);
        } else if constexpr (std::is_unsigned_v<T>) {
            return JsonValue::from_uint(value);
        } else if constexpr (std::is_floating_point_v<T>) {
            return JsonValue::from_double(value);
        } else if constexpr (std::is_same_v<T, const char*> || std::is_same_v<T, Slice> ||
                             std::is_same_v<T, std::string>) {
            return JsonValue::parse(value);
        } else {
            static_assert("not supported");
        }
    }

    // construct a JsonValue from simdjson::value
    static StatusOr<JsonValue> from_simdjson(simdjson::ondemand::value* value);
    static StatusOr<JsonValue> from_simdjson(simdjson::ondemand::object* obj);

    ////////////////// parsing  //////////////////////
    static Status parse(const Slice& src, JsonValue* out);
    static StatusOr<JsonValue> parse(const Slice& src);
    // Try to parse it as JSON object, otherwise consider it as a string
    static StatusOr<JsonValue> parse_json_or_string(const Slice& src);

    ////////////////// serialization  //////////////////////
    size_t serialize(uint8_t* dst) const;
    uint64_t serialize_size() const;

    ////////////////// RAW accessor ////////////////////////////
    Slice get_slice() const;
    VSlice to_vslice() const;
    const char* get_data() const { return binary_.data(); }

    ////////////////// access json values ////////////////////////
    JsonType get_type() const;
    StatusOr<bool> get_bool() const;
    StatusOr<int64_t> get_int() const;
    StatusOr<uint64_t> get_uint() const;
    StatusOr<double> get_double() const;
    StatusOr<Slice> get_string() const;
    StatusOr<JsonValue> get_obj(const std::string& key) const;
    bool is_null() const;
    bool is_none() const;

    ////////////////// util  //////////////////////
    StatusOr<std::string> to_string() const;
    std::string to_string_uncheck() const;
    int compare(const JsonValue& rhs) const;
    static int compare(const Slice& lhs, const Slice& rhs);
    int64_t hash() const;

private:
    template <class Ret, class Fn>
    StatusOr<Ret> callVPack(Fn fn);

    template <class Ret, class Fn>
    StatusOr<Ret> callVPack(Fn fn) const;

    // serialized binary of json
    // TODO(mofei) store vpack::Slice
    std::string binary_;
};

inline Status fromVPackException(const vpack::Exception& e) {
    return Status::JsonFormatError(Slice(e.what()));
}

inline JsonType fromVPackType(vpack::ValueType type) {
    switch (type) {
    case vpack::ValueType::Null:
    case vpack::ValueType::None:
        return JsonType::JSON_NULL;
    case vpack::ValueType::Bool:
        return JsonType::JSON_BOOL;
    case vpack::ValueType::Array:
        return JsonType::JSON_ARRAY;
    case vpack::ValueType::Object:
        return JsonType::JSON_OBJECT;
    case vpack::ValueType::Double:
    case vpack::ValueType::Int:
    case vpack::ValueType::UInt:
    case vpack::ValueType::SmallInt:
        return JsonType::JSON_NUMBER;
    case vpack::ValueType::String:
        return JsonType::JSON_STRING;
    default:
        DCHECK(false);
        return JsonType::JSON_NULL;
    }
}

inline vpack::Slice noneJsonSlice() {
    return vpack::Slice::noneSlice();
}

inline vpack::Slice nullJsonSlice() {
    return vpack::Slice::nullSlice();
}

inline vpack::Slice emptyStringJsonSlice() {
    return vpack::Slice::emptyStringSlice();
}

template <class Ret, class Fn>
inline StatusOr<Ret> JsonValue::callVPack(Fn fn) {
    try {
        return fn();
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }
}

template <class Ret, class Fn>
inline StatusOr<Ret> JsonValue::callVPack(Fn fn) const {
    try {
        return fn();
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }
}

// output
std::ostream& operator<<(std::ostream& os, const JsonValue& json);

// predicate operators
inline bool operator==(const JsonValue& lhs, const JsonValue& rhs) {
    return lhs.compare(rhs) == 0;
}
inline bool operator!=(const JsonValue& lhs, const JsonValue& rhs) {
    return lhs.compare(rhs) != 0;
}
inline bool operator<(const JsonValue& lhs, const JsonValue& rhs) {
    return lhs.compare(rhs) < 0;
}
inline bool operator<=(const JsonValue& lhs, const JsonValue& rhs) {
    return lhs.compare(rhs) <= 0;
}
inline bool operator>(const JsonValue& lhs, const JsonValue& rhs) {
    return lhs.compare(rhs) > 0;
}
inline bool operator>=(const JsonValue& lhs, const JsonValue& rhs) {
    return lhs.compare(rhs) >= 0;
}

} // namespace starrocks

// fmt::format
namespace fmt {
template <>
struct formatter<starrocks::JsonValue> : formatter<std::string> {
    template <typename FormatContext>
    auto format(const starrocks::JsonValue& p, FormatContext& ctx) -> decltype(ctx.out()) {
        return fmt::formatter<std::string>::format(p.to_string_uncheck(), ctx);
    }
};
} // namespace fmt

namespace std {

inline std::string to_string(const starrocks::JsonValue& value) {
    return value.to_string_uncheck();
}

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wunused-value")
template <>
struct less<starrocks::JsonValue> {
    bool operator()(const starrocks::JsonValue& lhs, const starrocks::JsonValue& rhs) const {
        return lhs.compare(rhs) < 0;
    }

    bool operator()(const starrocks::JsonValue* lhs, const starrocks::JsonValue* rhs) const {
        return starrocks::JsonValue::compare(lhs->get_slice(), rhs->get_slice()) < 0;
    }

    bool operator()(const starrocks::Slice& lhs, const starrocks::Slice& rhs) const {
        return starrocks::JsonValue::compare(lhs, rhs) < 0;
    }
};

template <>
struct less_equal<starrocks::JsonValue> {
    bool operator()(const starrocks::JsonValue& lhs, const starrocks::JsonValue& rhs) const {
        return lhs.compare(rhs) <= 0;
    }

    bool operator()(const starrocks::JsonValue* lhs, const starrocks::JsonValue* rhs) const {
        return starrocks::JsonValue::compare(lhs->get_slice(), rhs->get_slice()) <= 0;
    }

    bool operator()(const starrocks::Slice& lhs, const starrocks::Slice& rhs) const {
        return starrocks::JsonValue::compare(lhs, rhs) <= 0;
    }
};

template <>
struct greater<starrocks::JsonValue> {
    bool operator()(const starrocks::JsonValue& lhs, const starrocks::JsonValue& rhs) const {
        return lhs.compare(rhs) > 0;
    }
    bool operator()(const starrocks::JsonValue* lhs, const starrocks::JsonValue* rhs) const {
        return starrocks::JsonValue::compare(lhs->get_slice(), rhs->get_slice()) > 0;
    }

    bool operator()(const starrocks::Slice& lhs, const starrocks::Slice& rhs) const {
        return starrocks::JsonValue::compare(lhs, rhs) > 0;
    }
};
template <>
struct greater_equal<starrocks::JsonValue> {
    bool operator()(const starrocks::JsonValue& lhs, const starrocks::JsonValue& rhs) const {
        return lhs.compare(rhs) >= 0;
    }

    bool operator()(const starrocks::JsonValue* lhs, const starrocks::JsonValue* rhs) const {
        return starrocks::JsonValue::compare(lhs->get_slice(), rhs->get_slice()) >= 0;
    }

    bool operator()(const starrocks::Slice& lhs, const starrocks::Slice& rhs) const {
        return starrocks::JsonValue::compare(lhs, rhs) >= 0;
    }
};
template <>
struct equal_to<starrocks::JsonValue> {
    bool operator()(const starrocks::JsonValue& lhs, const starrocks::JsonValue& rhs) const {
        return lhs.compare(rhs) == 0;
    }

    bool operator()(const starrocks::JsonValue* lhs, const starrocks::JsonValue* rhs) const {
        return starrocks::JsonValue::compare(lhs->get_slice(), rhs->get_slice()) == 0;
    }

    bool operator()(const starrocks::Slice& lhs, const starrocks::Slice& rhs) const {
        return starrocks::JsonValue::compare(lhs, rhs) == 0;
    }
};
template <>
struct not_equal_to<starrocks::JsonValue> {
    bool operator()(const starrocks::JsonValue& lhs, const starrocks::JsonValue& rhs) const {
        return lhs.compare(rhs) != 0;
    }

    bool operator()(const starrocks::JsonValue* lhs, const starrocks::JsonValue* rhs) const {
        return starrocks::JsonValue::compare(lhs->get_slice(), rhs->get_slice()) != 0;
    }

    bool operator()(const starrocks::Slice& lhs, const starrocks::Slice& rhs) const {
        return starrocks::JsonValue::compare(lhs, rhs) != 0;
    }
};

DIAGNOSTIC_POP

} // namespace std
