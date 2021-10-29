// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

namespace starrocks {
namespace vectorized {
class ZoneMapDetail {
public:
    // ctors
    ZoneMapDetail() = default;
    ZoneMapDetail(Datum min_or_null_value, Datum max_value)
            : _has_null(min_or_null_value.is_null()), _min_value(min_or_null_value), _max_value(max_value) {}
    ZoneMapDetail(Datum min_value, Datum max_value, bool has_null)
            : _has_null(has_null), _min_value(min_value), _max_value(max_value) {}

    // methods
    bool has_null() const { return _has_null; }
    void set_has_null(bool v) { _has_null = v; }
    bool has_not_null() const { return !_min_value.is_null() || !_max_value.is_null(); }
    Datum& min_value() { return _min_value; }
    const Datum& min_value() const { return _min_value; }
    Datum& max_value() { return _max_value; }
    const Datum& max_value() const { return _max_value; }
    const Datum& min_or_null_value() const {
        if (_has_null) return _null_value;
        return _min_value;
    }
    size_t num_rows;

private:
    bool _has_null;
    Datum _null_value;
    Datum _min_value;
    Datum _max_value;
};
} // namespace vectorized
} // namespace starrocks