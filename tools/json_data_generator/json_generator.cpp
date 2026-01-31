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

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

static std::string join_strings(const std::vector<std::string>& parts, const std::string& delim) {
    std::string out;
    size_t total = 0;
    for (const auto& p : parts) total += p.size();
    if (!parts.empty()) total += delim.size() * (parts.size() - 1);
    out.reserve(total);
    for (size_t i = 0; i < parts.size(); i++) {
        if (i > 0) out += delim;
        out += parts[i];
    }
    return out;
}

static std::string to_lower_ascii(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

enum class FieldType { STRING, INT, BOOL, DATETIME, ARRAY, OBJECT };

enum class Cardinality { HIGH, MEDIUM, LOW };

struct CliOptions {
    int num_records = 1000;
    int num_fields = 10;
    double sparsity = 0.0;
    int max_depth = 4;
    double nest_probability = 0.2;
    std::vector<FieldType> field_types = {FieldType::STRING, FieldType::INT, FieldType::BOOL};
    int high_cardinality_fields = 0;
    int low_cardinality_fields = 0;
    int64_t seed = -1;

    // Output options
    std::string output_path;

    // Query generation options (match json_data_generator.py CLI)
    std::string gen_query_type;   // comma-separated: filter,aggregation,select
    int gen_query_num = 10;       // per type
    std::string gen_query_output; // empty => stdout
    std::string gen_query_table = "json_test_table";
    std::string gen_query_column = "json_data";
};

class ValueGenerator {
public:
    explicit ValueGenerator(int64_t seed = -1) : rng_(seed >= 0 ? seed : std::random_device{}()) {}

    std::string generate_string(Cardinality cardinality) {
        if (cardinality == Cardinality::HIGH) {
            string_counter_++;
            std::string random_part = generate_random_string(8);
            return fmt::format("unique_value_{}_{}", string_counter_, random_part);
        } else if (cardinality == Cardinality::LOW) {
            static const std::vector<std::string> low_strings = {"red",    "green", "blue",  "yellow", "purple",
                                                                 "orange", "pink",  "brown", "black",  "white"};
            std::uniform_int_distribution<size_t> dist(0, low_strings.size() - 1);
            return low_strings[dist(rng_)];
        } else { // MEDIUM
            static const std::vector<std::string> bases = {"value", "data", "item", "record", "entry"};
            std::uniform_int_distribution<size_t> base_dist(0, bases.size() - 1);
            std::uniform_int_distribution<int> num_dist(1, 100);
            return fmt::format("{}_{}", bases[base_dist(rng_)], num_dist(rng_));
        }
    }

    int64_t generate_int(Cardinality cardinality) {
        if (cardinality == Cardinality::HIGH) {
            int_counter_++;
            std::uniform_int_distribution<int64_t> dist(100000000, 999999999);
            return dist(rng_) + int_counter_;
        } else if (cardinality == Cardinality::LOW) {
            static const std::vector<int64_t> low_ints = {100, 200, 300, 400, 500};
            std::uniform_int_distribution<size_t> dist(0, low_ints.size() - 1);
            return low_ints[dist(rng_)];
        } else { // MEDIUM
            std::uniform_int_distribution<int64_t> dist(1, 10000);
            return dist(rng_);
        }
    }

    bool generate_bool() {
        std::uniform_int_distribution<int> dist(0, 1);
        return dist(rng_) == 1;
    }

    std::string generate_datetime() {
        auto start = std::chrono::system_clock::from_time_t(1577836800); // 2020-01-01
        auto end = std::chrono::system_clock::from_time_t(1767225600);   // 2025-12-31
        auto start_sec = std::chrono::duration_cast<std::chrono::seconds>(start.time_since_epoch()).count();
        auto end_sec = std::chrono::duration_cast<std::chrono::seconds>(end.time_since_epoch()).count();
        std::uniform_int_distribution<int64_t> dist(start_sec, end_sec);
        int64_t random_sec = dist(rng_);
        auto time_point = std::chrono::system_clock::from_time_t(random_sec);
        auto time_t = std::chrono::system_clock::to_time_t(time_point);
        auto tm = *std::localtime(&time_t);
        return fmt::format("{:%Y-%m-%d %H:%M:%S}", tm);
    }

private:
    std::mt19937 rng_;
    int64_t string_counter_ = 0;
    int64_t int_counter_ = 0;

    std::string generate_random_string(int length) {
        const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::uniform_int_distribution<int> dist(0, chars.size() - 1);
        std::string result;
        result.reserve(length);
        for (int i = 0; i < length; ++i) {
            result += chars[dist(rng_)];
        }
        return result;
    }
};

class JSONGenerator {
public:
    JSONGenerator(int num_fields, double sparsity, int max_depth, double nest_probability,
                  const std::vector<FieldType>& field_types, int high_cardinality_fields, int low_cardinality_fields,
                  int64_t seed)
            : num_fields_(num_fields),
              sparsity_(sparsity),
              max_depth_(max_depth),
              nest_probability_(nest_probability),
              field_types_(field_types.empty()
                                   ? std::vector<FieldType>{FieldType::STRING, FieldType::INT, FieldType::BOOL}
                                   : field_types),
              high_cardinality_fields_(high_cardinality_fields),
              low_cardinality_fields_(low_cardinality_fields),
              rng_(seed >= 0 ? seed : std::random_device{}()),
              value_gen_(seed) {
        generate_field_schemas();
    }

    std::string generate_record() {
        std::vector<std::string> fields;
        fields.reserve(field_schemas_.size());

        std::uniform_int_distribution<int> sparsity_dist(0, 9999);

        for (const auto& schema : field_schemas_) {
            if (sparsity_dist(rng_) < sparsity_ * 10000) {
                continue;
            }

            fields.push_back(fmt::format("{}:{}", escape_json_string(schema.name), generate_value_json(schema)));
        }

        return fmt::format("{{{}}}", join_strings(fields, ","));
    }

    const std::vector<std::string>& field_names() const { return field_names_cache_; }
    const std::vector<FieldType>& field_types_for_schema() const { return field_types_cache_; }
    const std::vector<Cardinality>& field_cardinalities() const { return field_cardinalities_cache_; }

private:
    struct FieldSchema {
        std::string name;
        FieldType field_type;
        Cardinality cardinality;
    };

    int num_fields_;
    double sparsity_;
    int max_depth_;
    double nest_probability_;
    std::vector<FieldType> field_types_;
    int high_cardinality_fields_;
    int low_cardinality_fields_;
    std::mt19937 rng_;
    ValueGenerator value_gen_;
    std::vector<FieldSchema> field_schemas_;
    std::vector<std::string> field_names_cache_;
    std::vector<FieldType> field_types_cache_;
    std::vector<Cardinality> field_cardinalities_cache_;

    void generate_field_schemas() {
        field_schemas_.clear();
        field_schemas_.reserve(num_fields_);

        for (int i = 0; i < num_fields_; ++i) {
            Cardinality cardinality;
            if (i < high_cardinality_fields_) {
                cardinality = Cardinality::HIGH;
            } else if (i < high_cardinality_fields_ + low_cardinality_fields_) {
                cardinality = Cardinality::LOW;
            } else {
                cardinality = Cardinality::MEDIUM;
            }

            std::string field_name = fmt::format("field_{}", i + 1);
            FieldType field_type = choose_field_type();

            field_schemas_.push_back({field_name, field_type, cardinality});
        }

        field_names_cache_.clear();
        field_types_cache_.clear();
        field_cardinalities_cache_.clear();
        field_names_cache_.reserve(field_schemas_.size());
        field_types_cache_.reserve(field_schemas_.size());
        field_cardinalities_cache_.reserve(field_schemas_.size());
        for (const auto& s : field_schemas_) {
            field_names_cache_.push_back(s.name);
            field_types_cache_.push_back(s.field_type);
            field_cardinalities_cache_.push_back(s.cardinality);
        }
    }

    FieldType choose_field_type() {
        std::uniform_int_distribution<size_t> dist(0, field_types_.size() - 1);
        return field_types_[dist(rng_)];
    }

    Cardinality choose_cardinality_for_nested() {
        std::uniform_int_distribution<int> dist(0, 99);
        int r = dist(rng_);
        if (r < 30) return Cardinality::HIGH;
        if (r < 50) return Cardinality::LOW;
        return Cardinality::MEDIUM;
    }

    std::string escape_json_string(const std::string& str) {
        std::string result;
        // Optimize: Reserve space to minimize reallocations.
        // Assuming ~12.5% expansion for escaped chars + 2 quotes.
        result.reserve(str.size() + str.size() / 8 + 2);
        result.push_back('"');

        const char* p = str.data();
        size_t len = str.size();
        size_t last_pos = 0;

        for (size_t i = 0; i < len; ++i) {
            char c = p[i];
            char replacement = 0;

            if (c == '"')
                replacement = '"';
            else if (c == '\\')
                replacement = '\\';
            else if (c == '\n')
                replacement = 'n';
            else if (c == '\r')
                replacement = 'r';
            else if (c == '\t')
                replacement = 't';
            else
                continue;

            result.append(p + last_pos, i - last_pos);
            result.push_back('\\');
            result.push_back(replacement);
            last_pos = i + 1;
        }

        result.append(p + last_pos, len - last_pos);
        result.push_back('"');
        return result;
    }

    std::string generate_nested_object_json(int depth) {
        if (depth >= max_depth_) {
            return escape_json_string(value_gen_.generate_string(Cardinality::MEDIUM));
        }

        std::uniform_int_distribution<int> num_fields_dist(2, std::min(5, num_fields_));
        int num_nested_fields = num_fields_dist(rng_);

        std::vector<std::string> fields;
        fields.reserve(num_nested_fields);

        for (int i = 0; i < num_nested_fields; ++i) {
            std::string nested_field_name = fmt::format("nested_{}_{}", depth, i + 1);
            FieldType nested_type = choose_field_type();
            Cardinality nested_cardinality = choose_cardinality_for_nested();

            FieldSchema nested_schema{nested_field_name, nested_type, nested_cardinality};

            std::string value;
            if (nested_type == FieldType::OBJECT && depth + 1 < max_depth_) {
                value = generate_nested_object_json(depth + 1);
            } else {
                value = generate_value_json(nested_schema, depth + 1);
            }

            fields.push_back(fmt::format("{}:{}", escape_json_string(nested_field_name), value));
        }

        return fmt::format("{{{}}}", join_strings(fields, ","));
    }

    std::string generate_value_json(const FieldSchema& schema, int depth = 0) {
        std::uniform_int_distribution<int> dist(0, 9999);
        bool should_nest =
                (schema.field_type == FieldType::OBJECT) ||
                (depth < max_depth_ &&
                 std::find(field_types_.begin(), field_types_.end(), schema.field_type) != field_types_.end() &&
                 dist(rng_) < nest_probability_ * 10000);

        if (should_nest) {
            return generate_nested_object_json(depth + 1);
        }

        switch (schema.field_type) {
        case FieldType::STRING:
            return escape_json_string(value_gen_.generate_string(schema.cardinality));
        case FieldType::INT:
            return std::to_string(value_gen_.generate_int(schema.cardinality));
        case FieldType::BOOL:
            return value_gen_.generate_bool() ? "true" : "false";
        case FieldType::DATETIME:
            return escape_json_string(value_gen_.generate_datetime());
        case FieldType::ARRAY: {
            FieldType array_type = FieldType::STRING;
            for (FieldType t : field_types_) {
                if (t != FieldType::ARRAY) {
                    array_type = t;
                    break;
                }
            }
            int array_size = 3;
            std::vector<std::string> elements;
            elements.reserve(array_size);
            FieldSchema array_schema{"", array_type, schema.cardinality};
            for (int i = 0; i < array_size; ++i) {
                elements.push_back(generate_value_json(array_schema, depth));
            }
            return fmt::format("[{}]", join_strings(elements, ","));
        }
        case FieldType::OBJECT:
            return generate_nested_object_json(depth + 1);
        default:
            return escape_json_string(value_gen_.generate_string(schema.cardinality));
        }
    }
};

FieldType parse_field_type(const char* type_str) {
    if (strcmp(type_str, "string") == 0) return FieldType::STRING;
    if (strcmp(type_str, "int") == 0) return FieldType::INT;
    if (strcmp(type_str, "bool") == 0) return FieldType::BOOL;
    if (strcmp(type_str, "datetime") == 0) return FieldType::DATETIME;
    if (strcmp(type_str, "array") == 0) return FieldType::ARRAY;
    if (strcmp(type_str, "object") == 0) return FieldType::OBJECT;
    return FieldType::STRING;
}

std::vector<FieldType> parse_field_types(const char* types_str) {
    std::vector<FieldType> result;
    std::string str(types_str);
    size_t pos = 0;
    while (pos < str.length()) {
        size_t comma = str.find(',', pos);
        if (comma == std::string::npos) {
            comma = str.length();
        }
        std::string type = str.substr(pos, comma - pos);
        // Trim whitespace
        while (!type.empty() && type[0] == ' ') type.erase(0, 1);
        while (!type.empty() && type[type.length() - 1] == ' ') type.erase(type.length() - 1);
        if (!type.empty()) {
            result.push_back(parse_field_type(type.c_str()));
        }
        pos = comma + 1;
    }
    return result;
}

static std::string json_extract_expr(const std::string& field_name, FieldType field_type,
                                     const std::string& json_column) {
    std::string json_path = fmt::format("$.{}", field_name);
    switch (field_type) {
    case FieldType::INT:
        return fmt::format("get_json_int({}, '{}')", json_column, json_path);
    case FieldType::STRING:
        return fmt::format("get_json_string({}, '{}')", json_column, json_path);
    case FieldType::BOOL:
        // Match python: CAST(get_json_string(...) AS BOOLEAN)
        return fmt::format("CAST(get_json_string({}, '{}') AS BOOLEAN)", json_column, json_path);
    case FieldType::DATETIME:
        return fmt::format("get_json_string({}, '{}')", json_column, json_path);
    default:
        return fmt::format("get_json_string({}, '{}')", json_column, json_path);
    }
}

// Extract a top-level JSON field value from a JSON object string, without full JSON parsing.
// Returns the raw token string for numbers/bools, or the decoded (unescaped) content for strings.
// If the value is an object/array or not found, returns nullopt.
static std::optional<std::pair<char, std::string>> extract_top_level_simple_value(const std::string& json,
                                                                                  const std::string& key) {
    std::string needle = fmt::format("\"{}\":", key);
    size_t pos = json.find(needle);
    if (pos == std::string::npos) return std::nullopt;
    pos += needle.size();
    if (pos >= json.size()) return std::nullopt;

    char c = json[pos];
    if (c == '{' || c == '[') return std::nullopt;

    if (c == '"') {
        // Parse a JSON string, with basic escape handling (enough for our generator output).
        std::string out;
        out.reserve(32);
        size_t i = pos + 1;
        while (i < json.size()) {
            char ch = json[i++];
            if (ch == '"') break;
            if (ch == '\\' && i < json.size()) {
                char esc = json[i++];
                switch (esc) {
                case '"':
                    out.push_back('"');
                    break;
                case '\\':
                    out.push_back('\\');
                    break;
                case 'n':
                    out.push_back('\n');
                    break;
                case 'r':
                    out.push_back('\r');
                    break;
                case 't':
                    out.push_back('\t');
                    break;
                default:
                    out.push_back(esc);
                    break;
                }
            } else {
                out.push_back(ch);
            }
        }
        return std::make_optional(std::make_pair('s', out));
    }

    // number / bool / null (generator doesn't emit null, but handle anyway)
    size_t i = pos;
    while (i < json.size()) {
        char ch = json[i];
        if (ch == ',' || ch == '}') break;
        i++;
    }
    if (i == pos) return std::nullopt;
    std::string token = json.substr(pos, i - pos);
    return std::make_optional(std::make_pair('t', token));
}

struct FieldStats {
    std::vector<std::string> values_as_string; // for string/datetime
    std::vector<int64_t> values_as_int;        // for int
    std::vector<std::string> values_as_token;  // for bool tokens "true"/"false"
};

class QueryGenerator {
public:
    QueryGenerator(std::vector<std::string> field_names, std::vector<FieldType> field_types,
                   std::vector<Cardinality> card)
            : field_names_(std::move(field_names)),
              field_types_(std::move(field_types)),
              cardinalities_(std::move(card)) {}

    void analyze_sample_records(const std::vector<std::string>& sample_jsonl) {
        stats_.clear();
        for (size_t i = 0; i < field_names_.size(); i++) {
            stats_[field_names_[i]] = FieldStats{};
        }

        for (const auto& line : sample_jsonl) {
            for (size_t i = 0; i < field_names_.size(); i++) {
                const auto& name = field_names_[i];
                FieldType t = field_types_[i];
                auto v = extract_top_level_simple_value(line, name);
                if (!v.has_value()) continue;
                if (t == FieldType::OBJECT || t == FieldType::ARRAY) continue;

                if (v->first == 's') {
                    if (t == FieldType::STRING || t == FieldType::DATETIME) {
                        stats_[name].values_as_string.push_back(v->second);
                    }
                } else {
                    // token: number/bool/null
                    if (t == FieldType::INT) {
                        try {
                            stats_[name].values_as_int.push_back(std::stoll(v->second));
                        } catch (...) {
                            // ignore
                        }
                    } else if (t == FieldType::BOOL) {
                        if (v->second == "true" || v->second == "false") {
                            stats_[name].values_as_token.push_back(v->second);
                        }
                    }
                }
            }
        }
    }

    std::string generate_filter_query(std::mt19937& rng, const std::string& table_name,
                                      const std::string& json_column) const {
        auto candidates = get_candidates([this](size_t i) {
            return cardinalities_[i] == Cardinality::LOW || cardinalities_[i] == Cardinality::MEDIUM;
        });
        if (candidates.empty()) {
            candidates = get_candidates();
        }
        if (candidates.empty()) {
            return fmt::format("SELECT * FROM {} LIMIT 10;", table_name);
        }

        auto selected = pick_random(rng, candidates, 1, 2);
        std::vector<std::string> conditions;
        for (size_t idx : selected) {
            auto cond = generate_condition(rng, idx, json_column);
            if (cond) conditions.push_back(*cond);
        }

        if (conditions.empty()) return fmt::format("SELECT * FROM {} LIMIT 10;", table_name);
        return fmt::format("SELECT * FROM {} WHERE {} LIMIT 100;", table_name, join_strings(conditions, " AND "));
    }

    std::string generate_select_query(std::mt19937& rng, const std::string& table_name,
                                      const std::string& json_column) const {
        auto available = get_candidates();
        if (available.empty()) return fmt::format("SELECT * FROM {} LIMIT 10;", table_name);

        auto selected = pick_random(rng, available, 2, 5);
        std::vector<std::string> select_parts;
        select_parts.emplace_back("id");
        for (size_t idx : selected) {
            const auto& name = field_names_[idx];
            FieldType t = field_types_[idx];
            select_parts.push_back(fmt::format("{} as {}", json_extract_expr(name, t, json_column), name));
        }
        return fmt::format("SELECT {} FROM {} LIMIT 100;", join_strings(select_parts, ", "), table_name);
    }

    std::string generate_aggregation_query(std::mt19937& rng, const std::string& table_name,
                                           const std::string& json_column) const {
        auto group_by_fields = get_candidates([this](size_t i) {
            FieldType t = field_types_[i];
            return (cardinalities_[i] == Cardinality::LOW || cardinalities_[i] == Cardinality::MEDIUM) &&
                   (t == FieldType::STRING || t == FieldType::INT || t == FieldType::BOOL);
        });
        auto agg_fields = get_candidates([this](size_t i) { return field_types_[i] == FieldType::INT; });

        if (group_by_fields.empty() && agg_fields.empty()) {
            return fmt::format("SELECT COUNT(*) as cnt FROM {};", table_name);
        }

        if (!group_by_fields.empty()) {
            size_t group_idx = pick_random(rng, group_by_fields, 1, 1)[0];
            const auto& group_name = field_names_[group_idx];
            FieldType group_t = field_types_[group_idx];
            std::string group_expr = json_extract_expr(group_name, group_t, json_column);

            std::vector<std::string> select_parts;
            select_parts.push_back(fmt::format("{} as {}", group_expr, group_name));

            std::string agg_alias = "cnt";
            if (!agg_fields.empty()) {
                size_t agg_idx = pick_random(rng, agg_fields, 1, 1)[0];
                std::vector<std::string> agg_types = {"COUNT", "AVG", "SUM", "MAX", "MIN"};
                std::uniform_int_distribution<size_t> td(0, agg_types.size() - 1);
                std::string agg_type = agg_types[td(rng)];
                if (agg_type == "COUNT") {
                    select_parts.emplace_back("COUNT(*) as cnt");
                } else {
                    agg_alias = to_lower(agg_type) + "_value";
                    std::string agg_expr = json_extract_expr(field_names_[agg_idx], field_types_[agg_idx], json_column);
                    select_parts.push_back(fmt::format("{}({}) as {}", agg_type, agg_expr, agg_alias));
                }
            } else {
                select_parts.emplace_back("COUNT(*) as cnt");
            }
            return fmt::format("SELECT {} FROM {} GROUP BY {} ORDER BY {} DESC LIMIT 20;",
                               join_strings(select_parts, ", "), table_name, group_expr, agg_alias);
        }

        // No GROUP BY, do simple aggregation
        if (!agg_fields.empty()) {
            size_t agg_idx = pick_random(rng, agg_fields, 1, 1)[0];
            std::vector<std::string> agg_types = {"AVG", "SUM", "MAX", "MIN", "COUNT"};
            std::uniform_int_distribution<size_t> td(0, agg_types.size() - 1);
            std::string agg_type = agg_types[td(rng)];
            if (agg_type == "COUNT") return fmt::format("SELECT COUNT(*) as total_count FROM {};", table_name);
            std::string agg_expr = json_extract_expr(field_names_[agg_idx], field_types_[agg_idx], json_column);
            return fmt::format("SELECT {}({}) as {}_value FROM {};", agg_type, agg_expr, to_lower(agg_type),
                               table_name);
        }
        return fmt::format("SELECT COUNT(*) as total_count FROM {};", table_name);
    }

private:
    std::vector<std::string> field_names_;
    std::vector<FieldType> field_types_;
    std::vector<Cardinality> cardinalities_;
    std::unordered_map<std::string, FieldStats> stats_;

    std::vector<size_t> get_candidates(const std::function<bool(size_t)>& predicate = nullptr) const {
        std::vector<size_t> candidates;
        for (size_t i = 0; i < field_names_.size(); i++) {
            auto it = stats_.find(field_names_[i]);
            if (it == stats_.end() || !has_any_values(it->second)) continue;
            if (!predicate || predicate(i)) {
                candidates.push_back(i);
            }
        }
        return candidates;
    }

    template <typename T>
    std::vector<T> pick_random(std::mt19937& rng, std::vector<T> candidates, int min_n, int max_n) const {
        if (candidates.empty()) return {};
        std::uniform_int_distribution<int> num_dist(min_n, max_n);
        int num = std::min<int>(num_dist(rng), static_cast<int>(candidates.size()));
        std::shuffle(candidates.begin(), candidates.end(), rng);
        candidates.resize(num);
        return candidates;
    }

    std::optional<std::string> generate_condition(std::mt19937& rng, size_t idx, const std::string& json_column) const {
        const auto& name = field_names_[idx];
        FieldType t = field_types_[idx];
        Cardinality card = cardinalities_[idx];
        const auto& st = stats_.at(name);
        std::string json_expr = json_extract_expr(name, t, json_column);

        if (t == FieldType::STRING) {
            if (st.values_as_string.empty()) return std::nullopt;
            std::uniform_int_distribution<size_t> dist(0, st.values_as_string.size() - 1);
            const std::string& v = st.values_as_string[dist(rng)];
            std::uniform_int_distribution<int> p(0, 99);
            if (p(rng) < 70 || card == Cardinality::LOW) {
                return fmt::format("{} = '{}'", json_expr, escape_sql_string(v));
            } else {
                std::string prefix = v.substr(0, std::min<size_t>(5, v.size()));
                return fmt::format("{} LIKE '{}%'", json_expr, escape_sql_string(prefix));
            }
        } else if (t == FieldType::INT) {
            if (st.values_as_int.empty()) return std::nullopt;
            std::uniform_int_distribution<size_t> dist(0, st.values_as_int.size() - 1);
            int64_t v = st.values_as_int[dist(rng)];
            std::uniform_int_distribution<int> p(0, 99);
            if (p(rng) < 50) {
                return fmt::format("{} = {}", json_expr, v);
            } else {
                auto [min_it, max_it] = std::minmax_element(st.values_as_int.begin(), st.values_as_int.end());
                int64_t min_v = *min_it;
                int64_t max_v = *max_it;
                if (min_v != max_v) {
                    std::uniform_int_distribution<int64_t> thresh(min_v, max_v);
                    return fmt::format("{} >= {}", json_expr, thresh(rng));
                } else {
                    return fmt::format("{} = {}", json_expr, v);
                }
            }
        } else if (t == FieldType::BOOL) {
            if (st.values_as_token.empty()) return std::nullopt;
            std::uniform_int_distribution<size_t> dist(0, st.values_as_token.size() - 1);
            const std::string& v = st.values_as_token[dist(rng)];
            return fmt::format("{} = {}", json_expr, v);
        } else if (t == FieldType::DATETIME) {
            if (st.values_as_string.empty()) return std::nullopt;
            std::uniform_int_distribution<size_t> dist(0, st.values_as_string.size() - 1);
            const std::string& v = st.values_as_string[dist(rng)];
            return fmt::format("{} = '{}'", json_expr, escape_sql_string(v));
        }
        return std::nullopt;
    }

    static bool has_any_values(const FieldStats& s) {
        return !s.values_as_string.empty() || !s.values_as_int.empty() || !s.values_as_token.empty();
    }

    static std::string to_lower(std::string s) { return to_lower_ascii(std::move(s)); }

    static std::string escape_sql_string(const std::string& s) {
        std::string out;
        out.reserve(s.size() + 8);
        for (char c : s) {
            if (c == '\'') out.push_back('\'');
            out.push_back(c);
        }
        return out;
    }
};

static std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    size_t pos = 0;
    while (pos < s.size()) {
        size_t comma = s.find(',', pos);
        if (comma == std::string::npos) comma = s.size();
        std::string token = s.substr(pos, comma - pos);
        while (!token.empty() && token.front() == ' ') token.erase(token.begin());
        while (!token.empty() && token.back() == ' ') token.pop_back();
        if (!token.empty()) out.push_back(token);
        pos = comma + 1;
    }
    return out;
}

static CliOptions parse_args(int argc, char* argv[]) {
    CliOptions opt;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--num-records") == 0 && i + 1 < argc) {
            opt.num_records = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--num-fields") == 0 && i + 1 < argc) {
            opt.num_fields = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--sparsity") == 0 && i + 1 < argc) {
            opt.sparsity = std::stod(argv[++i]);
        } else if (strcmp(argv[i], "--max-depth") == 0 && i + 1 < argc) {
            opt.max_depth = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--nest-probability") == 0 && i + 1 < argc) {
            opt.nest_probability = std::stod(argv[++i]);
        } else if (strcmp(argv[i], "--field-types") == 0 && i + 1 < argc) {
            opt.field_types = parse_field_types(argv[++i]);
        } else if (strcmp(argv[i], "--high-cardinality-fields") == 0 && i + 1 < argc) {
            opt.high_cardinality_fields = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--low-cardinality-fields") == 0 && i + 1 < argc) {
            opt.low_cardinality_fields = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) {
            opt.seed = std::stoll(argv[++i]);
        } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
            opt.output_path = argv[++i];
        } else if (strcmp(argv[i], "--gen-query-type") == 0 && i + 1 < argc) {
            opt.gen_query_type = argv[++i];
        } else if (strcmp(argv[i], "--gen-query-num") == 0 && i + 1 < argc) {
            opt.gen_query_num = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--gen-query-output") == 0 && i + 1 < argc) {
            opt.gen_query_output = argv[++i];
        } else if (strcmp(argv[i], "--gen-query-table") == 0 && i + 1 < argc) {
            opt.gen_query_table = argv[++i];
        } else if (strcmp(argv[i], "--gen-query-column") == 0 && i + 1 < argc) {
            opt.gen_query_column = argv[++i];
        }
    }
    return opt;
}

int main(int argc, char* argv[]) {
    CliOptions opt = parse_args(argc, argv);

    // Create generator
    JSONGenerator generator(opt.num_fields, opt.sparsity, opt.max_depth, opt.nest_probability, opt.field_types,
                            opt.high_cardinality_fields, opt.low_cardinality_fields, opt.seed);

    std::ostream* data_out = &std::cout;
    std::ofstream data_file;
    if (!opt.output_path.empty()) {
        data_file.open(opt.output_path, std::ios::out | std::ios::trunc);
        data_out = &data_file;
    }

    std::vector<std::string> sample_jsonl;
    int sample_size = 0;
    if (!opt.gen_query_type.empty()) {
        sample_size = std::min(100, std::max(0, opt.num_records));
        sample_jsonl.reserve(sample_size);
    }

    for (int i = 0; i < opt.num_records; ++i) {
        std::string rec = generator.generate_record();
        (*data_out) << rec << "\n";
        if (sample_size > 0 && static_cast<int>(sample_jsonl.size()) < sample_size) {
            sample_jsonl.push_back(std::move(rec));
        }
    }

    if (!opt.gen_query_type.empty()) {
        std::vector<std::string> qtypes = split_csv(opt.gen_query_type);
        QueryGenerator qg(generator.field_names(), generator.field_types_for_schema(), generator.field_cardinalities());
        qg.analyze_sample_records(sample_jsonl);

        std::ostream* q_out = &std::cout;
        std::ofstream q_file;
        if (!opt.gen_query_output.empty()) {
            q_file.open(opt.gen_query_output, std::ios::out | std::ios::trunc);
            q_out = &q_file;
        }

        std::mt19937 q_rng(opt.seed >= 0 ? opt.seed : std::random_device{}());
        for (const auto& qt : qtypes) {
            for (int i = 0; i < opt.gen_query_num; i++) {
                std::string sql;
                if (qt == "filter") {
                    sql = qg.generate_filter_query(q_rng, opt.gen_query_table, opt.gen_query_column);
                } else if (qt == "aggregation") {
                    sql = qg.generate_aggregation_query(q_rng, opt.gen_query_table, opt.gen_query_column);
                } else if (qt == "select") {
                    sql = qg.generate_select_query(q_rng, opt.gen_query_table, opt.gen_query_column);
                } else {
                    // Ignore unknown type
                    continue;
                }
                (*q_out) << sql << "\n";
            }
        }
    }

    return 0;
}
