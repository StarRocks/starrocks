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

#include <iostream>
#include <sstream>
#include <random>
#include <vector>
#include <string>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <cstring>

enum class FieldType {
    STRING,
    INT,
    BOOL,
    DATETIME,
    ARRAY,
    OBJECT
};

enum class Cardinality {
    HIGH,
    MEDIUM,
    LOW
};

class ValueGenerator {
public:
    explicit ValueGenerator(int64_t seed = -1) 
        : rng_(seed >= 0 ? seed : std::random_device{}()),
          string_counter_(0),
          int_counter_(0) {
    }
    
    std::string generate_string(Cardinality cardinality) {
        if (cardinality == Cardinality::HIGH) {
            string_counter_++;
            std::string random_part = generate_random_string(8);
            return "unique_value_" + std::to_string(string_counter_) + "_" + random_part;
        } else if (cardinality == Cardinality::LOW) {
            static const std::vector<std::string> low_strings = {
                "red", "green", "blue", "yellow", "purple", "orange", "pink", "brown", "black", "white"
            };
            std::uniform_int_distribution<size_t> dist(0, low_strings.size() - 1);
            return low_strings[dist(rng_)];
        } else { // MEDIUM
            static const std::vector<std::string> bases = {
                "value", "data", "item", "record", "entry"
            };
            std::uniform_int_distribution<size_t> base_dist(0, bases.size() - 1);
            std::uniform_int_distribution<int> num_dist(1, 100);
            return bases[base_dist(rng_)] + "_" + std::to_string(num_dist(rng_));
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
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

private:
    std::mt19937 rng_;
    int64_t string_counter_;
    int64_t int_counter_;
    
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
    JSONGenerator(
        int num_fields,
        double sparsity,
        int max_depth,
        double nest_probability,
        const std::vector<FieldType>& field_types,
        int high_cardinality_fields,
        int low_cardinality_fields,
        int64_t seed
    ) : num_fields_(num_fields),
        sparsity_(sparsity),
        max_depth_(max_depth),
        nest_probability_(nest_probability),
        field_types_(field_types.empty() ? std::vector<FieldType>{FieldType::STRING, FieldType::INT, FieldType::BOOL} : field_types),
        high_cardinality_fields_(high_cardinality_fields),
        low_cardinality_fields_(low_cardinality_fields),
        rng_(seed >= 0 ? seed : std::random_device{}()),
        value_gen_(seed) {
        generate_field_schemas();
    }
    
    std::string generate_record() {
        std::ostringstream oss;
        oss << "{";
        
        bool first = true;
        std::uniform_real_distribution<double> sparsity_dist(0.0, 1.0);
        
        for (const auto& schema : field_schemas_) {
            if (sparsity_dist(rng_) < sparsity_) {
                continue;
            }
            
            if (!first) {
                oss << ",";
            }
            first = false;
            
            oss << escape_json_string(schema.name) << ":" << generate_value_json(schema);
        }
        
        oss << "}";
        return oss.str();
    }

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
            
            std::string field_name = "field_" + std::to_string(i + 1);
            FieldType field_type = choose_field_type();
            
            field_schemas_.push_back({field_name, field_type, cardinality});
        }
    }
    
    FieldType choose_field_type() {
        std::uniform_int_distribution<size_t> dist(0, field_types_.size() - 1);
        return field_types_[dist(rng_)];
    }
    
    Cardinality choose_cardinality_for_nested() {
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        double r = dist(rng_);
        if (r < 0.3) return Cardinality::HIGH;
        if (r < 0.5) return Cardinality::LOW;
        return Cardinality::MEDIUM;
    }
    
    std::string escape_json_string(const std::string& str) {
        std::ostringstream oss;
        oss << "\"";
        for (char c : str) {
            if (c == '"' || c == '\\') {
                oss << "\\" << c;
            } else if (c == '\n') {
                oss << "\\n";
            } else if (c == '\r') {
                oss << "\\r";
            } else if (c == '\t') {
                oss << "\\t";
            } else {
                oss << c;
            }
        }
        oss << "\"";
        return oss.str();
    }
    
    std::string generate_nested_object_json(int depth) {
        if (depth >= max_depth_) {
            return escape_json_string(value_gen_.generate_string(Cardinality::MEDIUM));
        }
        
        std::ostringstream oss;
        oss << "{";
        
        std::uniform_int_distribution<int> num_fields_dist(2, std::min(5, num_fields_));
        int num_nested_fields = num_fields_dist(rng_);
        
        bool first = true;
        for (int i = 0; i < num_nested_fields; ++i) {
            if (!first) {
                oss << ",";
            }
            first = false;
            
            std::string nested_field_name = "nested_" + std::to_string(depth) + "_" + std::to_string(i + 1);
            FieldType nested_type = choose_field_type();
            Cardinality nested_cardinality = choose_cardinality_for_nested();
            
            FieldSchema nested_schema{nested_field_name, nested_type, nested_cardinality};
            
            oss << escape_json_string(nested_field_name) << ":";
            
            if (nested_type == FieldType::OBJECT && depth + 1 < max_depth_) {
                oss << generate_nested_object_json(depth + 1);
            } else {
                oss << generate_value_json(nested_schema, depth + 1);
            }
        }
        
        oss << "}";
        return oss.str();
    }
    
    std::string generate_value_json(const FieldSchema& schema, int depth = 0) {
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        bool should_nest = (schema.field_type == FieldType::OBJECT) ||
                           (depth < max_depth_ && 
                            std::find(field_types_.begin(), field_types_.end(), schema.field_type) != field_types_.end() &&
                            dist(rng_) < nest_probability_);
        
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
                std::ostringstream oss;
                oss << "[";
                int array_size = 3;
                bool first = true;
                for (int i = 0; i < array_size; ++i) {
                    if (!first) {
                        oss << ",";
                    }
                    first = false;
                    FieldSchema array_schema{"", array_type, schema.cardinality};
                    oss << generate_value_json(array_schema, depth);
                }
                oss << "]";
                return oss.str();
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
        while (!type.empty() && type[type.length()-1] == ' ') type.erase(type.length()-1);
        if (!type.empty()) {
            result.push_back(parse_field_type(type.c_str()));
        }
        pos = comma + 1;
    }
    return result;
}

int main(int argc, char* argv[]) {
    // Default parameters
    int num_records = 1000;
    int num_fields = 10;
    double sparsity = 0.0;
    int max_depth = 4;
    double nest_probability = 0.2;
    std::vector<FieldType> field_types = {FieldType::STRING, FieldType::INT, FieldType::BOOL};
    int high_cardinality_fields = 0;
    int low_cardinality_fields = 0;
    int64_t seed = -1;
    bool pretty = false;
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--num-records") == 0 && i + 1 < argc) {
            num_records = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--num-fields") == 0 && i + 1 < argc) {
            num_fields = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--sparsity") == 0 && i + 1 < argc) {
            sparsity = std::stod(argv[++i]);
        } else if (strcmp(argv[i], "--max-depth") == 0 && i + 1 < argc) {
            max_depth = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--nest-probability") == 0 && i + 1 < argc) {
            nest_probability = std::stod(argv[++i]);
        } else if (strcmp(argv[i], "--field-types") == 0 && i + 1 < argc) {
            field_types = parse_field_types(argv[++i]);
        } else if (strcmp(argv[i], "--high-cardinality-fields") == 0 && i + 1 < argc) {
            high_cardinality_fields = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--low-cardinality-fields") == 0 && i + 1 < argc) {
            low_cardinality_fields = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) {
            seed = std::stoll(argv[++i]);
        } else if (strcmp(argv[i], "--pretty") == 0) {
            pretty = true;
        }
    }
    
    // Create generator
    JSONGenerator generator(
        num_fields, sparsity, max_depth, nest_probability,
        field_types, high_cardinality_fields, low_cardinality_fields, seed
    );
    
    // Generate records
    for (int i = 0; i < num_records; ++i) {
        std::string json_str = generator.generate_record();
        
        if (pretty) {
            // Simple pretty printing (basic indentation)
            // For full pretty printing, we'd need a JSON parser, but this is simpler
            std::cout << json_str << std::endl;
        } else {
            std::cout << json_str << std::endl;
        }
    }
    
    return 0;
}
