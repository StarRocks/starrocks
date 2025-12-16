#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import json
import random
import string
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple


class FieldSchema:
    """Represents a field schema with type and cardinality information."""
    
    def __init__(self, name: str, field_type: str, cardinality: str = 'medium'):
        self.name = name
        self.field_type = field_type
        self.cardinality = cardinality  # 'high', 'low', or 'medium'
    
    def __repr__(self):
        return f"FieldSchema(name={self.name}, type={self.field_type}, cardinality={self.cardinality})"


class ValueGenerator:
    """Generates values based on field type and cardinality."""
    
    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        self._low_cardinality_strings = [
            "red", "green", "blue", "yellow", "purple", "orange", "pink", "brown", "black", "white"
        ]
        self._low_cardinality_ints = [100, 200, 300, 400, 500]
        self._string_counter = 0
        self._int_counter = 0
    
    def generate_string(self, cardinality: str) -> str:
        """Generate a string value based on cardinality."""
        if cardinality == 'high':
            # Generate unique or near-unique values
            self._string_counter += 1
            random_part = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            return f"unique_value_{self._string_counter}_{random_part}"
        elif cardinality == 'low':
            # Generate from limited set
            return random.choice(self._low_cardinality_strings)
        else:  # medium
            # Generate semi-unique values with some repetition
            base = random.choice(["value", "data", "item", "record", "entry"])
            num = random.randint(1, 100)
            return f"{base}_{num}"
    
    def generate_int(self, cardinality: str) -> int:
        """Generate an integer value based on cardinality."""
        if cardinality == 'high':
            # Generate unique or near-unique values
            self._int_counter += 1
            return random.randint(100000000, 999999999) + self._int_counter
        elif cardinality == 'low':
            # Generate from limited set
            return random.choice(self._low_cardinality_ints)
        else:  # medium
            # Generate from moderate range
            return random.randint(1, 10000)
    
    def generate_bool(self) -> bool:
        """Generate a boolean value."""
        return random.choice([True, False])
    
    def generate_datetime(self) -> str:
        """Generate a datetime-formatted string."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2025, 12, 31)
        random_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        return random_date.strftime('%Y-%m-%d %H:%M:%S')
    
    def generate_array(self, field_type: str, cardinality: str, size: int = 3) -> List[Any]:
        """Generate an array of values."""
        array = []
        for _ in range(size):
            if field_type == 'string':
                array.append(self.generate_string(cardinality))
            elif field_type == 'int':
                array.append(self.generate_int(cardinality))
            elif field_type == 'bool':
                array.append(self.generate_bool())
            elif field_type == 'datetime':
                array.append(self.generate_datetime())
            else:
                array.append(self.generate_string(cardinality))
        return array


class JSONDataGenerator:
    """Main class for generating JSON data with configurable characteristics."""
    
    def __init__(self, 
                 num_fields: int = 10,
                 sparsity: float = 0.0,
                 max_depth: int = 4,
                 nest_probability: float = 0.2,
                 field_types: List[str] = None,
                 high_cardinality_fields: int = 0,
                 low_cardinality_fields: int = 0,
                 seed: Optional[int] = None):
        self.num_fields = num_fields
        self.sparsity = sparsity
        self.max_depth = max_depth
        self.nest_probability = nest_probability
        self.field_types = field_types or ['string', 'int', 'bool']
        self.high_cardinality_fields = high_cardinality_fields
        self.low_cardinality_fields = low_cardinality_fields
        self.seed = seed
        
        if seed is not None:
            random.seed(seed)
        
        self.value_generator = ValueGenerator(seed)
        self.field_schemas = self._generate_field_schemas()
    
    def _generate_field_schemas(self) -> List[FieldSchema]:
        """Generate field schemas with types and cardinality assignments."""
        schemas = []
        
        # Determine cardinality for each field
        field_cardinalities = []
        for i in range(self.num_fields):
            if i < self.high_cardinality_fields:
                field_cardinalities.append('high')
            elif i < self.high_cardinality_fields + self.low_cardinality_fields:
                field_cardinalities.append('low')
            else:
                field_cardinalities.append('medium')
        
        # Generate field schemas
        for i in range(self.num_fields):
            field_name = f"field_{i+1}"
            field_type = random.choice(self.field_types)
            cardinality = field_cardinalities[i]
            schemas.append(FieldSchema(field_name, field_type, cardinality))
        
        return schemas
    
    def _generate_value(self, schema: FieldSchema, depth: int = 0) -> Any:
        """Generate a value for a field schema, supporting nesting."""
        # Check if we should create a nested object
        if (schema.field_type == 'object' or 
            (schema.field_type in self.field_types and 
             depth < self.max_depth and 
             random.random() < self.nest_probability)):
            return self._generate_nested_object(depth + 1)
        
        # Generate value based on type
        if schema.field_type == 'string':
            return self.value_generator.generate_string(schema.cardinality)
        elif schema.field_type == 'int':
            return self.value_generator.generate_int(schema.cardinality)
        elif schema.field_type == 'bool':
            return self.value_generator.generate_bool()
        elif schema.field_type == 'datetime':
            return self.value_generator.generate_datetime()
        elif schema.field_type == 'array':
            # For arrays, use the first non-array type or default to string
            array_type = next((t for t in self.field_types if t != 'array'), 'string')
            return self.value_generator.generate_array(array_type, schema.cardinality)
        elif schema.field_type == 'object':
            return self._generate_nested_object(depth + 1)
        else:
            return self.value_generator.generate_string(schema.cardinality)
    
    def _generate_nested_object(self, depth: int = 0) -> Dict[str, Any]:
        """Generate a nested object with random fields."""
        if depth >= self.max_depth:
            # At max depth, generate a simple value
            return self.value_generator.generate_string('medium')
        
        nested_obj = {}
        num_nested_fields = random.randint(2, min(5, self.num_fields))
        
        for i in range(num_nested_fields):
            nested_field_name = f"nested_{depth}_{i+1}"
            # Choose a type for nested field
            nested_type = random.choice(self.field_types)
            
            # Determine cardinality (use medium for nested fields by default)
            nested_cardinality = 'medium'
            if random.random() < 0.3:  # 30% chance of high cardinality
                nested_cardinality = 'high'
            elif random.random() < 0.5:  # 50% chance of low cardinality
                nested_cardinality = 'low'
            
            nested_schema = FieldSchema(nested_field_name, nested_type, nested_cardinality)
            
            # Recursively generate nested value
            if nested_type == 'object' and depth + 1 < self.max_depth:
                nested_obj[nested_field_name] = self._generate_nested_object(depth + 1)
            else:
                nested_obj[nested_field_name] = self._generate_value(nested_schema, depth + 1)
        
        return nested_obj
    
    def generate_record(self) -> Dict[str, Any]:
        """Generate a single JSON record."""
        record = {}
        
        for schema in self.field_schemas:
            # Apply sparsity: randomly omit fields
            if random.random() < self.sparsity:
                continue  # Omit this field
            
            record[schema.name] = self._generate_value(schema)
        
        return record
    
    def generate(self, num_records: int, output_file: Optional[str] = None, pretty: bool = False):
        """Generate JSONL records and write to file or stdout."""
        output = open(output_file, 'w') if output_file else sys.stdout
        
        try:
            for i in range(num_records):
                record = self.generate_record()
                if pretty:
                    json_str = json.dumps(record, indent=2, ensure_ascii=False)
                    output.write(json_str + '\n')
                else:
                    json_str = json.dumps(record, ensure_ascii=False)
                    output.write(json_str + '\n')
        finally:
            if output_file and output != sys.stdout:
                output.close()
    
    def generate_sample_records(self, num_samples: int = 100) -> List[Dict[str, Any]]:
        """Generate sample records for query generation analysis."""
        samples = []
        for _ in range(num_samples):
            samples.append(self.generate_record())
        return samples


class QueryGenerator:
    """Generates SQL queries for testing JSON data."""
    
    def __init__(self, generator: 'JSONDataGenerator', sample_data: List[Dict[str, Any]]):
        self.generator = generator
        self.sample_data = sample_data
        self.field_schemas = generator.field_schemas
        self._analyze_sample_data()
    
    def _analyze_sample_data(self):
        """Analyze sample data to extract value distributions for query generation."""
        self.field_values = {}  # field_name -> list of values
        
        for schema in self.field_schemas:
            values = []
            for record in self.sample_data:
                if schema.name in record:
                    value = record[schema.name]
                    # Only collect simple values (not nested objects/arrays for filter queries)
                    if not isinstance(value, (dict, list)):
                        values.append(value)
            
            if values:
                self.field_values[schema.name] = values
    
    def _get_json_extraction_expr(self, field_name: str, field_type: str, json_column: str = "json_data") -> str:
        """Generate JSON extraction expression using appropriate function based on field type."""
        json_path = f"$.{field_name}"
        
        if field_type == 'int':
            return f"get_json_int({json_column}, '{json_path}')"
        elif field_type == 'string':
            return f"get_json_string({json_column}, '{json_path}')"
        elif field_type == 'bool':
            # For boolean, use get_json_string and cast, or use get_json_int if stored as 0/1
            # We'll use get_json_string and cast for better compatibility
            return f"CAST(get_json_string({json_column}, '{json_path}') AS BOOLEAN)"
        elif field_type == 'datetime':
            return f"get_json_string({json_column}, '{json_path}')"
        else:
            # Default to string extraction
            return f"get_json_string({json_column}, '{json_path}')"
    
    def generate_filter_query(self, table_name: str = "json_test_table", json_column: str = "json_data") -> str:
        """Generate a filter query that is likely to return results."""
        # Select 1-3 fields for filtering, prefer low/medium cardinality fields
        filter_fields = []
        for schema in self.field_schemas:
            if schema.name in self.field_values and schema.cardinality in ['low', 'medium']:
                filter_fields.append(schema)
        
        if not filter_fields:
            # Fallback to any field with values
            for schema in self.field_schemas:
                if schema.name in self.field_values:
                    filter_fields.append(schema)
                    break
        
        if not filter_fields:
            return f"SELECT * FROM {table_name} LIMIT 10;"
        
        # Select 1-2 fields randomly
        num_filters = min(random.randint(1, 2), len(filter_fields))
        selected_fields = random.sample(filter_fields, num_filters)
        
        conditions = []
        for schema in selected_fields:
            values = self.field_values[schema.name]
            if not values:
                continue
            
            # Pick a value that exists in the data
            filter_value = random.choice(values)
            
            # Get JSON extraction expression for this field
            json_expr = self._get_json_extraction_expr(schema.name, schema.field_type, json_column)
            
            if schema.field_type == 'string':
                # String equality or LIKE
                if random.random() < 0.7:
                    # Exact match
                    conditions.append(f"{json_expr} = '{filter_value}'")
                else:
                    # LIKE pattern (for low cardinality, use exact match)
                    if schema.cardinality == 'low':
                        conditions.append(f"{json_expr} = '{filter_value}'")
                    else:
                        # Use LIKE with prefix
                        prefix = str(filter_value)[:min(5, len(str(filter_value)))]
                        conditions.append(f"{json_expr} LIKE '{prefix}%'")
            elif schema.field_type == 'int':
                # Integer comparison
                if random.random() < 0.5:
                    # Equality
                    conditions.append(f"{json_expr} = {filter_value}")
                else:
                    # Range query
                    min_val = min([v for v in values if isinstance(v, int)], default=filter_value)
                    max_val = max([v for v in values if isinstance(v, int)], default=filter_value)
                    if min_val != max_val:
                        threshold = random.randint(min_val, max_val)
                        conditions.append(f"{json_expr} >= {threshold}")
            elif schema.field_type == 'bool':
                conditions.append(f"{json_expr} = {filter_value}")
            elif schema.field_type == 'datetime':
                conditions.append(f"{json_expr} = '{filter_value}'")
        
        if not conditions:
            return f"SELECT * FROM {table_name} LIMIT 10;"
        
        where_clause = " AND ".join(conditions)
        return f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT 100;"
    
    def generate_aggregation_query(self, table_name: str = "json_test_table", json_column: str = "json_data") -> str:
        """Generate an aggregation query (GROUP BY, COUNT, AVG, etc.)."""
        # Find suitable fields for aggregation (prefer low/medium cardinality)
        group_by_fields = []
        aggregate_fields = []
        
        for schema in self.field_schemas:
            if schema.name in self.field_values:
                if schema.cardinality in ['low', 'medium'] and schema.field_type in ['string', 'int', 'bool']:
                    group_by_fields.append(schema)
                if schema.field_type == 'int':
                    aggregate_fields.append(schema)
        
        if not group_by_fields and not aggregate_fields:
            return f"SELECT COUNT(*) as cnt FROM {table_name};"
        
        # Generate GROUP BY query
        if group_by_fields:
            group_field = random.choice(group_by_fields)
            group_expr = self._get_json_extraction_expr(group_field.name, group_field.field_type, json_column)
            select_parts = [f"{group_expr} as {group_field.name}"]
            
            # Add aggregations
            agg_alias = "cnt"
            if aggregate_fields:
                agg_field = random.choice(aggregate_fields)
                agg_type = random.choice(['COUNT', 'AVG', 'SUM', 'MAX', 'MIN'])
                if agg_type == 'COUNT':
                    select_parts.append(f"COUNT(*) as {agg_alias}")
                else:
                    agg_alias = f"{agg_type.lower()}_value"
                    agg_expr = self._get_json_extraction_expr(agg_field.name, agg_field.field_type, json_column)
                    select_parts.append(f"{agg_type}({agg_expr}) as {agg_alias}")
            else:
                select_parts.append(f"COUNT(*) as {agg_alias}")
            
            return f"SELECT {', '.join(select_parts)} FROM {table_name} GROUP BY {group_expr} ORDER BY {agg_alias} DESC LIMIT 20;"
        else:
            # Simple aggregation without GROUP BY
            if aggregate_fields:
                agg_field = random.choice(aggregate_fields)
                agg_type = random.choice(['AVG', 'SUM', 'MAX', 'MIN', 'COUNT'])
                if agg_type == 'COUNT':
                    return f"SELECT COUNT(*) as total_count FROM {table_name};"
                else:
                    agg_expr = self._get_json_extraction_expr(agg_field.name, agg_field.field_type, json_column)
                    return f"SELECT {agg_type}({agg_expr}) as {agg_type.lower()}_value FROM {table_name};"
            else:
                return f"SELECT COUNT(*) as total_count FROM {table_name};"
    
    def generate_select_query(self, table_name: str = "json_test_table", json_column: str = "json_data") -> str:
        """Generate a simple SELECT query with field projections."""
        # Select 2-5 fields
        available_fields = [s for s in self.field_schemas if s.name in self.field_values]
        if not available_fields:
            return f"SELECT * FROM {table_name} LIMIT 10;"
        
        num_fields = min(random.randint(2, 5), len(available_fields))
        selected_fields = random.sample(available_fields, num_fields)
        
        select_parts = ["id"]
        for schema in selected_fields:
            json_expr = self._get_json_extraction_expr(schema.name, schema.field_type, json_column)
            select_parts.append(f"{json_expr} as {schema.name}")
        
        return f"SELECT {', '.join(select_parts)} FROM {table_name} LIMIT 100;"


def parse_field_types(field_types_str: str) -> List[str]:
    """Parse field types from comma-separated string."""
    valid_types = ['string', 'int', 'bool', 'datetime', 'array', 'object']
    types = [t.strip() for t in field_types_str.split(',')]
    
    for t in types:
        if t not in valid_types:
            raise ValueError(f"Invalid field type: {t}. Valid types are: {', '.join(valid_types)}")
    
    return types


def main():
    parser = argparse.ArgumentParser(
        description='Generate JSONL data files with configurable characteristics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 1000 records with 50 fields, 30% sparsity
  python json_data_generator.py --num-records 1000 --num-fields 50 --sparsity 0.3 --output data.jsonl
  
  # Generate with high and low cardinality fields
  python json_data_generator.py --num-records 100 --num-fields 20 \\
    --high-cardinality-fields 5 --low-cardinality-fields 10 --output data.jsonl
  
  # Generate with nested objects
  python json_data_generator.py --num-records 500 --max-depth 5 \\
    --nest-probability 0.3 --field-types string,int,object --output nested.jsonl
        """
    )
    
    parser.add_argument('--num-records', type=int, default=1000,
                       help='Number of JSON records to generate (default: 1000)')
    parser.add_argument('--num-fields', type=int, default=10,
                       help='Number of top-level fields per record (default: 10)')
    parser.add_argument('--sparsity', type=float, default=0.0,
                       help='Sparsity ratio (0.0 to 1.0). Fields are randomly omitted based on this ratio (default: 0.0)')
    parser.add_argument('--max-depth', type=int, default=4,
                       help='Maximum nesting depth for nested objects (default: 4)')
    parser.add_argument('--nest-probability', type=float, default=0.2,
                       help='Probability of creating nested objects (0.0 to 1.0) (default: 0.2)')
    parser.add_argument('--field-types', type=str, default='string,int,bool',
                       help='Comma-separated list of field types: string,int,bool,datetime,array,object (default: string,int,bool)')
    parser.add_argument('--high-cardinality-fields', type=int, default=0,
                       help='Number of fields with high cardinality (default: 0)')
    parser.add_argument('--low-cardinality-fields', type=int, default=0,
                       help='Number of fields with low cardinality (default: 0)')
    parser.add_argument('--seed', type=int, default=None,
                       help='Random seed for reproducible data generation (default: None)')
    parser.add_argument('--output', type=str, default=None,
                       help='Output file path (default: stdout)')
    parser.add_argument('--pretty', action='store_true',
                       help='Pretty-print JSON (default: False)')
    parser.add_argument('--gen-query-type', type=str,
                       help='Generate queries of specified type(s). Can be a single type or comma-separated list: filter,aggregation,select')
    parser.add_argument('--gen-query-num', type=int, default=10,
                       help='Number of queries to generate per type (default: 10). If multiple types specified, total queries = num_types * gen-query-num')
    parser.add_argument('--gen-query-output', type=str, default=None,
                       help='Output file for generated queries (default: stdout)')
    parser.add_argument('--gen-query-table', type=str, default='json_test_table',
                       help='Table name to use in generated queries (default: json_test_table)')
    parser.add_argument('--gen-query-column', type=str, default='json_data',
                       help='JSON column name to use in generated queries (default: json_data)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.num_records < 0:
        parser.error('--num-records must be non-negative')
    if args.num_fields < 0:
        parser.error('--num-fields must be non-negative')
    if not 0.0 <= args.sparsity <= 1.0:
        parser.error('--sparsity must be between 0.0 and 1.0')
    if args.max_depth < 1:
        parser.error('--max-depth must be at least 1')
    if not 0.0 <= args.nest_probability <= 1.0:
        parser.error('--nest-probability must be between 0.0 and 1.0')
    if args.high_cardinality_fields < 0:
        parser.error('--high-cardinality-fields must be non-negative')
    if args.low_cardinality_fields < 0:
        parser.error('--low-cardinality-fields must be non-negative')
    if args.high_cardinality_fields + args.low_cardinality_fields > args.num_fields:
        parser.error('Sum of --high-cardinality-fields and --low-cardinality-fields cannot exceed --num-fields')
    
    # Parse field types
    try:
        field_types = parse_field_types(args.field_types)
    except ValueError as e:
        parser.error(str(e))
    
    # Create generator
    generator = JSONDataGenerator(
        num_fields=args.num_fields,
        sparsity=args.sparsity,
        max_depth=args.max_depth,
        nest_probability=args.nest_probability,
        field_types=field_types,
        high_cardinality_fields=args.high_cardinality_fields,
        low_cardinality_fields=args.low_cardinality_fields,
        seed=args.seed
    )
    
    # Generate data
    generator.generate(args.num_records, args.output, args.pretty)
    
    # Generate queries if requested
    if args.gen_query_type:
        if args.gen_query_num < 1:
            parser.error('--gen-query-num must be at least 1')
        
        # Parse query types (support comma-separated list)
        valid_types = ['filter', 'aggregation', 'select']
        query_types = [t.strip() for t in args.gen_query_type.split(',')]
        
        # Validate query types
        for qtype in query_types:
            if qtype not in valid_types:
                parser.error(f'Invalid query type: {qtype}. Valid types are: {", ".join(valid_types)}')
        
        # Generate sample data for query analysis
        sample_size = min(100, args.num_records)
        sample_data = generator.generate_sample_records(sample_size)
        
        # Create query generator
        query_gen = QueryGenerator(generator, sample_data)
        
        # Generate queries for each type
        queries = []
        for qtype in query_types:
            for _ in range(args.gen_query_num):
                if qtype == 'filter':
                    query = query_gen.generate_filter_query(args.gen_query_table, args.gen_query_column)
                elif qtype == 'aggregation':
                    query = query_gen.generate_aggregation_query(args.gen_query_table, args.gen_query_column)
                else:  # select
                    query = query_gen.generate_select_query(args.gen_query_table, args.gen_query_column)
                queries.append(query)
        
        # Output queries
        query_output = open(args.gen_query_output, 'w') if args.gen_query_output else sys.stdout
        try:
            for query in queries:
                query_output.write(query + '\n')
        finally:
            if args.gen_query_output and query_output != sys.stdout:
                query_output.close()


if __name__ == '__main__':
    main()
