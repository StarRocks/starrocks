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
import sys
import subprocess
import os
from typing import Dict, List, Any, Optional


class FieldSchema:
    """Represents a field schema with type and cardinality information."""
    
    def __init__(self, name: str, field_type: str, cardinality: str = 'medium'):
        self.name = name
        self.field_type = field_type
        self.cardinality = cardinality  # 'high', 'low', or 'medium'
    
    def __repr__(self):
        return f"FieldSchema(name={self.name}, type={self.field_type}, cardinality={self.cardinality})"


class JSONDataGenerator:
    """Main class for generating JSON data with configurable characteristics.
    
    This class uses the C++ implementation for data generation. The C++ executable
    must be built before use (run ./build.sh).
    """
    
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
        
        # Check if C++ executable exists
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self._cpp_executable = os.path.join(script_dir, 'json_generator')
        
        if not os.path.exists(self._cpp_executable) or not os.access(self._cpp_executable, os.X_OK):
            raise RuntimeError(
                f"C++ executable not found: {self._cpp_executable}\n"
                "Please build it first by running: ./build.sh"
            )
    
    def generate(self, num_records: int, output_file: Optional[str] = None):
        """Generate JSONL records and write to file or stdout."""
        cmd = [
            self._cpp_executable,
            '--num-records', str(num_records),
            '--num-fields', str(self.num_fields),
            '--sparsity', str(self.sparsity),
            '--max-depth', str(self.max_depth),
            '--nest-probability', str(self.nest_probability),
            '--field-types', ','.join(self.field_types),
            '--high-cardinality-fields', str(self.high_cardinality_fields),
            '--low-cardinality-fields', str(self.low_cardinality_fields),
        ]
        if self.seed is not None:
            cmd.extend(['--seed', str(self.seed)])
        
        if output_file:
            with open(output_file, 'w') as f:
                subprocess.run(cmd, stdout=f, check=True)
        else:
            subprocess.run(cmd, stdout=sys.stdout, check=True)
    
    def generate_sample_records(self, num_samples: int = 100) -> List[Dict[str, Any]]:
        """Generate sample records for query generation analysis."""
        if num_samples <= 0:
            return []

        cmd = [
            self._cpp_executable,
            '--num-records', str(num_samples),
            '--num-fields', str(self.num_fields),
            '--sparsity', str(self.sparsity),
            '--max-depth', str(self.max_depth),
            '--nest-probability', str(self.nest_probability),
            '--field-types', ','.join(self.field_types),
            '--high-cardinality-fields', str(self.high_cardinality_fields),
            '--low-cardinality-fields', str(self.low_cardinality_fields),
        ]
        if self.seed is not None:
            cmd.extend(['--seed', str(self.seed)])

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        samples: List[Dict[str, Any]] = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            # The C++ generator outputs JSONL (one JSON object per line)
            samples.append(json.loads(line))

        return samples
    
    @property
    def field_schemas(self) -> List[FieldSchema]:
        """Get field schemas. Reconstructs them for compatibility with QueryGenerator."""
        if not hasattr(self, '_cached_field_schemas'):
            schemas = []
            field_cardinalities = []
            for i in range(self.num_fields):
                if i < self.high_cardinality_fields:
                    field_cardinalities.append('high')
                elif i < self.high_cardinality_fields + self.low_cardinality_fields:
                    field_cardinalities.append('low')
                else:
                    field_cardinalities.append('medium')
            
            # Generate consistent field types using deterministic approach based on seed
            if self.seed is not None:
                random.seed(self.seed)
            
            for i in range(self.num_fields):
                field_name = f"field_{i+1}"
                field_type = random.choice(self.field_types)
                cardinality = field_cardinalities[i]
                schemas.append(FieldSchema(field_name, field_type, cardinality))
            
            self._cached_field_schemas = schemas
        return self._cached_field_schemas


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
    generator.generate(args.num_records, args.output)
    
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
