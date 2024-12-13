# encoding: utf-8

# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow
import sys
import time
import json
import base64
import zipimport
import ast

class CallStub(object):
    def __init__(self, symbol, output_type, location, content):
        self.symbol = symbol
        self.output_type = output_type
        self.location = location
        self.content = content
        self.exec_env = {}
        self.eval_func = None
        # extract function object
        if location == "inline":
            try:
                exec(content, self.exec_env)
            except Exception as e:
                raise ValueError(f"Failed to evaluate UDF: {content} with error: {e}")
            if self.symbol not in self.exec_env:
                raise ValueError(f"Function {self.symbol} not found in UDF: {content}")
            self.eval_func = self.exec_env[self.symbol]
        else:
            try:
                module_with_symbol = self.symbol.split(".")
                module = self.load_module(location, module_with_symbol[0])
                self.eval_func = getattr(module, module_with_symbol[1])
            except Exception as e:
                raise ValueError(f"Failed to load UDF module: {location} symbol {symbol} with error: {e}")

    def cvt(self, py_list):
        return pa.array(py_list, self.output_type)

    def get_imported_packages_ast(self, importer, module_name):
        # acquire source
        source = importer.get_source(module_name)
        imported_packages = []
        if source is None:
            return imported_packages
        # parse source
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_packages.append(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                imported_packages.append(node.module.split('.')[0])
        return imported_packages

    def load_module(self, location, module_name):
        importer = zipimport.zipimporter(location)
        dependencies = self.get_imported_packages_ast(importer, module_name)
        for dep in dependencies:
            if importer.find_module(dep):
                importer.load_module(dep)
        module = importer.load_module(module_name)
        return module

class ScalarCallStub(CallStub):
    """
    Python Scalar Call stub
    """
    def __init__(self, symbol, output_type, location, content):
        CallStub.__init__(self, symbol, output_type, location, content)

    def evaluate(self, batch: pa.RecordBatch) -> pa.Array:
        num_rows = batch.num_rows
        num_cols = len(batch.columns)
        result_list = []
        for i in range(num_rows):
            params = [batch.columns[j][i].as_py() for j in range(num_cols)]
            res = self.eval_func(*params)
            result_list.append(res)
        # set result to output column
        return self.cvt(result_list)

class VectorizeArrowCallStub(CallStub):
    """
    Python Vectorized Call stub
    """
    def __init__(self, symbol, output_type, location, content):
        CallStub.__init__(self, symbol, output_type, location, content)

    def evaluate(self, batch: pa.RecordBatch) -> pa.Array:
        num_rows = batch.num_rows
        num_cols = len(batch.columns)
        result_list = []
        params = [batch.columns[j] for j in range(num_cols)]
        res = self.eval_func(*params)
        return res

def get_call_stub(desc):
    """
    Get call stub
    """
    symbol = desc["symbol"]
    location = desc["location"]
    content = desc["content"]
    input_type = desc["input_type"]
    return_type_base64 = desc["return_type"]
    binary_data = base64.b64decode(return_type_base64)
    return_type = pa.ipc.read_schema(pa.BufferReader(binary_data)).field(0).type
    if input_type == "scalar":
        return ScalarCallStub(symbol, return_type, location, content)
    elif input_type == "arrow":
        return VectorizeArrowCallStub(symbol, return_type, location, content)

class UDFFlightServer(flight.FlightServerBase):
    def do_exchange(self, context, descriptor, reader, writer):
        func_desc = json.loads(descriptor.command)
        stub = get_call_stub(func_desc)
        started = False
        for chunk in reader:
            if chunk.data:
                result_column = stub.evaluate(chunk.data)
                result_batch = pa.RecordBatch.from_arrays([result_column], ["result"])
                if not started:
                    writer.begin(result_batch.schema)
                    started = True
                writer.write_batch(result_batch)

def main(unix_socket_path):
    location = unix_socket_path
    server = UDFFlightServer(location)
    print("Pywork start success")
    sys.stdout.flush()
    server.wait()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an Arrow Flight echo server over Unix socket.")
    parser.add_argument("unix_socket_path", type=str, help="The path to the Unix socket.")
    args = parser.parse_args()
    main(args.unix_socket_path)
