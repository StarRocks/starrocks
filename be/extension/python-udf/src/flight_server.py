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
import base64
import json
import os
import sys
import time
import zipimport
import ast
import hashlib
import shutil
import tempfile
import threading
import urllib.request

import pyarrow as pa
import pyarrow.flight as flight

# Cache of already-downloaded UDF zips, keyed by their source URL -> local file path.
# In external-worker mode the BE hands us the original download URL (e.g. an http(s) URL)
# instead of a BE-local path, so we fetch the zip ourselves and zipimport it from local disk.
_DOWNLOAD_CACHE = {}
_DOWNLOAD_LOCK = threading.Lock()
# Socket timeout (seconds) for downloading a UDF package, so a slow/hung `file` URL cannot hang the
# worker (and thus the BE call) forever. Override with the SR_PY_UDF_DOWNLOAD_TIMEOUT env var.
_DOWNLOAD_TIMEOUT_SECONDS = float(os.environ.get("SR_PY_UDF_DOWNLOAD_TIMEOUT", "60"))


def _file_md5(path):
    digest = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_zip_location(location, checksum=""):
    if not (location.startswith("http://") or location.startswith("https://")):
        # spawn mode: already a local path
        return location
    with _DOWNLOAD_LOCK:
        cached = _DOWNLOAD_CACHE.get(location)
        if cached and os.path.exists(cached):
            return cached
        key = hashlib.md5(location.encode("utf-8")).hexdigest()
        local_path = os.path.join(tempfile.gettempdir(), "sr_udf_" + key + ".zip")
        need_download = True
        if os.path.exists(local_path) and checksum and _file_md5(local_path).lower() == checksum.lower():
            need_download = False
        if need_download:
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip", prefix="sr_udf_dl_")
            os.close(tmp_fd)
            try:
                with urllib.request.urlopen(location, timeout=_DOWNLOAD_TIMEOUT_SECONDS) as resp, \
                        open(tmp_path, "wb") as out:
                    shutil.copyfileobj(resp, out)
                # Verify integrity against the BE-provided md5 (matches FE computeMd5 / the
                # BE UserFunctionCache check). Empty checksum means "not provided" -> skip.
                if checksum:
                    actual = _file_md5(tmp_path)
                    if actual.lower() != checksum.lower():
                        raise ValueError(
                            f"UDF zip checksum mismatch for {location}: "
                            f"expected {checksum}, got {actual}")
                os.replace(tmp_path, local_path)
            except Exception:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                raise
        _DOWNLOAD_CACHE[location] = local_path
        return local_path


class CallStub(object):
    def __init__(self, symbol, output_type, location, content, checksum=""):
        self.symbol = symbol
        self.output_type = output_type
        self.location = location
        self.content = content
        self.checksum = checksum
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
        location = _resolve_zip_location(location, self.checksum)
        importer = zipimport.zipimporter(location)
        dependencies = self.get_imported_packages_ast(importer, module_name)
        for dep in dependencies:
            if importer.find_module(dep):
                importer.load_module(dep)
        module = importer.load_module(module_name)
        return module

def _normalize_scalar(value, arrow_type):
    # Recursively coerce the result of pa.Scalar.as_py() into idiomatic Python
    # values for nested types. The default pyarrow behavior returns a list of
    # (key, value) tuples for MapArray, which is awkward for UDF authors and
    # breaks when maps appear inside arrays, structs, or other maps.
    if value is None:
        return None
    if pa.types.is_map(arrow_type):
        key_type = arrow_type.key_type
        item_type = arrow_type.item_type
        return {_normalize_scalar(k, key_type): _normalize_scalar(v, item_type)
                for k, v in value}
    if (pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)):
        elem_type = arrow_type.value_type
        return [_normalize_scalar(item, elem_type) for item in value]
    if pa.types.is_struct(arrow_type):
        return {arrow_type.field(i).name:
                _normalize_scalar(value[arrow_type.field(i).name],
                                  arrow_type.field(i).type)
                for i in range(arrow_type.num_fields)}
    return value


class ScalarCallStub(CallStub):
    """
    Python Scalar Call stub
    """
    def __init__(self, symbol, output_type, location, content, checksum=""):
        CallStub.__init__(self, symbol, output_type, location, content, checksum)

    def evaluate(self, batch: pa.RecordBatch) -> pa.Array:
        num_rows = batch.num_rows
        num_cols = len(batch.columns)
        col_types = [batch.columns[j].type for j in range(num_cols)]
        result_list = []
        for i in range(num_rows):
            params = [_normalize_scalar(batch.columns[j][i].as_py(), col_types[j])
                      for j in range(num_cols)]
            res = self.eval_func(*params)
            result_list.append(res)
        # set result to output column
        return self.cvt(result_list)

class VectorizeArrowCallStub(CallStub):
    """
    Python Vectorized Call stub
    """
    def __init__(self, symbol, output_type, location, content, checksum=""):
        CallStub.__init__(self, symbol, output_type, location, content, checksum)

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
    checksum = desc.get("checksum", "")
    return_type_base64 = desc["return_type"]
    binary_data = base64.b64decode(return_type_base64)
    return_type = pa.ipc.read_schema(pa.BufferReader(binary_data)).field(0).type
    if input_type == "scalar":
        return ScalarCallStub(symbol, return_type, location, content, checksum)
    elif input_type == "arrow":
        return VectorizeArrowCallStub(symbol, return_type, location, content, checksum)

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

def build_socket_url(prefix):
    pid = os.getpid()
    return prefix + str(pid)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an Arrow Flight echo server over Unix socket.")
    parser.add_argument("unix_socket_path", type=str, help="The path to the Unix socket.")
    args = parser.parse_args()
    url = build_socket_url(args.unix_socket_path)
    main(url)
