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

namespace java com.starrocks.thrift
namespace cpp starrocks

enum TBrokerOperationStatusCode {
    OK = 0;
    END_OF_FILE = 301;
    
    // user input error
    NOT_AUTHORIZED = 401;
    DUPLICATE_REQUEST = 402;
    INVALID_INPUT_OFFSET = 403; // user input offset is invalid, is large than file length
    INVALID_INPUT_FILE_PATH = 404;
    INVALID_ARGUMENT = 405;
    
    // internal server error
    FILE_NOT_FOUND = 501;
    TARGET_STORAGE_SERVICE_ERROR = 502; // the target storage service error
    OPERATION_NOT_SUPPORTED = 503; // the api is not implemented
}

struct TBrokerOperationStatus {
    1: required TBrokerOperationStatusCode statusCode;
    2: optional string message;
}

enum TBrokerVersion {
    VERSION_ONE = 1;
}

enum TBrokerOpenMode {
    APPEND = 1;
}

struct TBrokerFileStatus {
    // file path
    1: required string path;
    // if this is a directory
    2: required bool isDir;
    // file size
    3: required i64 size;
    // If the value is false, then the file cannot be split and the whole file must be imported
    // as a complete map task, if it is a compressed file the return value is also false
    4: required bool isSplitable;
}

struct TBrokerFD {
    1: required i64 high;
    2: required i64 low;
}

struct TBrokerListResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional list<TBrokerFileStatus> files;
    
}

struct TBrokerOpenReaderResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional TBrokerFD fd;
    3: optional i64 size; // file size(Deprecated)
}

struct TBrokerReadResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional binary data; 
}

struct TBrokerOpenWriterResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional TBrokerFD fd;
}

struct TBrokerCheckPathExistResponse {
    1: required TBrokerOperationStatus opStatus;
    2: required bool isPathExist;
}

struct TBrokerListPathRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required bool isRecursive;
    4: required map<string,string> properties;
    5: optional bool fileNameOnly;
}

struct TBrokerDeletePathRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required map<string,string> properties;
}

struct TBrokerRenamePathRequest {
    1: required TBrokerVersion version;
    2: required string srcPath;
    3: required string destPath;
    4: required map<string,string> properties;
}

struct TBrokerCheckPathExistRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required map<string,string> properties;
}

struct TBrokerOpenReaderRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required i64 startOffset;
    4: required string clientId;
    5: required map<string,string> properties;
}

struct TBrokerPReadRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
    3: required i64 offset;
    4: required i64 length;
}

struct TBrokerSeekRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
    3: required i64 offset;
}

struct TBrokerCloseReaderRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
}

struct TBrokerOpenWriterRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required TBrokerOpenMode openMode;
    4: required string clientId;
    5: required map<string,string> properties;
}

struct TBrokerPWriteRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
    3: required i64 offset;
    4: required binary data;
}

struct TBrokerCloseWriterRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
}

struct TBrokerPingBrokerRequest {
    1: required TBrokerVersion version;
    2: required string clientId;
}

service TFileBrokerService {
    
    // return all files in directory
    TBrokerListResponse listPath(1: TBrokerListPathRequest request);
    
    // delete one file.
    // if failed, return error in status code
    TBrokerOperationStatus deletePath(1: TBrokerDeletePathRequest request);

    // rename file
    TBrokerOperationStatus renamePath(1: TBrokerRenamePathRequest request);

    // check if the file exists
    TBrokerCheckPathExistResponse checkPathExist(1: TBrokerCheckPathExistRequest request);
    
    // open a file to read
    // input:
    //     path: file path
    //     startOffset: offset to read
    // return:
    //     fd: The reason is that there may be more than one client side reading the same file on
    //     a broker, so the fd needs to be used to identify them separately.
    TBrokerOpenReaderResponse openReader(1: TBrokerOpenReaderRequest request);
    
    // read file data
    // input:
    //     fd: fd returned by open reader
    //     length: length to read
    //     offset: The user must read with an offset each time, but this does not mean that
    //     the user can specify an offset to read, this offset is mainly used by the backend to
    //     verify whether to repeat the read. In network communication it is likely that the user
    //     initiates a read, but does not get the result, and then reads again, it is likely that
    //     the first read has already occurred, but the user did not receive the result, so the
    //     user initiates a second read, the returned data is wrong, but the user can not sense.
    // return:
    //   Under normal circumstances, the binary data is returned. In case of exceptions, such as
    //   the reader is closed, the file is read to the end of the file, etc., the status code is
    //   needed to return
    //   The binary here will be wrapped into a response object later.
    TBrokerReadResponse pread(1: TBrokerPReadRequest request);
    
    // seek to position
    TBrokerOperationStatus seek(1: TBrokerSeekRequest request);
    
    // close reader
    TBrokerOperationStatus closeReader(1: TBrokerCloseReaderRequest request);
    
    // Open a file write stream according to the path, this API is mainly designed for the future
    // backup restore, the current import does not need this API
    //    1. If the file does not exist then it is created and fd is returned.
    //    2. If the file exists, but is a directory, then return failure
    // There is no parameter for recursive folder creation, the default is to create the folder
    // if it does not exist.
    // This API is currently considered for backup purposes only and has nothing to do with import.
    // input:
    //     openMode: overwrite, create new, append
    TBrokerOpenWriterResponse openWriter(1: TBrokerOpenWriterRequest request);
    
    // write data to fd
    TBrokerOperationStatus pwrite(1: TBrokerPWriteRequest request);
    
    // close write file
    TBrokerOperationStatus closeWriter(1: TBrokerCloseWriterRequest request);
    
    // 
    TBrokerOperationStatus ping(1: TBrokerPingBrokerRequest request);
}
