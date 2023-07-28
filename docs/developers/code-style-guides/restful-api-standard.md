# StarRocks Restful API Standard

## API Format

1. The API format follows the pattern: /api/{version}/{target-object-access-path}/{action}. 
2. {version} is denoted as v{number}, such as v1, v2, v3, v4, etc. 
3. {target-object-access-path} is organized in a hierarchical manner, which will be explained in detail later. 
4. {action} is optional, API implementors should utilize the HTTP METHOD to convey the operation's meaning as much as possible. Only when the HTTP methods' semantics cannot be fulfilled should the action be used. For example, if there is no HTTP method available to rename an object.

## Definition of Target Object Access Path

1. Target objects accessed by the REST API need to be categorized and organized into a hierarchical access path. The access path format is as follows:
```
/primary_categories/primary_object/secondary_categories/secondary_object/.../categories/object
```

Taking catalog, database, table, column as examples:
```
/catalogs: Represents all catalogs.
/catalogs/hive: Represents the specific catalog object named "hive" under the catalog category.
/catalogs/hive/databases: Represents all databases in the "hive" catalog.
/catalogs/hive/databases/tpch_100g: Represents the database named "tpch_100g" in the "hive" catalog.
/catalogs/hive/databases/tpch_100g/tables: Represents all tables in the "tpch_100g" database.
/catalogs/hive/databases/tpch_100g/tables/lineitem: Represents the tpch_100g.lineitem table.
/catalogs/hive/databases/tpch_100g/tables/lineitem/columns: Represents all columns in the tpch_100g.lineitem table.
/catalogs/hive/databases/tpch_100g/tables/lineitem/columns/l_orderkey: Represents the specific column l_orderkey in the tpch_100g.lineitem table.
```

2. Categories are named using snake-case, and the last word is in plural form.all the words are in lowercase, and multiple words are connected by underscores (_). Specific objects are named using their actual names. The hierarchical relationship of the target objects needs to be clearly defined.

## Selection of HTTP Method

1. GET: Use the GET method to show a single object and list all objects of a certain category. The GET method's access to objects is read-only and does not provide a request body.
```
# list all of the tables in database ssb_100g
GET /api/v2/catalogs/default/databases/ssb_100g/tables

# show the table ssb_100g.lineorder
GET /api/v2/catalogs/default/databases/ssb_100g/tables/lineorder
```

2. POST: Used to create objects. Parameters are passed through the request body. It is not idempotent. If the object already exists, the repeated creation will fail and return an error message.
```
POST /api/v2/catalogs/default/databases/ssb_100g/tables/create -d@create_customer.sql
```

3. PUT: Used to create objects. Parameters are passed through the request body. It is idempotent. If the object already exists, it will return success. PUT method is the CREATE IF NOT EXISTS version of the POST method.
```
PUT /api/v2/databases/ssb_100g/tables/create -d@create_customer.sql
```

4. DELETE: Used to delete objects. It does not provide a request body. If the object to be deleted does not exist, it will return success. DELETE method has the DROP IF EXISTS semantics.
```
DELETE /api/v2/catalogs/default/databases/ssb_100g/tables/customer
```

5. PATCH: Used to update objects. It provides a request body, which only contains the partial information that needs to be modified.
```
PATCH /api/v2/databases/ssb_100g/tables/customer -d '{"unique_key_constraints": ["c_custkey"]}'
```

## Authentication and Authorization

1. Authentication and authorization information is passed in the HTTP Request Header.

## HTTP Status Codes

1. HTTP status codes are returned by the REST API to indicate the success or failure of an operation. 
2. The status codes (2xx) for success operation as follows:

- 200 OK: Indicates that the request has been successfully completed. It is used for viewing/listing/deleting/updating objects and querying the status of pending tasks.
- 201 Created: Indicates that the object has been successfully created. It is used for PUT/POST methods. The response body must include the object URI for subsequent viewing/listing/deleting/updating.
- 202 Accepted: Indicates that the task submission is successful and the task is in a pending state. The response body must include the task URI for subsequent cancellation, deletion, and polling of task status.

3. The error codes (4xx) indicate client errors. Users need to adjust and modify the HTTP request and retry.
- 400 Bad Request: Invalid request parameters.
- 401 Unauthorized: Missing authentication information, illegal authentication information, authentication failure.
- 403 Forbidden: Authentication succeeded, but the user's operation failed the authorization check. No access permission.
- 404 Not Found: API URI encoding error. It does not belong to the registered REST API.
- 405 Method Not Allowed: Incorrect HTTP Method used.
- 406 Not Acceptable: The response format does not match the media type specified in the Accept header.
- 415 Not Acceptable: The media type of the request content does not match the media type specified in the Content-Type header.

4. The error codes (5xx) indicate server errors. Users do not need to modify the request and can retry later.
- 500 Internal Server Error: Internal server error, similar to Unknown error.
- 503 Service Unavailable: The service is temporarily unavailable. For example, the user's access frequency is too high and has reached the rate limit; or the service is currently unable to provide service due to internal status, such as when creating a table with 3 replicas, but only 2 BEs are available; all Tablet replicas involved in a user's query are unavailable.

## HTTP Response Format

1. When the API returns an HTTP status code of 200/201/202, the HTTP response is not empty. The API returns results in JSON format, including top-level fields "status", "message", and "result". All JSON fields are named using camel-case.

2. In a successful API response, the "status" is "0", the "message" is "OK", and the "result" contains the actual results.
```json
{
   "status":"0",
   "message": "OK",
   "result": {....}
}
```

3. In a failed API response, the "status" is not "0", the "message" is a simple error message, and the "result" can contain detailed error information, such as error stack traces.
```json
{
   "status":"1",
   "message": "Analyze error",
   "result": {....}
}
```

## Parameter Passing

1. API parameters are passed in the precedence order of path, request body, query parameters, and header. Choose the appropriate method for parameter passing.

2. Path parameters: Required parameters that represent the object's hierarchical relationship are placed in the path parameters.
```
 /api/v2/warehouses/{warehouseName}/backends/{backendId}
 /api/v2/warehouses/ware0/backends/10027
```

3. Request body: Parameters are passed using application/json. Parameters can be of required or optional types.

4. Query parameters: Using query parameters and request body parameters at the same time is not allowed. For the same API, choose either one. If the number of parameters excluding header parameters and path parameters is not more than 2, query parameters can be used; otherwise, use the request body to pass parameters.

5. HEADER parameters: Headers should be used to pass HTTP standard parameters such as Content-type and Accept are placed in the header, implementors should not abuse http headers to pass customized parameters. When using headers to pass parameters for user extensions, the header name should be in the format "x-starrocks-{name}", where the name can contain multiple English words, and each word is in lowercase and concatenated by hyphens (-).
