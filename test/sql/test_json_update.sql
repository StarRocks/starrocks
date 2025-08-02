-- Test cases for json_update function
-- This file demonstrates the usage of the newly implemented json_update function

-- Test 1: Basic object update
SELECT json_update(PARSE_JSON('{"a": 1, "b": 2}'), 'a', PARSE_JSON('42')) as test1;
-- Expected: {"a": 42, "b": 2}

-- Test 2: Nested object update
SELECT json_update(PARSE_JSON('{"a": {"b": 1, "c": 2}, "d": 3}'), 'a.b', PARSE_JSON('99')) as test2;
-- Expected: {"a": {"b": 99, "c": 2}, "d": 3}

-- Test 3: Array element update
SELECT json_update(PARSE_JSON('{"arr": [1, 2, 3]}'), 'arr[1]', PARSE_JSON('99')) as test3;
-- Expected: {"arr": [1, 99, 3]}

-- Test 4: Add new key
SELECT json_update(PARSE_JSON('{"a": 1}'), 'b', PARSE_JSON('"new_value"')) as test4;
-- Expected: {"a": 1, "b": "new_value"}

-- Test 5: JSON path with root notation
SELECT json_update(PARSE_JSON('{"a": 1, "b": 2}'), '$.a', PARSE_JSON('100')) as test5;
-- Expected: {"a": 100, "b": 2}

-- Test 6: Update with string value
SELECT json_update(PARSE_JSON('{"name": "John", "age": 30}'), 'name', PARSE_JSON('"Jane"')) as test6;
-- Expected: {"name": "Jane", "age": 30}

-- Test 7: Update with boolean value
SELECT json_update(PARSE_JSON('{"active": false, "count": 5}'), 'active', PARSE_JSON('true')) as test7;
-- Expected: {"active": true, "count": 5}

-- Test 8: Update with null value
SELECT json_update(PARSE_JSON('{"data": "value"}'), 'data', PARSE_JSON('null')) as test8;
-- Expected: {"data": null}

-- Test 9: Complex nested update
SELECT json_update(
    PARSE_JSON('{"user": {"profile": {"name": "John", "settings": {"theme": "dark"}}}}'), 
    'user.profile.settings.theme', 
    PARSE_JSON('"light"')
) as test9;
-- Expected: {"user": {"profile": {"name": "John", "settings": {"theme": "light"}}}}

-- Test 10: NULL handling
SELECT json_update(NULL, 'a', PARSE_JSON('1')) as test10;
-- Expected: NULL