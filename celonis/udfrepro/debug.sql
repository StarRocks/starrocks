CREATE DATABASE duplicate_checker;
USE duplicate_checker;
CREATE FUNCTION Identity(string)
RETURNS string
properties (
    "symbol" = "udffailures.Identity",
    "type" = "StarrocksJar",
    "file" = "http://127.0.0.1:7000/duplicate-invoice-checker-udf-1.0-SNAPSHOT.jar"
);
SELECT Identity("foo");
