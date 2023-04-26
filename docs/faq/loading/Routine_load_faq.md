# Routine Load

## Does mysql binlog data produced by kafka count as text format data?

When imported into kafka via canal, it is in json format.

## Can 'semantic consistency' be guaranteed by consuming data from kafka, writing it to StarRocks and directly connecting to StarRocks?

Yes it can!

## recompile librdkafka with libsasl2 or openssl support

**Issue description:**

Failure to perform Routine Load to Kafka cluster. Errors are reported as:

```plain text
ErrorReason{errCode = 4, msg='Job failed to fetch all current partition with error [Failed to send proxy request: failed to send proxy request: [PAUSE: failed to create kafka consumer: No provider for SASL mechanism GSSAPI: recompile librdkafka with libsasl2 or openssl support. Current build options: PLAIN SASL_SCRAM]]'}
```

**Cause:**

The current librdkafka does not support sasl authentication.

**Solution:**

Compile librdkafka again. Please refer to 'compile librdkafka'.
