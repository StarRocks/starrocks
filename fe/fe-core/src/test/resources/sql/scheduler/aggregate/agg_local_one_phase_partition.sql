[sql]
SELECT /*+SET_VAR(pipeline_dop=16,enable_tablet_internal_parallel=true)*/ l_orderkey, count(1) FROM lineitem_partition GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 16
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          17,2,5,8,11,14
        0:OlapScanNode
          1. partitionID=1044,tabletID=1058
          2. partitionID=1045,tabletID=1130
          3. partitionID=1046,tabletID=1094
          4. partitionID=1047,tabletID=1202
          5. partitionID=1048,tabletID=1166
          6. partitionID=1049,tabletID=1274
          7. partitionID=1050,tabletID=1238
          8. partitionID=1044,tabletID=1064
          9. partitionID=1045,tabletID=1136
          10. partitionID=1046,tabletID=1100
          11. partitionID=1047,tabletID=1208
          12. partitionID=1048,tabletID=1172
          13. partitionID=1049,tabletID=1280
          14. partitionID=1050,tabletID=1244
          15. partitionID=1044,tabletID=1070
          16. partitionID=1045,tabletID=1142
          17. partitionID=1046,tabletID=1106
          18. partitionID=1047,tabletID=1214
          19. partitionID=1048,tabletID=1178
          20. partitionID=1049,tabletID=1286
          21. partitionID=1050,tabletID=1250
          22. partitionID=1044,tabletID=1076
          23. partitionID=1045,tabletID=1148
          24. partitionID=1046,tabletID=1112
          25. partitionID=1047,tabletID=1220
          26. partitionID=1048,tabletID=1184
          27. partitionID=1049,tabletID=1292
          28. partitionID=1050,tabletID=1256
          29. partitionID=1044,tabletID=1082
          30. partitionID=1045,tabletID=1154
          31. partitionID=1046,tabletID=1118
          32. partitionID=1047,tabletID=1226
          33. partitionID=1048,tabletID=1190
          34. partitionID=1049,tabletID=1298
          35. partitionID=1050,tabletID=1262
          36. partitionID=1044,tabletID=1088
          37. partitionID=1045,tabletID=1160
          38. partitionID=1046,tabletID=1124
          39. partitionID=1047,tabletID=1232
          40. partitionID=1048,tabletID=1196
          41. partitionID=1049,tabletID=1304
          42. partitionID=1050,tabletID=1268
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0,3,6,9,12,15
        0:OlapScanNode
          1. partitionID=1044,tabletID=1054
          2. partitionID=1045,tabletID=1126
          3. partitionID=1046,tabletID=1090
          4. partitionID=1047,tabletID=1198
          5. partitionID=1048,tabletID=1162
          6. partitionID=1049,tabletID=1270
          7. partitionID=1050,tabletID=1234
          8. partitionID=1044,tabletID=1060
          9. partitionID=1045,tabletID=1132
          10. partitionID=1046,tabletID=1096
          11. partitionID=1047,tabletID=1204
          12. partitionID=1048,tabletID=1168
          13. partitionID=1049,tabletID=1276
          14. partitionID=1050,tabletID=1240
          15. partitionID=1044,tabletID=1066
          16. partitionID=1045,tabletID=1138
          17. partitionID=1046,tabletID=1102
          18. partitionID=1047,tabletID=1210
          19. partitionID=1048,tabletID=1174
          20. partitionID=1049,tabletID=1282
          21. partitionID=1050,tabletID=1246
          22. partitionID=1044,tabletID=1072
          23. partitionID=1045,tabletID=1144
          24. partitionID=1046,tabletID=1108
          25. partitionID=1047,tabletID=1216
          26. partitionID=1048,tabletID=1180
          27. partitionID=1049,tabletID=1288
          28. partitionID=1050,tabletID=1252
          29. partitionID=1044,tabletID=1078
          30. partitionID=1045,tabletID=1150
          31. partitionID=1046,tabletID=1114
          32. partitionID=1047,tabletID=1222
          33. partitionID=1048,tabletID=1186
          34. partitionID=1049,tabletID=1294
          35. partitionID=1050,tabletID=1258
          36. partitionID=1044,tabletID=1084
          37. partitionID=1045,tabletID=1156
          38. partitionID=1046,tabletID=1120
          39. partitionID=1047,tabletID=1228
          40. partitionID=1048,tabletID=1192
          41. partitionID=1049,tabletID=1300
          42. partitionID=1050,tabletID=1264
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          16,1,4,7,10,13
        0:OlapScanNode
          1. partitionID=1044,tabletID=1056
          2. partitionID=1045,tabletID=1128
          3. partitionID=1046,tabletID=1092
          4. partitionID=1047,tabletID=1200
          5. partitionID=1048,tabletID=1164
          6. partitionID=1049,tabletID=1272
          7. partitionID=1050,tabletID=1236
          8. partitionID=1044,tabletID=1062
          9. partitionID=1045,tabletID=1134
          10. partitionID=1046,tabletID=1098
          11. partitionID=1047,tabletID=1206
          12. partitionID=1048,tabletID=1170
          13. partitionID=1049,tabletID=1278
          14. partitionID=1050,tabletID=1242
          15. partitionID=1044,tabletID=1068
          16. partitionID=1045,tabletID=1140
          17. partitionID=1046,tabletID=1104
          18. partitionID=1047,tabletID=1212
          19. partitionID=1048,tabletID=1176
          20. partitionID=1049,tabletID=1284
          21. partitionID=1050,tabletID=1248
          22. partitionID=1044,tabletID=1074
          23. partitionID=1045,tabletID=1146
          24. partitionID=1046,tabletID=1110
          25. partitionID=1047,tabletID=1218
          26. partitionID=1048,tabletID=1182
          27. partitionID=1049,tabletID=1290
          28. partitionID=1050,tabletID=1254
          29. partitionID=1044,tabletID=1080
          30. partitionID=1045,tabletID=1152
          31. partitionID=1046,tabletID=1116
          32. partitionID=1047,tabletID=1224
          33. partitionID=1048,tabletID=1188
          34. partitionID=1049,tabletID=1296
          35. partitionID=1050,tabletID=1260
          36. partitionID=1044,tabletID=1086
          37. partitionID=1045,tabletID=1158
          38. partitionID=1046,tabletID=1122
          39. partitionID=1047,tabletID=1230
          40. partitionID=1048,tabletID=1194
          41. partitionID=1049,tabletID=1302
          42. partitionID=1050,tabletID=1266

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
  |
  0:OlapScanNode
     TABLE: lineitem_partition
     PREAGGREGATION: ON
     partitions=7/7
     rollup: lineitem_partition
     tabletRatio=126/126
     tabletList=1054,1056,1058,1060,1062,1064,1066,1068,1070,1072 ...
     cardinality=1
     avgRowSize=8.0

[end]

[sql]
SELECT /*+SET_VAR(pipeline_dop=3,enable_tablet_internal_parallel=true)*/ l_orderkey, count(1) FROM lineitem_partition GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 3
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10002

PLAN FRAGMENT 1(F00)
  DOP: 3
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 3
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          17:2,2:0,5:1,8:2,11:0,14:1
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1058
            2. partitionID=1045,tabletID=1130
            3. partitionID=1046,tabletID=1094
            4. partitionID=1047,tabletID=1202
            5. partitionID=1048,tabletID=1166
            6. partitionID=1049,tabletID=1274
            7. partitionID=1050,tabletID=1238
            8. partitionID=1044,tabletID=1076
            9. partitionID=1045,tabletID=1148
            10. partitionID=1046,tabletID=1112
            11. partitionID=1047,tabletID=1220
            12. partitionID=1048,tabletID=1184
            13. partitionID=1049,tabletID=1292
            14. partitionID=1050,tabletID=1256
          DriverSequence#1
            15. partitionID=1044,tabletID=1064
            16. partitionID=1045,tabletID=1136
            17. partitionID=1046,tabletID=1100
            18. partitionID=1047,tabletID=1208
            19. partitionID=1048,tabletID=1172
            20. partitionID=1049,tabletID=1280
            21. partitionID=1050,tabletID=1244
            22. partitionID=1044,tabletID=1082
            23. partitionID=1045,tabletID=1154
            24. partitionID=1046,tabletID=1118
            25. partitionID=1047,tabletID=1226
            26. partitionID=1048,tabletID=1190
            27. partitionID=1049,tabletID=1298
            28. partitionID=1050,tabletID=1262
          DriverSequence#2
            29. partitionID=1044,tabletID=1070
            30. partitionID=1045,tabletID=1142
            31. partitionID=1046,tabletID=1106
            32. partitionID=1047,tabletID=1214
            33. partitionID=1048,tabletID=1178
            34. partitionID=1049,tabletID=1286
            35. partitionID=1050,tabletID=1250
            36. partitionID=1044,tabletID=1088
            37. partitionID=1045,tabletID=1160
            38. partitionID=1046,tabletID=1124
            39. partitionID=1047,tabletID=1232
            40. partitionID=1048,tabletID=1196
            41. partitionID=1049,tabletID=1304
            42. partitionID=1050,tabletID=1268
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 3
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          0:0,3:1,6:2,9:0,12:1,15:2
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1054
            2. partitionID=1045,tabletID=1126
            3. partitionID=1046,tabletID=1090
            4. partitionID=1047,tabletID=1198
            5. partitionID=1048,tabletID=1162
            6. partitionID=1049,tabletID=1270
            7. partitionID=1050,tabletID=1234
            8. partitionID=1044,tabletID=1072
            9. partitionID=1045,tabletID=1144
            10. partitionID=1046,tabletID=1108
            11. partitionID=1047,tabletID=1216
            12. partitionID=1048,tabletID=1180
            13. partitionID=1049,tabletID=1288
            14. partitionID=1050,tabletID=1252
          DriverSequence#1
            15. partitionID=1044,tabletID=1060
            16. partitionID=1045,tabletID=1132
            17. partitionID=1046,tabletID=1096
            18. partitionID=1047,tabletID=1204
            19. partitionID=1048,tabletID=1168
            20. partitionID=1049,tabletID=1276
            21. partitionID=1050,tabletID=1240
            22. partitionID=1044,tabletID=1078
            23. partitionID=1045,tabletID=1150
            24. partitionID=1046,tabletID=1114
            25. partitionID=1047,tabletID=1222
            26. partitionID=1048,tabletID=1186
            27. partitionID=1049,tabletID=1294
            28. partitionID=1050,tabletID=1258
          DriverSequence#2
            29. partitionID=1044,tabletID=1066
            30. partitionID=1045,tabletID=1138
            31. partitionID=1046,tabletID=1102
            32. partitionID=1047,tabletID=1210
            33. partitionID=1048,tabletID=1174
            34. partitionID=1049,tabletID=1282
            35. partitionID=1050,tabletID=1246
            36. partitionID=1044,tabletID=1084
            37. partitionID=1045,tabletID=1156
            38. partitionID=1046,tabletID=1120
            39. partitionID=1047,tabletID=1228
            40. partitionID=1048,tabletID=1192
            41. partitionID=1049,tabletID=1300
            42. partitionID=1050,tabletID=1264
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 3
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          16:2,1:0,4:1,7:2,10:0,13:1
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1056
            2. partitionID=1045,tabletID=1128
            3. partitionID=1046,tabletID=1092
            4. partitionID=1047,tabletID=1200
            5. partitionID=1048,tabletID=1164
            6. partitionID=1049,tabletID=1272
            7. partitionID=1050,tabletID=1236
            8. partitionID=1044,tabletID=1074
            9. partitionID=1045,tabletID=1146
            10. partitionID=1046,tabletID=1110
            11. partitionID=1047,tabletID=1218
            12. partitionID=1048,tabletID=1182
            13. partitionID=1049,tabletID=1290
            14. partitionID=1050,tabletID=1254
          DriverSequence#1
            15. partitionID=1044,tabletID=1062
            16. partitionID=1045,tabletID=1134
            17. partitionID=1046,tabletID=1098
            18. partitionID=1047,tabletID=1206
            19. partitionID=1048,tabletID=1170
            20. partitionID=1049,tabletID=1278
            21. partitionID=1050,tabletID=1242
            22. partitionID=1044,tabletID=1080
            23. partitionID=1045,tabletID=1152
            24. partitionID=1046,tabletID=1116
            25. partitionID=1047,tabletID=1224
            26. partitionID=1048,tabletID=1188
            27. partitionID=1049,tabletID=1296
            28. partitionID=1050,tabletID=1260
          DriverSequence#2
            29. partitionID=1044,tabletID=1068
            30. partitionID=1045,tabletID=1140
            31. partitionID=1046,tabletID=1104
            32. partitionID=1047,tabletID=1212
            33. partitionID=1048,tabletID=1176
            34. partitionID=1049,tabletID=1284
            35. partitionID=1050,tabletID=1248
            36. partitionID=1044,tabletID=1086
            37. partitionID=1045,tabletID=1158
            38. partitionID=1046,tabletID=1122
            39. partitionID=1047,tabletID=1230
            40. partitionID=1048,tabletID=1194
            41. partitionID=1049,tabletID=1302
            42. partitionID=1050,tabletID=1266

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
  |
  0:OlapScanNode
     TABLE: lineitem_partition
     PREAGGREGATION: ON
     partitions=7/7
     rollup: lineitem_partition
     tabletRatio=126/126
     tabletList=1054,1056,1058,1060,1062,1064,1066,1068,1070,1072 ...
     cardinality=1
     avgRowSize=8.0

[end]

[sql]
SELECT /*+SET_VAR(pipeline_dop=2, enable_tablet_internal_parallel=true)*/ l_orderkey, count(1) FROM lineitem_partition GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 2
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10003

PLAN FRAGMENT 1(F00)
  DOP: 2
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 2
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          17:1,2:0,5:1,8:0,11:1,14:0
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1058
            2. partitionID=1045,tabletID=1130
            3. partitionID=1046,tabletID=1094
            4. partitionID=1047,tabletID=1202
            5. partitionID=1048,tabletID=1166
            6. partitionID=1049,tabletID=1274
            7. partitionID=1050,tabletID=1238
            8. partitionID=1044,tabletID=1070
            9. partitionID=1045,tabletID=1142
            10. partitionID=1046,tabletID=1106
            11. partitionID=1047,tabletID=1214
            12. partitionID=1048,tabletID=1178
            13. partitionID=1049,tabletID=1286
            14. partitionID=1050,tabletID=1250
            15. partitionID=1044,tabletID=1082
            16. partitionID=1045,tabletID=1154
            17. partitionID=1046,tabletID=1118
            18. partitionID=1047,tabletID=1226
            19. partitionID=1048,tabletID=1190
            20. partitionID=1049,tabletID=1298
            21. partitionID=1050,tabletID=1262
          DriverSequence#1
            22. partitionID=1044,tabletID=1064
            23. partitionID=1045,tabletID=1136
            24. partitionID=1046,tabletID=1100
            25. partitionID=1047,tabletID=1208
            26. partitionID=1048,tabletID=1172
            27. partitionID=1049,tabletID=1280
            28. partitionID=1050,tabletID=1244
            29. partitionID=1044,tabletID=1076
            30. partitionID=1045,tabletID=1148
            31. partitionID=1046,tabletID=1112
            32. partitionID=1047,tabletID=1220
            33. partitionID=1048,tabletID=1184
            34. partitionID=1049,tabletID=1292
            35. partitionID=1050,tabletID=1256
            36. partitionID=1044,tabletID=1088
            37. partitionID=1045,tabletID=1160
            38. partitionID=1046,tabletID=1124
            39. partitionID=1047,tabletID=1232
            40. partitionID=1048,tabletID=1196
            41. partitionID=1049,tabletID=1304
            42. partitionID=1050,tabletID=1268
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 2
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          0:0,3:1,6:0,9:1,12:0,15:1
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1054
            2. partitionID=1045,tabletID=1126
            3. partitionID=1046,tabletID=1090
            4. partitionID=1047,tabletID=1198
            5. partitionID=1048,tabletID=1162
            6. partitionID=1049,tabletID=1270
            7. partitionID=1050,tabletID=1234
            8. partitionID=1044,tabletID=1066
            9. partitionID=1045,tabletID=1138
            10. partitionID=1046,tabletID=1102
            11. partitionID=1047,tabletID=1210
            12. partitionID=1048,tabletID=1174
            13. partitionID=1049,tabletID=1282
            14. partitionID=1050,tabletID=1246
            15. partitionID=1044,tabletID=1078
            16. partitionID=1045,tabletID=1150
            17. partitionID=1046,tabletID=1114
            18. partitionID=1047,tabletID=1222
            19. partitionID=1048,tabletID=1186
            20. partitionID=1049,tabletID=1294
            21. partitionID=1050,tabletID=1258
          DriverSequence#1
            22. partitionID=1044,tabletID=1060
            23. partitionID=1045,tabletID=1132
            24. partitionID=1046,tabletID=1096
            25. partitionID=1047,tabletID=1204
            26. partitionID=1048,tabletID=1168
            27. partitionID=1049,tabletID=1276
            28. partitionID=1050,tabletID=1240
            29. partitionID=1044,tabletID=1072
            30. partitionID=1045,tabletID=1144
            31. partitionID=1046,tabletID=1108
            32. partitionID=1047,tabletID=1216
            33. partitionID=1048,tabletID=1180
            34. partitionID=1049,tabletID=1288
            35. partitionID=1050,tabletID=1252
            36. partitionID=1044,tabletID=1084
            37. partitionID=1045,tabletID=1156
            38. partitionID=1046,tabletID=1120
            39. partitionID=1047,tabletID=1228
            40. partitionID=1048,tabletID=1192
            41. partitionID=1049,tabletID=1300
            42. partitionID=1050,tabletID=1264
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 2
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          16:1,1:0,4:1,7:0,10:1,13:0
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1056
            2. partitionID=1045,tabletID=1128
            3. partitionID=1046,tabletID=1092
            4. partitionID=1047,tabletID=1200
            5. partitionID=1048,tabletID=1164
            6. partitionID=1049,tabletID=1272
            7. partitionID=1050,tabletID=1236
            8. partitionID=1044,tabletID=1068
            9. partitionID=1045,tabletID=1140
            10. partitionID=1046,tabletID=1104
            11. partitionID=1047,tabletID=1212
            12. partitionID=1048,tabletID=1176
            13. partitionID=1049,tabletID=1284
            14. partitionID=1050,tabletID=1248
            15. partitionID=1044,tabletID=1080
            16. partitionID=1045,tabletID=1152
            17. partitionID=1046,tabletID=1116
            18. partitionID=1047,tabletID=1224
            19. partitionID=1048,tabletID=1188
            20. partitionID=1049,tabletID=1296
            21. partitionID=1050,tabletID=1260
          DriverSequence#1
            22. partitionID=1044,tabletID=1062
            23. partitionID=1045,tabletID=1134
            24. partitionID=1046,tabletID=1098
            25. partitionID=1047,tabletID=1206
            26. partitionID=1048,tabletID=1170
            27. partitionID=1049,tabletID=1278
            28. partitionID=1050,tabletID=1242
            29. partitionID=1044,tabletID=1074
            30. partitionID=1045,tabletID=1146
            31. partitionID=1046,tabletID=1110
            32. partitionID=1047,tabletID=1218
            33. partitionID=1048,tabletID=1182
            34. partitionID=1049,tabletID=1290
            35. partitionID=1050,tabletID=1254
            36. partitionID=1044,tabletID=1086
            37. partitionID=1045,tabletID=1158
            38. partitionID=1046,tabletID=1122
            39. partitionID=1047,tabletID=1230
            40. partitionID=1048,tabletID=1194
            41. partitionID=1049,tabletID=1302
            42. partitionID=1050,tabletID=1266

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
  |
  0:OlapScanNode
     TABLE: lineitem_partition
     PREAGGREGATION: ON
     partitions=7/7
     rollup: lineitem_partition
     tabletRatio=126/126
     tabletList=1054,1056,1058,1060,1062,1064,1066,1068,1070,1072 ...
     cardinality=1
     avgRowSize=8.0

[end]

[sql]
SELECT /*+SET_VAR(pipeline_dop=1, enable_tablet_internal_parallel=true)*/ l_orderkey, count(1) FROM lineitem_partition GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 1
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 1
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 1
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          17:0,2:0,5:0,8:0,11:0,14:0
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1058
            2. partitionID=1045,tabletID=1130
            3. partitionID=1046,tabletID=1094
            4. partitionID=1047,tabletID=1202
            5. partitionID=1048,tabletID=1166
            6. partitionID=1049,tabletID=1274
            7. partitionID=1050,tabletID=1238
            8. partitionID=1044,tabletID=1064
            9. partitionID=1045,tabletID=1136
            10. partitionID=1046,tabletID=1100
            11. partitionID=1047,tabletID=1208
            12. partitionID=1048,tabletID=1172
            13. partitionID=1049,tabletID=1280
            14. partitionID=1050,tabletID=1244
            15. partitionID=1044,tabletID=1070
            16. partitionID=1045,tabletID=1142
            17. partitionID=1046,tabletID=1106
            18. partitionID=1047,tabletID=1214
            19. partitionID=1048,tabletID=1178
            20. partitionID=1049,tabletID=1286
            21. partitionID=1050,tabletID=1250
            22. partitionID=1044,tabletID=1076
            23. partitionID=1045,tabletID=1148
            24. partitionID=1046,tabletID=1112
            25. partitionID=1047,tabletID=1220
            26. partitionID=1048,tabletID=1184
            27. partitionID=1049,tabletID=1292
            28. partitionID=1050,tabletID=1256
            29. partitionID=1044,tabletID=1082
            30. partitionID=1045,tabletID=1154
            31. partitionID=1046,tabletID=1118
            32. partitionID=1047,tabletID=1226
            33. partitionID=1048,tabletID=1190
            34. partitionID=1049,tabletID=1298
            35. partitionID=1050,tabletID=1262
            36. partitionID=1044,tabletID=1088
            37. partitionID=1045,tabletID=1160
            38. partitionID=1046,tabletID=1124
            39. partitionID=1047,tabletID=1232
            40. partitionID=1048,tabletID=1196
            41. partitionID=1049,tabletID=1304
            42. partitionID=1050,tabletID=1268
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 1
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          0:0,3:0,6:0,9:0,12:0,15:0
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1054
            2. partitionID=1045,tabletID=1126
            3. partitionID=1046,tabletID=1090
            4. partitionID=1047,tabletID=1198
            5. partitionID=1048,tabletID=1162
            6. partitionID=1049,tabletID=1270
            7. partitionID=1050,tabletID=1234
            8. partitionID=1044,tabletID=1060
            9. partitionID=1045,tabletID=1132
            10. partitionID=1046,tabletID=1096
            11. partitionID=1047,tabletID=1204
            12. partitionID=1048,tabletID=1168
            13. partitionID=1049,tabletID=1276
            14. partitionID=1050,tabletID=1240
            15. partitionID=1044,tabletID=1066
            16. partitionID=1045,tabletID=1138
            17. partitionID=1046,tabletID=1102
            18. partitionID=1047,tabletID=1210
            19. partitionID=1048,tabletID=1174
            20. partitionID=1049,tabletID=1282
            21. partitionID=1050,tabletID=1246
            22. partitionID=1044,tabletID=1072
            23. partitionID=1045,tabletID=1144
            24. partitionID=1046,tabletID=1108
            25. partitionID=1047,tabletID=1216
            26. partitionID=1048,tabletID=1180
            27. partitionID=1049,tabletID=1288
            28. partitionID=1050,tabletID=1252
            29. partitionID=1044,tabletID=1078
            30. partitionID=1045,tabletID=1150
            31. partitionID=1046,tabletID=1114
            32. partitionID=1047,tabletID=1222
            33. partitionID=1048,tabletID=1186
            34. partitionID=1049,tabletID=1294
            35. partitionID=1050,tabletID=1258
            36. partitionID=1044,tabletID=1084
            37. partitionID=1045,tabletID=1156
            38. partitionID=1046,tabletID=1120
            39. partitionID=1047,tabletID=1228
            40. partitionID=1048,tabletID=1192
            41. partitionID=1049,tabletID=1300
            42. partitionID=1050,tabletID=1264
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 1
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          16:0,1:0,4:0,7:0,10:0,13:0
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1056
            2. partitionID=1045,tabletID=1128
            3. partitionID=1046,tabletID=1092
            4. partitionID=1047,tabletID=1200
            5. partitionID=1048,tabletID=1164
            6. partitionID=1049,tabletID=1272
            7. partitionID=1050,tabletID=1236
            8. partitionID=1044,tabletID=1062
            9. partitionID=1045,tabletID=1134
            10. partitionID=1046,tabletID=1098
            11. partitionID=1047,tabletID=1206
            12. partitionID=1048,tabletID=1170
            13. partitionID=1049,tabletID=1278
            14. partitionID=1050,tabletID=1242
            15. partitionID=1044,tabletID=1068
            16. partitionID=1045,tabletID=1140
            17. partitionID=1046,tabletID=1104
            18. partitionID=1047,tabletID=1212
            19. partitionID=1048,tabletID=1176
            20. partitionID=1049,tabletID=1284
            21. partitionID=1050,tabletID=1248
            22. partitionID=1044,tabletID=1074
            23. partitionID=1045,tabletID=1146
            24. partitionID=1046,tabletID=1110
            25. partitionID=1047,tabletID=1218
            26. partitionID=1048,tabletID=1182
            27. partitionID=1049,tabletID=1290
            28. partitionID=1050,tabletID=1254
            29. partitionID=1044,tabletID=1080
            30. partitionID=1045,tabletID=1152
            31. partitionID=1046,tabletID=1116
            32. partitionID=1047,tabletID=1224
            33. partitionID=1048,tabletID=1188
            34. partitionID=1049,tabletID=1296
            35. partitionID=1050,tabletID=1260
            36. partitionID=1044,tabletID=1086
            37. partitionID=1045,tabletID=1158
            38. partitionID=1046,tabletID=1122
            39. partitionID=1047,tabletID=1230
            40. partitionID=1048,tabletID=1194
            41. partitionID=1049,tabletID=1302
            42. partitionID=1050,tabletID=1266

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
  |
  0:OlapScanNode
     TABLE: lineitem_partition
     PREAGGREGATION: ON
     partitions=7/7
     rollup: lineitem_partition
     tabletRatio=126/126
     tabletList=1054,1056,1058,1060,1062,1064,1066,1068,1070,1072 ...
     cardinality=1
     avgRowSize=8.0

[end]

[sql]
SELECT /*+SET_VAR(pipeline_dop=16,enable_tablet_internal_parallel=false)*/ l_orderkey, count(1) FROM lineitem_partition GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 16
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10002

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 6
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          17:5,2:0,5:1,8:2,11:3,14:4
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1058
            2. partitionID=1045,tabletID=1130
            3. partitionID=1046,tabletID=1094
            4. partitionID=1047,tabletID=1202
            5. partitionID=1048,tabletID=1166
            6. partitionID=1049,tabletID=1274
            7. partitionID=1050,tabletID=1238
          DriverSequence#1
            8. partitionID=1044,tabletID=1064
            9. partitionID=1045,tabletID=1136
            10. partitionID=1046,tabletID=1100
            11. partitionID=1047,tabletID=1208
            12. partitionID=1048,tabletID=1172
            13. partitionID=1049,tabletID=1280
            14. partitionID=1050,tabletID=1244
          DriverSequence#2
            15. partitionID=1044,tabletID=1070
            16. partitionID=1045,tabletID=1142
            17. partitionID=1046,tabletID=1106
            18. partitionID=1047,tabletID=1214
            19. partitionID=1048,tabletID=1178
            20. partitionID=1049,tabletID=1286
            21. partitionID=1050,tabletID=1250
          DriverSequence#3
            22. partitionID=1044,tabletID=1076
            23. partitionID=1045,tabletID=1148
            24. partitionID=1046,tabletID=1112
            25. partitionID=1047,tabletID=1220
            26. partitionID=1048,tabletID=1184
            27. partitionID=1049,tabletID=1292
            28. partitionID=1050,tabletID=1256
          DriverSequence#4
            29. partitionID=1044,tabletID=1082
            30. partitionID=1045,tabletID=1154
            31. partitionID=1046,tabletID=1118
            32. partitionID=1047,tabletID=1226
            33. partitionID=1048,tabletID=1190
            34. partitionID=1049,tabletID=1298
            35. partitionID=1050,tabletID=1262
          DriverSequence#5
            36. partitionID=1044,tabletID=1088
            37. partitionID=1045,tabletID=1160
            38. partitionID=1046,tabletID=1124
            39. partitionID=1047,tabletID=1232
            40. partitionID=1048,tabletID=1196
            41. partitionID=1049,tabletID=1304
            42. partitionID=1050,tabletID=1268
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 6
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          0:0,3:1,6:2,9:3,12:4,15:5
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1054
            2. partitionID=1045,tabletID=1126
            3. partitionID=1046,tabletID=1090
            4. partitionID=1047,tabletID=1198
            5. partitionID=1048,tabletID=1162
            6. partitionID=1049,tabletID=1270
            7. partitionID=1050,tabletID=1234
          DriverSequence#1
            8. partitionID=1044,tabletID=1060
            9. partitionID=1045,tabletID=1132
            10. partitionID=1046,tabletID=1096
            11. partitionID=1047,tabletID=1204
            12. partitionID=1048,tabletID=1168
            13. partitionID=1049,tabletID=1276
            14. partitionID=1050,tabletID=1240
          DriverSequence#2
            15. partitionID=1044,tabletID=1066
            16. partitionID=1045,tabletID=1138
            17. partitionID=1046,tabletID=1102
            18. partitionID=1047,tabletID=1210
            19. partitionID=1048,tabletID=1174
            20. partitionID=1049,tabletID=1282
            21. partitionID=1050,tabletID=1246
          DriverSequence#3
            22. partitionID=1044,tabletID=1072
            23. partitionID=1045,tabletID=1144
            24. partitionID=1046,tabletID=1108
            25. partitionID=1047,tabletID=1216
            26. partitionID=1048,tabletID=1180
            27. partitionID=1049,tabletID=1288
            28. partitionID=1050,tabletID=1252
          DriverSequence#4
            29. partitionID=1044,tabletID=1078
            30. partitionID=1045,tabletID=1150
            31. partitionID=1046,tabletID=1114
            32. partitionID=1047,tabletID=1222
            33. partitionID=1048,tabletID=1186
            34. partitionID=1049,tabletID=1294
            35. partitionID=1050,tabletID=1258
          DriverSequence#5
            36. partitionID=1044,tabletID=1084
            37. partitionID=1045,tabletID=1156
            38. partitionID=1046,tabletID=1120
            39. partitionID=1047,tabletID=1228
            40. partitionID=1048,tabletID=1192
            41. partitionID=1049,tabletID=1300
            42. partitionID=1050,tabletID=1264
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 6
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          16:5,1:0,4:1,7:2,10:3,13:4
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1056
            2. partitionID=1045,tabletID=1128
            3. partitionID=1046,tabletID=1092
            4. partitionID=1047,tabletID=1200
            5. partitionID=1048,tabletID=1164
            6. partitionID=1049,tabletID=1272
            7. partitionID=1050,tabletID=1236
          DriverSequence#1
            8. partitionID=1044,tabletID=1062
            9. partitionID=1045,tabletID=1134
            10. partitionID=1046,tabletID=1098
            11. partitionID=1047,tabletID=1206
            12. partitionID=1048,tabletID=1170
            13. partitionID=1049,tabletID=1278
            14. partitionID=1050,tabletID=1242
          DriverSequence#2
            15. partitionID=1044,tabletID=1068
            16. partitionID=1045,tabletID=1140
            17. partitionID=1046,tabletID=1104
            18. partitionID=1047,tabletID=1212
            19. partitionID=1048,tabletID=1176
            20. partitionID=1049,tabletID=1284
            21. partitionID=1050,tabletID=1248
          DriverSequence#3
            22. partitionID=1044,tabletID=1074
            23. partitionID=1045,tabletID=1146
            24. partitionID=1046,tabletID=1110
            25. partitionID=1047,tabletID=1218
            26. partitionID=1048,tabletID=1182
            27. partitionID=1049,tabletID=1290
            28. partitionID=1050,tabletID=1254
          DriverSequence#4
            29. partitionID=1044,tabletID=1080
            30. partitionID=1045,tabletID=1152
            31. partitionID=1046,tabletID=1116
            32. partitionID=1047,tabletID=1224
            33. partitionID=1048,tabletID=1188
            34. partitionID=1049,tabletID=1296
            35. partitionID=1050,tabletID=1260
          DriverSequence#5
            36. partitionID=1044,tabletID=1086
            37. partitionID=1045,tabletID=1158
            38. partitionID=1046,tabletID=1122
            39. partitionID=1047,tabletID=1230
            40. partitionID=1048,tabletID=1194
            41. partitionID=1049,tabletID=1302
            42. partitionID=1050,tabletID=1266

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
  |
  0:OlapScanNode
     TABLE: lineitem_partition
     PREAGGREGATION: ON
     partitions=7/7
     rollup: lineitem_partition
     tabletRatio=126/126
     tabletList=1054,1056,1058,1060,1062,1064,1066,1068,1070,1072 ...
     cardinality=1
     avgRowSize=8.0

[end]

[sql]
SELECT /*+SET_VAR(pipeline_dop=3,enable_tablet_internal_parallel=false)*/ l_orderkey, count(1) FROM lineitem_partition GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 3
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10003

PLAN FRAGMENT 1(F00)
  DOP: 3
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 3
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          17:2,2:0,5:1,8:2,11:0,14:1
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1058
            2. partitionID=1045,tabletID=1130
            3. partitionID=1046,tabletID=1094
            4. partitionID=1047,tabletID=1202
            5. partitionID=1048,tabletID=1166
            6. partitionID=1049,tabletID=1274
            7. partitionID=1050,tabletID=1238
            8. partitionID=1044,tabletID=1076
            9. partitionID=1045,tabletID=1148
            10. partitionID=1046,tabletID=1112
            11. partitionID=1047,tabletID=1220
            12. partitionID=1048,tabletID=1184
            13. partitionID=1049,tabletID=1292
            14. partitionID=1050,tabletID=1256
          DriverSequence#1
            15. partitionID=1044,tabletID=1064
            16. partitionID=1045,tabletID=1136
            17. partitionID=1046,tabletID=1100
            18. partitionID=1047,tabletID=1208
            19. partitionID=1048,tabletID=1172
            20. partitionID=1049,tabletID=1280
            21. partitionID=1050,tabletID=1244
            22. partitionID=1044,tabletID=1082
            23. partitionID=1045,tabletID=1154
            24. partitionID=1046,tabletID=1118
            25. partitionID=1047,tabletID=1226
            26. partitionID=1048,tabletID=1190
            27. partitionID=1049,tabletID=1298
            28. partitionID=1050,tabletID=1262
          DriverSequence#2
            29. partitionID=1044,tabletID=1070
            30. partitionID=1045,tabletID=1142
            31. partitionID=1046,tabletID=1106
            32. partitionID=1047,tabletID=1214
            33. partitionID=1048,tabletID=1178
            34. partitionID=1049,tabletID=1286
            35. partitionID=1050,tabletID=1250
            36. partitionID=1044,tabletID=1088
            37. partitionID=1045,tabletID=1160
            38. partitionID=1046,tabletID=1124
            39. partitionID=1047,tabletID=1232
            40. partitionID=1048,tabletID=1196
            41. partitionID=1049,tabletID=1304
            42. partitionID=1050,tabletID=1268
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 3
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          0:0,3:1,6:2,9:0,12:1,15:2
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1054
            2. partitionID=1045,tabletID=1126
            3. partitionID=1046,tabletID=1090
            4. partitionID=1047,tabletID=1198
            5. partitionID=1048,tabletID=1162
            6. partitionID=1049,tabletID=1270
            7. partitionID=1050,tabletID=1234
            8. partitionID=1044,tabletID=1072
            9. partitionID=1045,tabletID=1144
            10. partitionID=1046,tabletID=1108
            11. partitionID=1047,tabletID=1216
            12. partitionID=1048,tabletID=1180
            13. partitionID=1049,tabletID=1288
            14. partitionID=1050,tabletID=1252
          DriverSequence#1
            15. partitionID=1044,tabletID=1060
            16. partitionID=1045,tabletID=1132
            17. partitionID=1046,tabletID=1096
            18. partitionID=1047,tabletID=1204
            19. partitionID=1048,tabletID=1168
            20. partitionID=1049,tabletID=1276
            21. partitionID=1050,tabletID=1240
            22. partitionID=1044,tabletID=1078
            23. partitionID=1045,tabletID=1150
            24. partitionID=1046,tabletID=1114
            25. partitionID=1047,tabletID=1222
            26. partitionID=1048,tabletID=1186
            27. partitionID=1049,tabletID=1294
            28. partitionID=1050,tabletID=1258
          DriverSequence#2
            29. partitionID=1044,tabletID=1066
            30. partitionID=1045,tabletID=1138
            31. partitionID=1046,tabletID=1102
            32. partitionID=1047,tabletID=1210
            33. partitionID=1048,tabletID=1174
            34. partitionID=1049,tabletID=1282
            35. partitionID=1050,tabletID=1246
            36. partitionID=1044,tabletID=1084
            37. partitionID=1045,tabletID=1156
            38. partitionID=1046,tabletID=1120
            39. partitionID=1047,tabletID=1228
            40. partitionID=1048,tabletID=1192
            41. partitionID=1049,tabletID=1300
            42. partitionID=1050,tabletID=1264
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 3
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          16:2,1:0,4:1,7:2,10:0,13:1
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1056
            2. partitionID=1045,tabletID=1128
            3. partitionID=1046,tabletID=1092
            4. partitionID=1047,tabletID=1200
            5. partitionID=1048,tabletID=1164
            6. partitionID=1049,tabletID=1272
            7. partitionID=1050,tabletID=1236
            8. partitionID=1044,tabletID=1074
            9. partitionID=1045,tabletID=1146
            10. partitionID=1046,tabletID=1110
            11. partitionID=1047,tabletID=1218
            12. partitionID=1048,tabletID=1182
            13. partitionID=1049,tabletID=1290
            14. partitionID=1050,tabletID=1254
          DriverSequence#1
            15. partitionID=1044,tabletID=1062
            16. partitionID=1045,tabletID=1134
            17. partitionID=1046,tabletID=1098
            18. partitionID=1047,tabletID=1206
            19. partitionID=1048,tabletID=1170
            20. partitionID=1049,tabletID=1278
            21. partitionID=1050,tabletID=1242
            22. partitionID=1044,tabletID=1080
            23. partitionID=1045,tabletID=1152
            24. partitionID=1046,tabletID=1116
            25. partitionID=1047,tabletID=1224
            26. partitionID=1048,tabletID=1188
            27. partitionID=1049,tabletID=1296
            28. partitionID=1050,tabletID=1260
          DriverSequence#2
            29. partitionID=1044,tabletID=1068
            30. partitionID=1045,tabletID=1140
            31. partitionID=1046,tabletID=1104
            32. partitionID=1047,tabletID=1212
            33. partitionID=1048,tabletID=1176
            34. partitionID=1049,tabletID=1284
            35. partitionID=1050,tabletID=1248
            36. partitionID=1044,tabletID=1086
            37. partitionID=1045,tabletID=1158
            38. partitionID=1046,tabletID=1122
            39. partitionID=1047,tabletID=1230
            40. partitionID=1048,tabletID=1194
            41. partitionID=1049,tabletID=1302
            42. partitionID=1050,tabletID=1266

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
  |
  0:OlapScanNode
     TABLE: lineitem_partition
     PREAGGREGATION: ON
     partitions=7/7
     rollup: lineitem_partition
     tabletRatio=126/126
     tabletList=1054,1056,1058,1060,1062,1064,1066,1068,1070,1072 ...
     cardinality=1
     avgRowSize=8.0

[end]

[sql]
SELECT /*+SET_VAR(pipeline_dop=1, enable_tablet_internal_parallel=false)*/ l_orderkey, count(1) FROM lineitem_partition GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 1
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 1
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 1
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          17:0,2:0,5:0,8:0,11:0,14:0
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1058
            2. partitionID=1045,tabletID=1130
            3. partitionID=1046,tabletID=1094
            4. partitionID=1047,tabletID=1202
            5. partitionID=1048,tabletID=1166
            6. partitionID=1049,tabletID=1274
            7. partitionID=1050,tabletID=1238
            8. partitionID=1044,tabletID=1064
            9. partitionID=1045,tabletID=1136
            10. partitionID=1046,tabletID=1100
            11. partitionID=1047,tabletID=1208
            12. partitionID=1048,tabletID=1172
            13. partitionID=1049,tabletID=1280
            14. partitionID=1050,tabletID=1244
            15. partitionID=1044,tabletID=1070
            16. partitionID=1045,tabletID=1142
            17. partitionID=1046,tabletID=1106
            18. partitionID=1047,tabletID=1214
            19. partitionID=1048,tabletID=1178
            20. partitionID=1049,tabletID=1286
            21. partitionID=1050,tabletID=1250
            22. partitionID=1044,tabletID=1076
            23. partitionID=1045,tabletID=1148
            24. partitionID=1046,tabletID=1112
            25. partitionID=1047,tabletID=1220
            26. partitionID=1048,tabletID=1184
            27. partitionID=1049,tabletID=1292
            28. partitionID=1050,tabletID=1256
            29. partitionID=1044,tabletID=1082
            30. partitionID=1045,tabletID=1154
            31. partitionID=1046,tabletID=1118
            32. partitionID=1047,tabletID=1226
            33. partitionID=1048,tabletID=1190
            34. partitionID=1049,tabletID=1298
            35. partitionID=1050,tabletID=1262
            36. partitionID=1044,tabletID=1088
            37. partitionID=1045,tabletID=1160
            38. partitionID=1046,tabletID=1124
            39. partitionID=1047,tabletID=1232
            40. partitionID=1048,tabletID=1196
            41. partitionID=1049,tabletID=1304
            42. partitionID=1050,tabletID=1268
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 1
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          0:0,3:0,6:0,9:0,12:0,15:0
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1054
            2. partitionID=1045,tabletID=1126
            3. partitionID=1046,tabletID=1090
            4. partitionID=1047,tabletID=1198
            5. partitionID=1048,tabletID=1162
            6. partitionID=1049,tabletID=1270
            7. partitionID=1050,tabletID=1234
            8. partitionID=1044,tabletID=1060
            9. partitionID=1045,tabletID=1132
            10. partitionID=1046,tabletID=1096
            11. partitionID=1047,tabletID=1204
            12. partitionID=1048,tabletID=1168
            13. partitionID=1049,tabletID=1276
            14. partitionID=1050,tabletID=1240
            15. partitionID=1044,tabletID=1066
            16. partitionID=1045,tabletID=1138
            17. partitionID=1046,tabletID=1102
            18. partitionID=1047,tabletID=1210
            19. partitionID=1048,tabletID=1174
            20. partitionID=1049,tabletID=1282
            21. partitionID=1050,tabletID=1246
            22. partitionID=1044,tabletID=1072
            23. partitionID=1045,tabletID=1144
            24. partitionID=1046,tabletID=1108
            25. partitionID=1047,tabletID=1216
            26. partitionID=1048,tabletID=1180
            27. partitionID=1049,tabletID=1288
            28. partitionID=1050,tabletID=1252
            29. partitionID=1044,tabletID=1078
            30. partitionID=1045,tabletID=1150
            31. partitionID=1046,tabletID=1114
            32. partitionID=1047,tabletID=1222
            33. partitionID=1048,tabletID=1186
            34. partitionID=1049,tabletID=1294
            35. partitionID=1050,tabletID=1258
            36. partitionID=1044,tabletID=1084
            37. partitionID=1045,tabletID=1156
            38. partitionID=1046,tabletID=1120
            39. partitionID=1047,tabletID=1228
            40. partitionID=1048,tabletID=1192
            41. partitionID=1049,tabletID=1300
            42. partitionID=1050,tabletID=1264
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 1
      SCAN RANGES (per driver sequence)
        BUCKET SEQUENCE TO DRIVER SEQUENCE
          16:0,1:0,4:0,7:0,10:0,13:0
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1044,tabletID=1056
            2. partitionID=1045,tabletID=1128
            3. partitionID=1046,tabletID=1092
            4. partitionID=1047,tabletID=1200
            5. partitionID=1048,tabletID=1164
            6. partitionID=1049,tabletID=1272
            7. partitionID=1050,tabletID=1236
            8. partitionID=1044,tabletID=1062
            9. partitionID=1045,tabletID=1134
            10. partitionID=1046,tabletID=1098
            11. partitionID=1047,tabletID=1206
            12. partitionID=1048,tabletID=1170
            13. partitionID=1049,tabletID=1278
            14. partitionID=1050,tabletID=1242
            15. partitionID=1044,tabletID=1068
            16. partitionID=1045,tabletID=1140
            17. partitionID=1046,tabletID=1104
            18. partitionID=1047,tabletID=1212
            19. partitionID=1048,tabletID=1176
            20. partitionID=1049,tabletID=1284
            21. partitionID=1050,tabletID=1248
            22. partitionID=1044,tabletID=1074
            23. partitionID=1045,tabletID=1146
            24. partitionID=1046,tabletID=1110
            25. partitionID=1047,tabletID=1218
            26. partitionID=1048,tabletID=1182
            27. partitionID=1049,tabletID=1290
            28. partitionID=1050,tabletID=1254
            29. partitionID=1044,tabletID=1080
            30. partitionID=1045,tabletID=1152
            31. partitionID=1046,tabletID=1116
            32. partitionID=1047,tabletID=1224
            33. partitionID=1048,tabletID=1188
            34. partitionID=1049,tabletID=1296
            35. partitionID=1050,tabletID=1260
            36. partitionID=1044,tabletID=1086
            37. partitionID=1045,tabletID=1158
            38. partitionID=1046,tabletID=1122
            39. partitionID=1047,tabletID=1230
            40. partitionID=1048,tabletID=1194
            41. partitionID=1049,tabletID=1302
            42. partitionID=1050,tabletID=1266

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
  |
  0:OlapScanNode
     TABLE: lineitem_partition
     PREAGGREGATION: ON
     partitions=7/7
     rollup: lineitem_partition
     tabletRatio=126/126
     tabletList=1054,1056,1058,1060,1062,1064,1066,1068,1070,1072 ...
     cardinality=1
     avgRowSize=8.0

[end]

