# ST_Distance_Sphere

## Description

Calculates the spherical distance between two points on the Earth in "meters". The parameters inputted are longitude at X, latitude at X, longitude at Y, latitude at Y.

## Syntax

```Haskell
DOUBLE ST_Distance_Sphere(DOUBLE x_lng, DOUBLE x_lat, DOUBLE y_lng, DOUBLE x_lat)
```

## Examples

```Plain Text
MySQL > select st_distance_sphere(116.35620117, 39.939093, 116.4274406433, 39.9020987219);
+----------------------------------------------------------------------------+
| st_distance_sphere(116.35620117, 39.939093, 116.4274406433, 39.9020987219) |
+----------------------------------------------------------------------------+
|                                                         7336.9135549995917 |
+----------------------------------------------------------------------------+
```

## keyword

ST_DISTANCE_SPHERE,ST,DISTANCE,SPHERE
