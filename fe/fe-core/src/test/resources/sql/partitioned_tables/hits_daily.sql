-- hits_daily
CREATE TABLE hits_daily (
    EventDate DATE NOT NULL,
    EventTime DATETIME NOT NULL,
    EventDateS VARCHAR(128) NOT NULL,
    EventTimeS VARCHAR(128) NOT NULL,
    UserID BIGINT NOT NULL,
    M0 DECIMAL(7,2)
)  
DUPLICATE KEY (EventDate,EventTime,EventDateS,EventTimeS,UserID)
PARTITION BY RANGE(EventDate)(
    START("2024-01-01") END("2024-01-31") EVERY(INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(UserID) BUCKETS 48
PROPERTIES ( "replication_num"="1");
