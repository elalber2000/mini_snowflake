# mini_snowflake
Custom implementation of a MiniSnowflake using parallel duckdb instances


execute(
    "CREATE TABLE events(
        event_id    INT,
        user_id     INT,
        event_type  VARCHAR,
        value       DOUBLE,
        event_time  TIMESTAMP
    )"
)

df = pd.DataFrame(
    {
        "event_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "user_id":  [10, 10, 11, 12, None, 13, 13, 14, None, 15],
        "event_type": [
            "click",
            "click",
            "view",
            "click",
            "view",
            "purchase",
            "purchase",
            "click",
            "view",
            "click",
        ],
        "value": [1.5, 2.0, 0.0, 3.5, 1.0, 20.0, 30.0, 1.0, 0.5, -1.0],
        "event_time": [
            "2025-01-01 10:00:00",
            "2025-01-01 10:05:00",
            "2025-01-01 10:10:00",
            "2025-01-02 09:00:00",
            "2025-01-02 09:30:00",
            "2025-01-02 10:00:00",
            "2025-01-03 11:00:00",
            "2025-01-03 11:30:00",
            "2025-01-03 12:00:00",
            "2025-01-04 08:00:00",
        ],
    }
)
execute(
    "INSERT INTO events",
    df
)

execute(
    "SELECT *
    FROM events;"
)
execute(
    "SELECT event_id, event_type
    FROM events;"
)
execute(
    "SELECT event_id, value
    FROM events
    WHERE event_type = 'click'
        AND value > 1.0;"
)
execute(
    "SELECT
        COUNT(*) AS n,
        SUM(value) AS total_value
    FROM events;"
)
execute(
    "SELECT
        event_type,
        COUNT(*) AS n_events
    FROM events
    GROUP BY event_type;"
)
execute(
    "SELECT
        event_type,
        COUNT(*) AS n_events
    FROM events
    WHERE value >= 1.0
    GROUP BY event_type;"
)
execute(
    "SELECT
        event_type,
        COUNT(*) AS n,
        SUM(value) AS total,
        AVG(value) AS avg
    FROM events
    WHERE user_id IS NOT NULL
    GROUP BY event_type;"
)
execute(
    "SELECT
        event_type,
        COUNT(*) AS n,
        SUM(value) AS total,
        AVG(value) AS avg
    FROM events
    WHERE user_id IS NOT NULL
    GROUP BY event_type;"
)


// Test with 1-n docker container workers and plot speed