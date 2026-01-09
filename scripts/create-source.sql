CREATE USER delfos WITH PASSWORD 'pass#delf';

CREATE DATABASE "delfos-source";

\c delfos-source;

CREATE TABLE data (
    -- Column definitions
    timestamp timestamptz NOT NULL PRIMARY KEY, --timestamp with time zone
    wind_speed float,
    power float,
    ambient_temperature float,

    -- Sanity checks
    CONSTRAINT check_date_validity CHECK (timestamp > '2000-01-01'::timestamptz),
    CONSTRAINT check_wind_speed_positive CHECK ((wind_speed >= 0)),
    CONSTRAINT check_power_positive CHECK ((power >= 0)),
    CONSTRAINT check_temp_realistic CHECK (ambient_temperature BETWEEN -50 AND 60)
);

-- Index on timestamp for performance
CREATE INDEX idx_data_timestamp ON data (timestamp);

INSERT INTO data
SELECT
    ts AS timestamp,
    (random()*25)::numeric(5,2) AS wind_speed,
    (random()*3000)::numeric(6,2) AS power,
    (random()*50 - 10)::numeric(5,2) AS ambient_temperature
FROM
    generate_series(
        '2025-01-01 00:00:00'::timestamptz, --start
        '2025-01-11 00:00:00'::timestamptz, --stop
        '1 minute'::interval                --step
    ) AS ts;