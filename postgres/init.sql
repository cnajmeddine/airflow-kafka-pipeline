CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100),
    temperature FLOAT,
    humidity INTEGER,
    pressure INTEGER,
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_city_timestamp ON weather_data(city_name, timestamp);