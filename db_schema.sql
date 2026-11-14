-- Mirrors the normalised record produced by ingest.make_record (not yet written by the pipeline;
-- ingest.py currently stores everything in latest_forecasts.json).
CREATE TABLE IF NOT EXISTS weather_forecasts (
    id BIGSERIAL PRIMARY KEY,
    provider VARCHAR(100) NOT NULL,
    model_source VARCHAR(100),
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    forecast_date DATE NOT NULL,            -- local (IST) calendar day
    -- thermal
    temp_min_c DOUBLE PRECISION,
    temp_max_c DOUBLE PRECISION,
    feels_like_max_c DOUBLE PRECISION,
    dew_point_c DOUBLE PRECISION,
    -- precipitation
    precip_mm DOUBLE PRECISION,
    precip_prob_pct DOUBLE PRECISION,
    precip_hours DOUBLE PRECISION,
    precip_type VARCHAR(40),
    -- wind
    wind_speed_kph DOUBLE PRECISION,
    wind_basis VARCHAR(8),                  -- 'max' or 'avg'
    wind_gust_kph DOUBLE PRECISION,
    wind_dir_deg DOUBLE PRECISION,
    -- solar
    uv_index DOUBLE PRECISION,
    sunshine_hours DOUBLE PRECISION,
    solar_radiation_wm2 DOUBLE PRECISION,   -- daily mean
    -- sky & atmosphere
    cloud_cover_pct DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,          -- mean sea level
    visibility_km DOUBLE PRECISION,
    -- marine
    wave_height_m DOUBLE PRECISION,
    swell_height_m DOUBLE PRECISION,
    sea_surface_temp_c DOUBLE PRECISION,
    -- astronomy
    sunrise VARCHAR(5),
    sunset VARCHAR(5),
    moon_phase VARCHAR(30),
    moon_illumination_pct DOUBLE PRECISION,
    -- quality
    coverage_hours SMALLINT,                -- hours of the local day the provider covered
    flags TEXT[] NOT NULL DEFAULT '{}',     -- e.g. {'trial'}, {'reanalysis'}
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload JSONB,
    UNIQUE (provider, model_source, city, forecast_date, fetched_at)
);

CREATE INDEX IF NOT EXISTS idx_forecasts_city_date ON weather_forecasts (city, forecast_date);
