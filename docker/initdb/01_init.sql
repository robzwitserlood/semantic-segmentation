-- Enable PostGIS on the default database (aerial_imagery)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create and configure the satellite database
CREATE DATABASE satellite_imagery;
\connect satellite_imagery
CREATE EXTENSION IF NOT EXISTS postgis;
