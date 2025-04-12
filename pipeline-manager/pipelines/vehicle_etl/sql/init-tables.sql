CREATE DATABASE IF NOT EXISTS vehicle_datalake;
USE vehicle_datalake;

CREATE TABLE IF NOT EXISTS events (
                                      id INT,
                                      make STRING,
                                      model STRING,
                                      year INT,
                                      color STRING,
                                      vin STRING,
                                      manufacturer STRING,
                                      gearPosition STRING,
                                      timestamp BIGINT
) USING csv OPTIONS (path "/app/data/vehicles.csv", header "true");

CREATE TABLE IF NOT EXISTS processed_vehicles (
                                                  id INT,
                                                  make STRING,
                                                  model STRING,
                                                  year INT,
                                                  color STRING,
                                                  vin STRING,
                                                  manufacturer STRING,
                                                  gearPosition INT
) PARTITIONED BY (date STRING, hour STRING);

SHOW TABLES;