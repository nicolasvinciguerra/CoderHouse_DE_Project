create or replace procedure pETL_load_dim_location()
language plpgsql
as $$
begin 
        -- merge changes to dim_location
        MERGE INTO dim_location
        USING stg_locations
        ON dim_location.id = stg_locations.id
        WHEN MATCHED THEN
            UPDATE SET
                id = stg_locations.id,
                city = stg_locations.city,
                name = stg_locations.name,
                country_code = stg_locations.country,
                sources = stg_locations.sources,
                ismobile = stg_locations.ismobile,
                isanalysis = stg_locations.isanalysis,
                sensortype = stg_locations.sensortype,
                coordinates_latitude = stg_locations.coordinates_latitude,
                coordinates_longitude = stg_locations.coordinates_longitude,
                updated_at = current_timestamp
        WHEN NOT MATCHED THEN
            INSERT (id, city, name, country_code, sources, ismobile, isanalysis, sensortype, 
                    coordinates_latitude, coordinates_longitude)
             VALUES (stg_locations.id, stg_locations.city, stg_locations.name, stg_locations.country, stg_locations.sources,
                    stg_locations.ismobile, stg_locations.isanalysis, stg_locations.sensortype, stg_locations.coordinates_latitude,
                    stg_locations.coordinates_longitude);
end;
$$

;
create or replace procedure pETL_update_country_dim_location()
language plpgsql
as $$
begin 
        -- update country name to dim location
        UPDATE dim_location
        SET country_name = stg_countries.name
        FROM stg_countries
        WHERE dim_location.country_code = stg_countries.code;
end;
$$

;
create or replace procedure pETL_load_dim_parameter()
language plpgsql
as $$
begin 
        -- merge changes to dim parameter
        MERGE INTO dim_parameter
        USING stg_parameters
        ON dim_parameter.id = stg_parameters.id
        WHEN MATCHED THEN
            UPDATE SET
                code = stg_parameters.name,
                displayName = stg_parameters.displayName,
                description = stg_parameters.description,
                preferredUnit = stg_parameters.preferredUnit,
                updated_at = current_timestamp
        WHEN NOT MATCHED THEN
            INSERT (id, code, displayName, description, preferredUnit)
            VALUES (stg_parameters.id, stg_parameters.name, stg_parameters.displayName, 
                    stg_parameters.description, stg_parameters.preferredUnit);
end;
$$

;
create or replace procedure pETL_load_fact_measure()
language plpgsql
as $$
begin 
         -- merge changes to fact_measure
        MERGE INTO fact_measure
        USING stg_measurements
        ON SHA1(stg_measurements.locationid + stg_measurements.parameter + stg_measurements.value + stg_measurements.date_utc)
            = SHA1(fact_measure.location_id + fact_measure.parameter_code + fact_measure.value + fact_measure.date_utc)
        WHEN MATCHED THEN
            UPDATE SET
                location_id = stg_measurements.locationid,
                parameter_code = stg_measurements.parameter,
                value = stg_measurements.value,
                date_local = stg_measurements.date_local,
                date_utc = stg_measurements.date_utc,
                updated_at = current_timestamp
        WHEN NOT MATCHED THEN
            INSERT (location_id, parameter_code, value, date_local, date_utc)
            VALUES (stg_measurements.locationid, stg_measurements.parameter, 
                stg_measurements.value, stg_measurements.date_local, stg_measurements.date_utc);
end;
$$
;