{{ config(schema='staging') }}

WITH excluded AS (
    SELECT
        id 
    FROM 
        {{ source('audit', 'source_json_property_exists_ra_23a028027a4198c7868836d301831362') }}
)

extracted_data AS (    
    SELECT
        CAST(JSON_VALUE(data, '$.message_event_time') AS DATETIME) AS message_event_time,
        CAST(JSON_VALUE(data, '$.patient_mrn') AS VARCHAR(255)) AS patient_id,
        CAST(JSON_VALUE(data, '$.patient_name') AS VARCHAR(255)) AS patient_name,
        CAST(JSON_VALUE(data, '$.patient_gender') AS VARCHAR(255)) AS patient_gender,
        CAST(JSON_VALUE(data, '$.patient_demo_primary_phone') AS VARCHAR(255)) AS patient_demo_primary_phone,
        CAST(JSON_VALUE(data, '$.patient_dob') AS DATE) AS patient_dob
    FROM
        {{ source('raw', 'philips_raw_messages') }}
    WHERE
        message_type IN ('ORM', 'ORU')
    AND
        id NOT IN (SELECT id FROM excluded)
)

SELECT
    *
FROM
    extracted_data