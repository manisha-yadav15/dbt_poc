-- models/staging.sql
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='append'
) }}

SELECT
    id,
    JSONExtractString(message, 'patient_mrn') as patient_id,
    JSONExtractString(message, 'patient_name') as patient_name,
    JSONExtractString(message, 'patient_gender') as patient_gender,
    CAST(JSONExtractString(message, 'patient_dob') AS Date) as patient_birthdate,
    JSONExtractString(message, 'site_id') as site_id,
    JSONExtractString(message, 'site') as site,
    JSONExtractString(message, 'site_name') as site_name,
    CAST(JSONExtractString(message, 'site_class') AS String) as site_class,
    message_event_time as created_at, 
    message_type
FROM {{ source('helloworld', 'json_messages_raw') }}

{% if is_incremental() %}
WHERE message_event_time > (SELECT max(created_at) FROM {{ this }})
{% endif %}
