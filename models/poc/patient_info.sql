-- models/adt_patient_info.sql
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

WITH
source_data as (
    SELECT
        patient_id as id,
        patient_name as name,
        patient_gender as gender,
        patient_birthdate as birthdate,
        created_at as updated_at
    FROM {{ ref('staging') }}
    WHERE message_type = 'ADT'

    {% if is_incremental() %}
    AND created_at > (SELECT max(updated_at) FROM {{ this }})
    {% endif %}
),

existing_data as (
    select 
        id,
        name,
        gender,
        birthdate,
        updated_at
    from 
        {{ this }}
    where id in (select id from source_data)
),

merged_data as (
    select 
        coalesce(src.id, dest.id) as id,
        coalesce(src.name, dest.name) as name,
        coalesce(src.gender, dest.gender) as gender,
        coalesce(src.birthdate, dest.birthdate) as birthdate,
        coalesce(src.updated_at, dest.updated_at) as updated_at
    from 
        source_data src
    left join  
        existing_data dest
    on 
        src.id = dest.id
)

select * from merged_data
