{% snapshot patients_region1 %}

{{
    config(
      target_schema='snapshots',
      unique_key='patient_id',

      strategy='check',
      check_cols=['first_name','last_name','birth_date','region'],
    )
}}

select * from {{ ref('region1_raw_patients') }}

{% endsnapshot %}

{% snapshot patients_region2 %}

{{
    config(
      target_schema='snapshots',
      unique_key='patient_id',

      strategy='check',
      check_cols=['first_name','last_name','birth_date','region'],
    )
}}

select * from {{ ref('region2_raw_patients') }}

{% endsnapshot %}

{% snapshot patients_convergence %}

{{
    config(
      target_schema='snapshots',
      unique_key='patient_id',

      strategy='check',
      check_cols=['first_name','last_name','birth_date','region', 'start_date'],
    )
}}

select * from {{ ref('convergence_patients') }}

{% endsnapshot %}