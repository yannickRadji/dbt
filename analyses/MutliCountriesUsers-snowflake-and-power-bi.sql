# https://github.com/Snowflake-Labs/sf-samples/blob/main/samples/tasty_bytes/FY25_Zero_To_Snowflake/tb_introduction.sql

/*
Now, that we've profiled the dataset, let's create a new schema to store our 
Dynamic Tables that will be queried by our Power BI semantic model.
*/
use role sysadmin;

create or replace schema tb_101.powerbi;

use schema tb_101.powerbi;

/*
-create a warehouse for Power BI if it doesn't already exist
-we won't use this warehouse just yet, but it will be the warehouse used within our Power BI semantic model
*/
create or replace warehouse tb_powerbi_wh
warehouse_size = 'MEDIUM'
max_cluster_count = 1
min_cluster_count = 1
auto_suspend = 300
initially_suspended = true
comment = 'Warehouse used for the TB Power BI DQ semantic model';


/* ensure we're still using our tb_dev_wh */
use warehouse tb_dev_wh;



-------------------------------------------------

--CREATE TEST USER, ROLES, AND GRANT PPRIVILEGES

-------------------------------------------------
/*
Create a new user called tb_bi_analyst that will be used to connect to Snowflake from Power BI

Use a strong, randomly generated password
*/
use role useradmin;

create or replace user tb_bi_analyst
--populate with your own password
  password = 'a'
  default_role = 'tb_bi_analyst_global';

ALTER USER tb_bi_analyst set password='a';

/* 
sample script for applying a user-level network policy:
use role accountadmin;
alter user tb_bi_analyst set network_policy = 'BI_ANALYST_NETWORK_POLICY';
*/

create or replace role tb_bi_analyst_global;
create or replace role tb_bi_analyst_emea;
create or replace role tb_bi_analyst_na;
create or replace role tb_bi_analyst_apac;

/* grant the roles to the user we created above */
grant role tb_bi_analyst_global to user tb_bi_analyst;
grant role tb_bi_analyst_emea to user tb_bi_analyst;
grant role tb_bi_analyst_na to user tb_bi_analyst;
grant role tb_bi_analyst_apac to user tb_bi_analyst;

/* assign roles to sysadmin */ 
grant role tb_bi_analyst_global to role sysadmin;
grant role tb_bi_analyst_emea to role sysadmin;
grant role tb_bi_analyst_na to role sysadmin;
grant role tb_bi_analyst_apac to role sysadmin;

/* grant permissions to database */
grant usage on database tb_101 to role tb_bi_analyst_global;
grant usage on database tb_101 to role tb_bi_analyst_emea;
grant usage on database tb_101 to role tb_bi_analyst_na;
grant usage on database tb_101 to role tb_bi_analyst_apac;

/* next, we'll add permissions to our powerbi schema */
grant all on schema tb_101.powerbi to role tb_data_engineer;
grant all on schema tb_101.powerbi to role tb_bi_analyst_global;
grant all on schema tb_101.powerbi to role tb_bi_analyst_emea;
grant all on schema tb_101.powerbi to role tb_bi_analyst_na;
grant all on schema tb_101.powerbi to role tb_bi_analyst_apac;

/* next, we'll add future grants so our analyst roles have access to any newly created objects */
grant all on future tables in schema tb_101.powerbi to role tb_data_engineer;
grant all on future tables in schema tb_101.powerbi to role tb_bi_analyst_global;
grant all on future tables in schema tb_101.powerbi to role tb_bi_analyst_emea;
grant all on future tables in schema tb_101.powerbi to role tb_bi_analyst_na;
grant all on future tables in schema tb_101.powerbi to role tb_bi_analyst_apac;

/* future grants for Dynamic Tables */
grant all on dynamic tables in schema tb_101.powerbi to role tb_data_engineer;
grant all on dynamic tables in schema tb_101.powerbi to role tb_bi_analyst_global;
grant all on dynamic tables in schema tb_101.powerbi to role tb_bi_analyst_emea;
grant all on dynamic tables in schema tb_101.powerbi to role tb_bi_analyst_na;
grant all on dynamic tables in schema tb_101.powerbi to role tb_bi_analyst_apac;

/* lastly, grant usage on the tb_powerbi_wh and the tb_dev_wh so they can be used by each role */
grant usage on warehouse tb_powerbi_wh to role tb_bi_analyst_global;
grant usage on warehouse tb_powerbi_wh to role tb_bi_analyst_emea;
grant usage on warehouse tb_powerbi_wh to role tb_bi_analyst_na;
grant usage on warehouse tb_powerbi_wh to role tb_bi_analyst_apac;

grant usage on warehouse tb_dev_wh to role tb_bi_analyst_global;
grant usage on warehouse tb_dev_wh to role tb_bi_analyst_emea;
grant usage on warehouse tb_dev_wh to role tb_bi_analyst_na;
grant usage on warehouse tb_dev_wh to role tb_bi_analyst_apac;

/*Dependent on getting the SafeGraph: frostbyte listing from marketplace */

/* set the worksheet context */
use role tb_dev;
use schema safegraph_frostbyte.public;
use warehouse tb_dev_wh;


/* sample the dataset */
select *
from frostbyte_tb_safegraph_s;

/* view location counts by country */
select 
    country,
    count(*)
from frostbyte_tb_safegraph_s
group by all;

/* issue a cross-database join to the raw_pos.location table and try joining on placekey */
select
    l.location_id,
    l.location,
    l.city as location_city,
    l.country as location_country,
    l.iso_country_code as location_country_iso,
    sg.top_category as location_category,
    sg.sub_category as location_subcategory,
    sg.latitude as location_latitude,
    sg.longitude as location_longitude,
    sg.street_address as location_street_address,
    sg.postal_code as location_postal_code
from tb_101.raw_pos.location l
left join safegraph_frostbyte.public.frostbyte_tb_safegraph_s sg
    ON sg.placekey = l.placekey;


/* create a copy of the shared SafeGraph data in the raw_pos schema so 
it can be included in our Dynamic Table definition in the next section */
create or replace table tb_101.raw_pos.safegraph_frostbyte_location
as
select *
from safegraph_frostbyte.public.frostbyte_tb_safegraph_s;

/* set worksheet context */
use role sysadmin;
use schema tb_101.powerbi;
use warehouse tb_dev_wh;


/*-------------------------------------------------

--CREATE STATIC DATE & TIME DIMENSIONS

-------------------------------------------------*/

/*
    --let's temporarily scale up our tb_de_wh to quickly create the fact tables and perform the initital data load
    --we'll scale this back down at the end
    --notice how Snowflake's elastic compute is available instantly!
*/
alter warehouse tb_de_wh set warehouse_size = '2x-large';


/*--------------------------------------------
--dim_date
--simple date dimension script sourced from - https://community.snowflake.com/s/question/0D50Z00008MprP2SAJ/snowflake-how-to-build-a-calendar-dim-table
--Can also easily source a free date dimension sourced from Marketplace providers
--------------------------------------------*/

/* set the date range to build date dimension */
set min_date = to_date('2018-01-01');
set max_date = to_date('2024-12-31');
set days = (select $max_date - $min_date);

create or replace table tb_101.powerbi.dim_date
(
   date_id int,
   date date,
   year string, 
   month smallint,  
   month_name string,  
   day_of_month smallint,  
   day_of_week  smallint,  
   weekday string,
   week_of_year smallint,  
   day_of_year  smallint,
   weekend_flag boolean
)
as
  with dates as 
  (
    select dateadd(day, SEQ4(), $min_date) as my_date
    from TABLE(generator(rowcount=> $days))  -- Number of days after reference date in previous line
  )
  select 
        to_number(replace(to_varchar(my_date), '-')),
        my_date,
        year(my_date),
        month(my_date),
        monthname(my_date),
        day(my_date),
        dayofweek(my_date),
        dayname(my_date),
        weekofyear(my_date),
        dayofyear(my_date),
        case when dayofweek(my_date) in (0,6) then 1 else 0 end as weekend_flag
    from dates;



/*--------------------------------------------
--dim_time
--simple time dimension (hour:min:seconds) script 
--------------------------------------------*/

--set the date range to build date dimension
set min_time = to_time('00:00:00');
set max_time = to_time('11:59:59');
set seconds = 86400;

create or replace table tb_101.powerbi.dim_time
(
  time_id int,
  time time,
  hour smallint,   
  minute smallint,  
  second smallint,   
  am_or_pm string,   
  hour_am_pm  string  
)
as
  with seconds as 
  (
    select timeadd(second, SEQ4(), $min_time) as my_time
    from table(generator(rowcount=> $seconds))  -- Number of seconds in a day
  )
  select
         to_number(left(to_varchar(my_time), 2) || substr(to_varchar(my_time),4, 2) || right(to_varchar(my_time), 2)),
         my_time,
         hour(my_time),
         minute(my_time),
         second(my_time),
         case
            when hour(my_time) < 12 THEN 'AM'
            else 'PM'
         end as am_or_pm,
         case
             when hour(my_time) = 0 THEN '12AM'
             when hour(my_time) < 12 THEN hour(my_time) || 'AM'
             when hour(my_time) = 12 THEN '12PM'
             when hour(my_time) = 13 THEN '1PM'
             when hour(my_time) = 14 THEN '2PM'
             when hour(my_time) = 15 THEN '3PM'
             when hour(my_time) = 16 THEN '4PM'
             when hour(my_time) = 17 THEN '5PM'
             when hour(my_time) = 18 THEN '6PM'
             when hour(my_time) = 19 THEN '7PM'
             when hour(my_time) = 20 THEN '8PM'
             when hour(my_time) = 21 THEN '9PM'
             when hour(my_time) = 22 THEN '10PM'
             when hour(my_time) = 23 THEN '11PM'
         end as Hour_am_pm
    from seconds;  


/*-------------------------------------------------

--DYNAMIC TABLES FOR OUR BUSINESS DIMENSIONS AND FACTS

-------------------------------------------------*/

/* dim_truck */
create or replace dynamic table dt_dim_truck
  target_lag = 'DOWNSTREAM'
  warehouse = tb_de_wh
  refresh_mode = incremental
  initialize = on_create
  as
    select distinct
        t.truck_id,
        t.franchise_id,
        m.truck_brand_name,
        t.primary_city as truck_city,
        t.region as truck_region,
        t.iso_region as truck_region_iso,
        t.country as truck_country,
        t.iso_country_code as truck_country_iso,
        t.franchise_flag,
        year as truck_year,
        (2023 - year) as truck_age,
        replace(t.make, 'Ford_', 'Ford') as truck_make,
        t.model as truck_model,
        t.ev_flag,
        t.truck_opening_date
    from tb_101.raw_pos.truck t
    join tb_101.raw_pos.menu m on m.menu_type_id = t.menu_type_id;

  
/* dim_franchise */
create or replace dynamic table dt_dim_franchise
  target_lag = 'DOWNSTREAM'
  warehouse = tb_de_wh
  refresh_mode = incremental
  initialize = on_create
  as
  with remove_duplicates as
  (
    select distinct
        f.franchise_id,
        f.first_name as franchise_first_name,
        f.last_name as franchise_last_name,
        f.city as franchise_city,
        f.country as franchise_country,
        f.e_mail as franchise_email,
        f.phone_number as franchise_phone_number,
        row_number()over(partition by franchise_id order by franchise_city ) as row_num
    from tb_101.raw_pos.franchise f
    )

    select *
    from remove_duplicates
    where row_num = 1;


/* dim_menu_item */
create or replace dynamic table dt_dim_menu_item
  target_lag = 'DOWNSTREAM'
  warehouse = tb_de_wh
  refresh_mode = incremental
  initialize = on_create
  as
     select 
        menu_item_id,
        menu_type_id,
        menu_type,
        item_category as menu_item_category,
        item_subcategory as menu_item_subcategory,
        menu_item_name,
        cost_of_goods_usd as cogs_usd,
        sale_price_usd,
        menu_item_health_metrics_obj:menu_item_health_metrics[0].ingredients as ingredients,
        menu_item_health_metrics_obj:menu_item_health_metrics[0].is_dairy_free_flag as is_dairy_free_flag,
        menu_item_health_metrics_obj:menu_item_health_metrics[0].is_gluten_free_flag as is_gluten_free_flag,
        menu_item_health_metrics_obj:menu_item_health_metrics[0].is_healthy_flag as is_healthy_flag,
        menu_item_health_metrics_obj:menu_item_health_metrics[0].is_nut_free_flag as is_nut_free_flag
    from tb_101.raw_pos.menu m;


/* dim_location */
create or replace dynamic table dt_dim_location
  target_lag = 'DOWNSTREAM'
  warehouse = tb_de_wh
  refresh_mode = incremental
  initialize = on_create
  as
    select
        l.location_id,
        l.location,
        l.city as location_city,
        case
            when l.country in ('England', 'France', 'Germany', 'Poland', 'Spain', 'Sweden') then 'EMEA'
            when l.country in ('Canada', 'Mexico', 'United States') then 'North America'
            when l.country in ('Australia', 'India', 'Japan', 'South Korea') then 'APAC'
            else 'Other'
        end as location_region,
        l.country as location_country,
        l.iso_country_code as location_country_iso,
  
        sg.top_category as location_category,
        sg.sub_category as location_subcategory,
        sg.latitude as location_latitude,
        sg.longitude as location_longitude,
        sg.street_address as location_street_address,
        sg.postal_code as location_postal_code

    from tb_101.raw_pos.location l;
    left join tb_101.raw_pos.safegraph_frostbyte_location sg on sg.placekey = l.placekey;


/* dim_customer */
create or replace dynamic table dt_dim_customer
  target_lag = 'DOWNSTREAM'
  warehouse = tb_de_wh
  refresh_mode = incremental
  initialize = on_create
  as
    select
        cl.customer_id,
        cl.first_name as customer_first_name,
        cl.last_name as customer_last_name,
        cl.first_name || ' ' || cl.last_name as customer_full_name,
        cl.last_name || ', ' || cl.first_name as customer_last_first_name,
        cl.city as customer_city,
        cl.country as customer_country,
        cl.postal_code as customer_postal_code,
        cl.preferred_language as customer_preferred_language,
        cl.gender as customer_gender,
        cl.favourite_brand as customer_favorite_band,
        cl.marital_status as customer_marital_status,
        cl.children_count as customer_children_count,
        cl.sign_up_date as customer_signup_date,
        cl.birthday_date as customer_dob,
        cl.e_mail as customer_email,
        cl.phone_number as customer_phone_number
    from tb_101.raw_customer.customer_loyalty cl;


/* fact_order_detail */
create or replace dynamic table dt_fact_order_detail
  target_lag = 'DOWNSTREAM'
  warehouse = tb_de_wh
  refresh_mode = incremental
  initialize = on_create
  as

    with natural_keys
    as
    (
        select 
        od.order_id,
        od.order_detail_id,
        oh.truck_id,
        t.franchise_id,
        cast(oh.location_id as int) as location_id,
        od.menu_item_id,
        to_date(oh.order_ts) as date_id,
        to_time(oh.order_ts) as time_id,
        oh.customer_id,
        od.quantity,
        od.unit_price,
        od.price as line_total
    from tb_101.raw_pos.order_detail od
    join tb_101.raw_pos.order_header oh on oh.order_id = od.order_id
    join tb_101.raw_pos.truck t on t.truck_id = oh.truck_id
    )


    select
        nk.order_id,
        nk.order_detail_id,
        dt.truck_id, 
        df.franchise_id, 
        dl.location_id, 
        dmi.menu_item_id, 
        dc.customer_id, 
        dd.date_id, 
        ti.time_id, 
        --measures
        nk.quantity,
        nk.unit_price,
        nk.line_total
    from natural_keys nk
    --dimension joins to enforce downstream dependencies in DT graph
    join powerbi.dt_dim_truck dt on dt.truck_id = nk.truck_id and dt.franchise_id = nk.franchise_id
    join powerbi.dt_dim_franchise df on df.franchise_id = nk.franchise_id
    join powerbi.dt_dim_location dl on dl.location_id = nk.location_id
    join powerbi.dt_dim_menu_item dmi on dmi.menu_item_id = nk.menu_item_id
    join powerbi.dim_date dd on dd.date = nk.date_id
    join powerbi.dim_time ti on ti.time = nk.time_id
    left join powerbi.dt_dim_customer dc on dc.customer_id = nk.customer_id;


  
/* fact_order_header */
create or replace dynamic table dt_fact_order_header
  target_lag = 'DOWNSTREAM'
  warehouse = tb_de_wh
  refresh_mode = full
  initialize = on_create
  as
    select
        order_id, 
        truck_id,
        franchise_id,
        location_id,
        customer_id,
        date_id,
        time_id,
        count(order_detail_id) as order_line_count,
        sum(quantity) as order_qty,
        sum(line_total) as order_total
    from dt_fact_order_detail
    group by 
        order_id, 
        truck_id,
        franchise_id,
        location_id,
        customer_id,
        date_id,
        time_id;


/* fact_order_agg */
create or replace dynamic table dt_fact_order_agg
  target_lag = '1 hour'
  warehouse = tb_de_wh
  refresh_mode = full
  initialize = on_create
  as
    select 
        truck_id,
        franchise_id,
        location_id,
        customer_id,
        date_id,
        count(order_id) as order_count, 
        sum(order_line_count) as order_line_count,
        sum(order_qty) as order_qty,
        sum(order_total) as order_total 
    from dt_fact_order_header
    group by
        truck_id,
        franchise_id,
        location_id,
        customer_id,
        date_id;

alter warehouse tb_de_wh set warehouse_size = 'small';

use role sysadmin;
use schema tb_101.powerbi;
use warehouse tb_dev_wh;

show tables in schema tb_101.powerbi;

/* 
run the built in procedure below to run the 
classification process against our Customer dimension */
call system$classify('tb_101.powerbi.dt_dim_customer', {'auto_tag': true});

/*
--view the system tags generated by Snowflake
--these can be viewed in the Snowsight UI as well via the Governance dashboard or directly on the DT_Dim_Customer table
*/
select *
from table(
  tb_101.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'tb_101.powerbi.dt_dim_customer',
    'table'
));

/* create custom tags */
create or replace tag tb_101.powerbi.pii_name_tag
    comment = 'PII Tag for Name Columns';
  
create or replace tag tb_101.powerbi.pii_phone_number_tag
    comment = 'PII Tag for Phone Number Columns';
  
create or replace tag tb_101.powerbi.pii_email_tag
    comment = 'PII Tag for E-mail Columns';

create or replace tag tb_101.powerbi.pii_dob_tag
    comment = 'PII Tag for Date of Birth Columns';

  
/* 
with the custom tags created, assign them to the relevant 
columns in our customer dimension dynamic table 
*/
alter table tb_101.powerbi.dt_dim_customer 
    modify column customer_first_name 
        set tag tb_101.powerbi.pii_name_tag = 'First Name';

alter table tb_101.powerbi.dt_dim_customer 
    modify column customer_last_name 
        set tag tb_101.powerbi.pii_name_tag = 'Last Name';

alter table tb_101.powerbi.dt_dim_customer 
    modify column customer_full_name 
        set tag tb_101.powerbi.pii_name_tag = 'Full Name';

alter table tb_101.powerbi.dt_dim_customer 
    modify column customer_last_first_name 
        set tag tb_101.powerbi.pii_name_tag = 'Full Name';   

alter table tb_101.powerbi.dt_dim_customer 
    modify column customer_phone_number 
        set tag tb_101.powerbi.pii_phone_number_tag = 'Phone Number';

alter table tb_101.powerbi.dt_dim_customer 
    modify column customer_email
        set tag tb_101.powerbi.pii_email_tag = 'E-mail Address';

alter table tb_101.powerbi.dt_dim_customer 
    modify column customer_dob
        set tag tb_101.powerbi.pii_dob_tag = 'Date of Birth';

        select *
from table(
  tb_101.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'tb_101.powerbi.dt_dim_customer',
    'table'
));


create or replace masking policy tb_101.powerbi.name_mask AS (val STRING) RETURNS STRING ->
    case 
        when CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN', 'TB_BI_ANALYST_GLOBAL') THEN val
    else '**~MASKED~**'
end;


/* create our phone_mask to return only the first 3 numbers unmasked */
create or replace masking policy tb_101.powerbi.phone_mask AS (val STRING) RETURNS STRING ->
    case
        when CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN', 'TB_BI_ANALYST_GLOBAL') THEN val
    else CONCAT(LEFT(val,3), '-***-****')
end;


/* create our email_mask to return ******@<provider>.com */
create or replace masking policy tb_101.powerbi.email_mask AS (val STRING) RETURNS STRING ->
    case 
        when CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    else CONCAT('******','@', SPLIT_PART(val, '@', -1))
end;


/* create our date of birth mask to truncate to first of month */
create or replace masking policy tb_101.powerbi.dob_mask AS (val date) RETURNS date ->
    case 
        when CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN', 'TB_BI_ANALYST_GLOBAL') THEN val
    else date_trunc('month', val)
end;

/* 
use an alter tag statement to set the masking policies and create tag-based masking policies
NOTE: since the tags have already been set on PII columns, the masking policies will be implicity applied
*/

alter tag tb_101.powerbi.pii_name_tag 
    set masking policy tb_101.powerbi.name_mask;
  
alter tag tb_101.powerbi.pii_phone_number_tag
    set masking policy tb_101.powerbi.phone_mask;
  
alter tag tb_101.powerbi.pii_email_tag
    set masking policy tb_101.powerbi.email_mask;

alter tag tb_101.powerbi.pii_dob_tag
    set masking policy tb_101.powerbi.dob_mask; 

/* validate that our tag-based masking policies work */
use role sysadmin;

/* 
notice how actual values are displayed when 
we use an elevated role like SYSADMIN 
*/
select 
    customer_first_name,
    customer_last_name,
    customer_full_name,
    customer_dob,
    customer_email,
    customer_phone_number
from tb_101.powerbi.dt_dim_customer limit 5;

/* now use one of the analyst roles created earlier */
use role tb_bi_analyst_na;

/* 
notice how masked values are displayed when 
we use our custom role
*/
select
    customer_first_name,
    customer_last_name,
    customer_full_name,
    customer_dob,
    customer_email,
    customer_phone_number
from tb_101.powerbi.dt_dim_customer limit 5;

/*-------------------------------------------------

--ROW ACCESS POLICIES

-------------------------------------------------*/

use role sysadmin;

create or replace table tb_101.powerbi.row_policy_map
    (
    role STRING, 
    location_id NUMBER
    );

/*
  - with the table in place, we will now insert the relevant role to location mappings
  - the TB_BI_ANALYST_GLOBAL role will have unrestricted access, while the region-specific roles will be mapped to specific regions
*/
insert into tb_101.powerbi.row_policy_map (role, location_id)
    select
        case 
            when location_region = 'EMEA' THEN 'TB_BI_ANALYST_EMEA'
            when location_region = 'North America' THEN 'TB_BI_ANALYST_NA'
            when location_region = 'APAC' THEN 'TB_BI_ANALYST_APAC'
        end AS role,
        location_id
    from dt_dim_location;


select * from tb_101.powerbi.row_policy_map;

create or replace row access policy tb_101.powerbi.rap_dim_location_policy
    as (location_id NUMBER) RETURNS BOOLEAN ->
       CURRENT_ROLE() in 
       /* list of roles that will not be subject to the policy  */
           (
            'ACCOUNTADMIN','SYSADMIN', 'TB_BI_ANALYST_GLOBAL'
           )
        or exists
        /* this clause references our mapping table from above to handle the row level filtering */
            (
            select rp.role 
                from tb_101.powerbi.row_policy_map rp
            where 1=1
                and rp.role = CURRENT_ROLE()
                and rp.location_id = location_id
            );

GRANT OWNERSHIP ON tb_101.powerbi.dt_fact_order_detail TO SYSADMIN REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON tb_101.powerbi.dt_fact_order_header TO SYSADMIN REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON tb_101.powerbi.dt_fact_order_agg TO SYSADMIN REVOKE CURRENT GRANTS;

/* Lastly, apply the row policy to our fact tables */
alter table tb_101.powerbi.dt_fact_order_detail
    add row access policy tb_101.powerbi.rap_dim_location_policy ON (location_id);

alter table tb_101.powerbi.dt_fact_order_header
    add row access policy tb_101.powerbi.rap_dim_location_policy ON (location_id);

alter table tb_101.powerbi.dt_fact_order_agg
    add row access policy tb_101.powerbi.rap_dim_location_policy ON (location_id);


/* validate that our tag-based masking policies work */

/* test with our global role*/


grant all on tb_101.powerbi.dt_fact_order_agg to role tb_data_engineer;
grant all on tb_101.powerbi.dt_fact_order_agg to role tb_bi_analyst_global;
grant all on tb_101.powerbi.dt_fact_order_agg to role tb_bi_analyst_emea;
grant all on tb_101.powerbi.dt_fact_order_agg to role tb_bi_analyst_na;
grant all on tb_101.powerbi.dt_fact_order_agg to role tb_bi_analyst_apac;

grant all on tb_101.powerbi.dt_dim_location to role tb_data_engineer;
grant all on tb_101.powerbi.dt_dim_location to role tb_bi_analyst_global;
grant all on tb_101.powerbi.dt_dim_location to role tb_bi_analyst_emea;
grant all on tb_101.powerbi.dt_dim_location to role tb_bi_analyst_na;
grant all on tb_101.powerbi.dt_dim_location to role tb_bi_analyst_apac;
/* Faire un GRANT ALL revoke le row access policies il faut le rerun la ca été fait à cause du revoke lors du transfer ownership*/

use role tb_bi_analyst_global;
select
    l.location_country,
    count(*) as record_count,
    sum(f.order_total) as sales_amt
from tb_101.powerbi.dt_fact_order_agg f
join tb_101.powerbi.dt_dim_location l on l.location_id = f.location_id
group by all;


/* test with our North America role */
use role tb_bi_analyst_na;

select
    l.location_country,
    count(*) as record_count,
    sum(f.order_total) as sales_amt
from tb_101.powerbi.dt_fact_order_agg f
join tb_101.powerbi.dt_dim_location l on l.location_id = f.location_id
group by all;


/* test with our APAC role */
use role tb_bi_analyst_apac;

select
    l.location_country,
    count(*) as record_count,
    sum(f.order_total) as sales_amt
from tb_101.powerbi.dt_fact_order_agg f
join tb_101.powerbi.dt_dim_location l on l.location_id = f.location_id
group by all;