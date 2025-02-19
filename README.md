This **dbt** project demonstrates:
- How to set up and run dbt locally using **PostgreSQL** but only works also with **Snowflake** in the cloud.
- How to handle multiple regional databases of people recieving blood (here labeled as `region1`, `region2`, etc.) and merge them into a single consolidated data set for patients, deliveries & blood analyses.

To set up initial data and then extra data use sql files in the analyses.