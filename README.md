# DBT DEMO

This **dbt** project demonstrates:
- How to set up and run dbt locally using **PostgreSQL** , works also with **Snowflake** in the cloud.
- How to handle multiple regional databases of people recieving blood (here labeled as `region1`, `region2`, etc.) and merge them into a single consolidated data set for patients, deliveries & blood analyses.

To set up initial data and then extra data use sql files in the analyses.
All data models come with two technical columns prepend by "_"
There is doc that I recommend to read to do so:

## (Optional) Create a virtual environment
C:\ProgramData\miniconda3\python.exe -m venv .venv
source .venv/bin/activate  # macOS/Linux
.\.venv\Scripts\activate   # Windows

## Install dbt packages:
pip install -r requirements.txt

## Configuration
dbt requires a profiles.yml file (usually in ~/.dbt/profiles.yml) to define connection details.

dbt deps     # Install dbt packages if any are referenced

dbt docs generate
dbt docs serve

run the setup.sql
dbt run      # Execute all models
dbt test     # Run tests defined in /tests or embedded in models
(or do dbt build to run+test+snapshot in DAG order)
run extradata.sql
dbt build

enjoy