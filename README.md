# Data Profiling (Snowflake)

## What It Does

- Accepts any SQL query as input
- Executes the query and loads it into a pandas DataFrame
- Generates a full HTML profiling report (nulls, distributions, correlations, etc.)
- Uploads the report to a specified Snowflake stage
- Returns the location of the uploaded file

## How to Set It Up
1. **Create the Python stored procedure**

<pre>
CREATE OR REPLACE PROCEDURE DATABASE_NAME.SCHEMA_NAME.STORED_PROCEDURE_NAME("SQL_QUERY" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'ydata-profiling')
HANDLER = 'profile_table'
EXECUTE AS CALLER
AS
$$
import _snowflake
import pandas as pd
from ydata_profiling import ProfileReport
import os
from datetime import datetime

def profile_table(session, sql_query):
    # Generate unique file name per user
    user = session.sql("SELECT CURRENT_USER()").to_pandas().iloc[0][0]
    report_file_name = f"{user}.html"
    report_file_path = f"/tmp/{report_file_name}"

    # Run the input SQL query and get results
    df = session.sql(sql_query).to_pandas()

    # Attempt to convert object columns to datetime if applicable
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_datetime(df[col], errors='raise')
            except Exception:
                pass

    # Generate the profiling report
    profile = ProfileReport(df)
    profile.to_file(report_file_path)

    # Upload the report to your Snowflake stage
    session.file.put(report_file_path, "DATABASE_NAME.SCHEMA_NAME.STAGE_NAME", auto_compress=False, overwrite=True)

    # Clean up the temporary file
    os.remove(report_file_path)

    return f"Report uploaded to @DATABASE_NAME.SCHEMA_NAME.STAGE_NAME as {report_file_name}"
$$;
</pre>

2. **Create the stage** 
<pre>
CREATE OR REPLACE STAGE DATABASE_NAME.SCHEMA_NAME.STAGE_NAME;
</pre>

## How to use

1. <pre>CALL DATABASE_NAME.SCHEMA_NAME.STORED_PROCEDURE_NAME('SELECT * FROM MY_DB.MY_SCHEMA.MY_TABLE');</pre> You should see a success message after the query runs.

2. Navigate to stage and download file
![Screenshot 2025-06-05 at 4 44 23 PM](https://github.com/user-attachments/assets/f2071ec4-c327-4879-9123-5f831b1db9d1)


Enjoy!
![image](https://github.com/user-attachments/assets/43503419-765e-4046-8a96-b6f33ede502e)



