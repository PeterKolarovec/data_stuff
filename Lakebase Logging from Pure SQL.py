# Databricks notebook source
# DBTITLE 1,PoC Overview
# MAGIC %md
# MAGIC ## PoC: Python UDF → Lakebase Logging (without package installation from pure SQL)
# MAGIC
# MAGIC **Goal:** Verify whether it's possible to write logs to a Lakebase table from pure SQL (by calling a function) without needing to install anything in the client environment.
# MAGIC
# MAGIC **Architecture:**
# MAGIC 1. Python UDF in Unity Catalog with `ENVIRONMENT` clause (automatically installs `psycopg` on first call)
# MAGIC 2. Lakebase instance as the target log storage
# MAGIC 3. OAuth short-term credentials via `generate_database_credential()` from Databricks SDK
# MAGIC
# MAGIC **Key questions to verify:**
# MAGIC - ✅ Does `psycopg` work with `ENVIRONMENT` clause in Python UDF?
# MAGIC - ❓ Does `WorkspaceClient` / OAuth work inside the UDF sandbox?
# MAGIC - ❓ If not, what's the alternative for securely storing credentials?
# MAGIC - ❓ Is the entire flow actually usable as `SELECT log_to_lakebase(...)`?

# COMMAND ----------

# DBTITLE 1,Install psycopg
# MAGIC %pip install "psycopg[binary,pool]" --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python after install
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration
# === CONFIGURATION ===
# Create widget parameters with default values
dbutils.widgets.text("catalog", "my_catalog", "Catalog")
dbutils.widgets.text("schema", "my_schema", "Schema")

# Read from widgets
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
INSTANCE_NAME = "pg-dev"  # Change this to your Lakebase instance name

# Derived paths
FQ_SCHEMA = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/udf_libs"

print(f"Catalog:   {CATALOG}")
print(f"Schema:    {SCHEMA}")
print(f"FQ Schema: {FQ_SCHEMA}")
print(f"Volume:    {VOLUME_PATH}")
print(f"Lakebase:  {INSTANCE_NAME}")

# COMMAND ----------

# DBTITLE 1,Step 1: Get Lakebase instance info & generate credentials
# Step 1: Test connection to Lakebase using OAuth short-term credentials
import psycopg
import uuid
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get instance details
instance = w.database.get_database_instance(name=INSTANCE_NAME)
print(f"Instance: {instance.name}")
print(f"Host: {instance.read_write_dns}")
print(f"State: {instance.state}")

# Generate short-term OAuth credential
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[INSTANCE_NAME]
)
print(f"\nCredential generated (token length: {len(cred.token)} chars)")
print(f"Token expires in ~60 minutes")

# COMMAND ----------

# DBTITLE 1,Step 2: Connect & create logging table
# Step 2: Connect to Lakebase and create logging table
import psycopg
from datetime import datetime

# Get current user for the connection
current_user = w.current_user.me()
username = current_user.user_name
print(f"Connecting as: {username}")

conn = psycopg.connect(
    host=instance.read_write_dns,
    dbname="databricks_postgres",
    user=username,
    password=cred.token,
    sslmode="require"
)

print("\n✅ Connected to Lakebase successfully!")

# Verify connection
with conn.cursor() as cur:
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    print(f"PostgreSQL version: {version[:60]}...")

# Create logging table
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.app_log (
            id SERIAL PRIMARY KEY,
            log_time TIMESTAMP DEFAULT NOW(),
            log_level VARCHAR(10) NOT NULL,
            source VARCHAR(100),
            message TEXT,
            extra JSONB
        )
    """)
    conn.commit()
    print("\n✅ Table public.app_log created (or already exists)")

# Test insert
with conn.cursor() as cur:
    cur.execute("""
        INSERT INTO public.app_log (log_level, source, message, extra)
        VALUES (%s, %s, %s, %s)
        RETURNING id, log_time
    """, ('INFO', 'poc_notebook', 'PoC connectivity test successful', '{"step": "initial_test"}'))
    row = cur.fetchone()
    conn.commit()
    print(f"\n✅ Test log inserted: id={row[0]}, time={row[1]}")

conn.close()
print("\nConnection closed. Notebook-level connectivity: CONFIRMED")

# COMMAND ----------

# DBTITLE 1,Approaches
# MAGIC %md
# MAGIC ### Key Experiment: Python UDF with ENVIRONMENT clause
# MAGIC
# MAGIC **Approach A** (ideal): UDF self-generates OAuth token via `databricks-sdk` → no secrets, no parameters
# MAGIC
# MAGIC **Approach B** (fallback): Token generated externally and passed as parameter → requires wrapper
# MAGIC
# MAGIC **Approach C** (production): Native Postgres role with password in Databricks secrets → more stable than OAuth

# COMMAND ----------

# DBTITLE 1,Step 3A: Create UDF with OAuth self-auth (ideal)
# Step 3A: Create self-authenticating UDF (Approach A)
# Key insight: UDF sandbox CAN reach workspace API + Azure AD
# but CANNOT discover auth context automatically.
# Solution: embed credentials in UDF body at creation time.
#
# For PRODUCTION: use a Service Principal with minimal permissions
# For PoC: use current session token (short-lived)

import uuid
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
workspace_host = w.config.host
auth_token = w.config.authenticate().get('Authorization', '').replace('Bearer ', '')

# Build the SQL separately to avoid f-string escaping hell
udf_sql = f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.log_to_lakebase_v2(
    log_level STRING,
    source STRING,
    message STRING
)
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
    dependencies = '[""" + f'"{VOLUME_PATH}/psycopg-3.3.4-py3-none-any.whl", "{VOLUME_PATH}/psycopg_binary-3.3.4-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl"' + f"""]',
    environment_version = 'None'
)
AS $func$
import psycopg
import uuid
import json

_HOST = "{workspace_host}"
_TOKEN = "{auth_token}"
_INSTANCE = "{INSTANCE_NAME}"

def main(log_level, source, message):
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(host=_HOST, token=_TOKEN)
        
        instance = w.database.get_database_instance(name=_INSTANCE)
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[_INSTANCE]
        )
        current_user = w.current_user.me()
        
        conn = psycopg.connect(
            host=instance.read_write_dns,
            dbname="databricks_postgres",
            user=current_user.user_name,
            password=cred.token,
            sslmode="require",
            connect_timeout=10
        )
        
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.app_log (log_level, source, message, extra) VALUES (%s, %s, %s, %s) RETURNING id",
                (log_level, source, message, json.dumps(dict(auth="approach_a", self_auth=True)))
            )
            row_id = cur.fetchone()[0]
            conn.commit()
        
        conn.close()
        return f"OK:id=" + str(row_id)
        
    except Exception as e:
        return f"ERROR:" + type(e).__name__ + ":" + str(e)[:300]

return main(log_level, source, message)
$func$
"""

spark.sql(udf_sql)

print(f"✅ UDF {FQ_SCHEMA}.log_to_lakebase_v2 created (Approach A - self-authenticating!)")
print(f"   Workspace: {workspace_host}")
print(f"   Token embedded: {auth_token[:8]}... ({len(auth_token)} chars)")
print(f"\n   Pure SQL usage: SELECT {FQ_SCHEMA}.log_to_lakebase_v2('INFO', 'app', 'msg')")

# COMMAND ----------

# DBTITLE 1,Step 3A Test: Call OAuth UDF from SQL
# MAGIC %sql
# MAGIC -- Step 3A Test: Pure SQL call - ZERO parameters, fully self-authenticating!
# MAGIC -- The UDF reads credentials embedded at creation time, generates short-term DB creds,
# MAGIC -- and writes to Lakebase autonomously.
# MAGIC -- Uses IDENTIFIER() for SQL Warehouse compatibility
# MAGIC SELECT IDENTIFIER(:catalog || '.' || :schema || '.log_to_lakebase_v2')(
# MAGIC     'INFO',
# MAGIC     'pure_sql_approach_a',
# MAGIC     'Approach A works! Self-authenticating UDF from pure SQL.'
# MAGIC ) AS result

# COMMAND ----------

# DBTITLE 1,Step 3A Test (alternative)
# MAGIC %sql
# MAGIC -- Alternative test
# MAGIC -- Uses IDENTIFIER() for SQL Warehouse compatibility
# MAGIC SELECT IDENTIFIER(:catalog || '.' || :schema || '.log_to_lakebase_v2')(
# MAGIC     'INFO',
# MAGIC     'pure_sql_approach_a',
# MAGIC     'Approach A works! Self-authenticating UDF from pure SQL.'
# MAGIC ) AS result

# COMMAND ----------

# DBTITLE 1,Step 3B: Create UDF with credential params (fallback)
# Step 3B (Fallback): UDF that accepts connection params
# Approach A fails (UDF sandbox can't authenticate SDK)
# This approach passes credentials generated externally

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.log_to_lakebase(
    log_level STRING,
    source STRING,
    message STRING,
    pg_host STRING,
    pg_user STRING,
    pg_password STRING
)
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
    dependencies = '["{VOLUME_PATH}/psycopg-3.3.4-py3-none-any.whl", "{VOLUME_PATH}/psycopg_binary-3.3.4-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl"]',
    environment_version = 'None'
)
AS $$
import psycopg
import json

def main(log_level, source, message, pg_host, pg_user, pg_password):
    try:
        conn = psycopg.connect(
            host=pg_host,
            dbname="databricks_postgres",
            user=pg_user,
            password=pg_password,
            sslmode="require",
            connect_timeout=10
        )
        
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.app_log (log_level, source, message, extra) VALUES (%s, %s, %s, %s) RETURNING id, log_time",
                (log_level, source, message, json.dumps({{"auth": "param_pass", "udf": True}}))
            )
            row = cur.fetchone()
            conn.commit()
        
        conn.close()
        return f"OK:id={{row[0]}},time={{row[1]}}"
        
    except Exception as e:
        return f"ERROR:{{type(e).__name__}}:{{str(e)[:200]}}"

return main(log_level, source, message, pg_host, pg_user, pg_password)
$$
""")

print(f"✅ UDF {FQ_SCHEMA}.log_to_lakebase created (with credential params)")

# COMMAND ----------

# DBTITLE 1,Step 3B Test: Call UDF with external credentials
# Step 3B Test: Generate credential externally and pass to UDF
import uuid
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Generate fresh credential
instance = w.database.get_database_instance(name=INSTANCE_NAME)
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[INSTANCE_NAME]
)
current_user = w.current_user.me()

print(f"Host: {instance.read_write_dns}")
print(f"User: {current_user.user_name}")
print(f"Token length: {len(cred.token)}")

# Call UDF with full credentials
result = spark.sql(f"""
    SELECT {FQ_SCHEMA}.log_to_lakebase(
        'INFO',
        'poc_test_parampass',
        'Test: credential passed from driver to UDF',
        '{instance.read_write_dns}',
        '{current_user.user_name}',
        '{cred.token}'
    ) AS result
""")

display(result)

# COMMAND ----------

# DBTITLE 1,Approach C: SQL Procedure Wrapper
# MAGIC %md
# MAGIC ### Approach C: SQL Stored Procedure as wrapper
# MAGIC
# MAGIC If Approach A doesn't work (UDF sandbox blocks SDK), we can create a **SQL Stored Procedure** that:
# MAGIC 1. Uses Python UDF to generate credentials (if SDK works in UDF)
# MAGIC 2. Or uses Databricks Secrets to retrieve stored credentials
# MAGIC 3. Provides clean SQL interface: `CALL log_event('INFO', 'source', 'message')`

# COMMAND ----------

# DBTITLE 1,Step 4: Create credential helper UDF
# Step 4: Create credential helper UDF (test if SDK auth works in sandbox)
# databricks-sdk is pre-installed → no ENVIRONMENT needed
# Expected: will FAIL with same auth error as Approach A

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ_SCHEMA}.get_lakebase_cred()
RETURNS STRING
LANGUAGE PYTHON
AS $$
import uuid
import json

def main():
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        
        instance_name = "{INSTANCE_NAME}"
        instance = w.database.get_database_instance(name=instance_name)
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance_name]
        )
        current_user = w.current_user.me()
        
        return json.dumps({{
            "host": instance.read_write_dns,
            "user": current_user.user_name,
            "token": cred.token,
            "status": "ok"
        }})
    except Exception as e:
        return json.dumps({{"status": "error", "error": f"{{type(e).__name__}}: {{str(e)[:200]}}"}})

return main()
$$
""")

print(f"✅ Helper UDF {FQ_SCHEMA}.get_lakebase_cred created")
print("Testing if SDK auth works in UDF sandbox (expected: NO)...")

# COMMAND ----------

# DBTITLE 1,Step 4 Test: Get credentials from UDF sandbox
# Test: Can we get credentials from inside a UDF? (Expected: error - no auth context)
display(spark.sql(f"SELECT {FQ_SCHEMA}.get_lakebase_cred() AS credential_result"))

# COMMAND ----------

# DBTITLE 1,Step 5: Create SQL Stored Procedure wrapper
# Step 5: Create SQL Stored Procedure - the final clean interface
# Since UDF sandbox can't self-authenticate, the procedure uses:
# - Hardcoded host (stable, not secret)
# - Hardcoded service user (could be a dedicated PG role)
# - Password passed as param (in production: from secrets or native PG role)
#
# For PRODUCTION: create a native Postgres role with password stored in Databricks secrets
# For this PoC: we demonstrate the pattern works

spark.sql(f"""
CREATE OR REPLACE PROCEDURE {FQ_SCHEMA}.log_event(
    log_level STRING,
    source STRING,
    message STRING
)
SQL SECURITY INVOKER
BEGIN
    -- In production, these would come from a secure source
    -- For PoC: hardcode host (not secret) and pass token
    DECLARE pg_host STRING DEFAULT '{instance.read_write_dns}';
    DECLARE pg_user STRING DEFAULT '{current_user.user_name}';
    DECLARE pg_token STRING DEFAULT '{cred.token}';
    
    -- Call the logging UDF with connection params
    SELECT {FQ_SCHEMA}.log_to_lakebase(
        log_level,
        source,
        message,
        pg_host,
        pg_user,
        pg_token
    ) AS result;
END
""")

print(f"✅ Stored Procedure {FQ_SCHEMA}.log_event created")
print("\n⚠️  Note: This PoC version has credentials baked in (valid ~60min).")
print("    Production version would use a native PG role with stable password.")
print(f"\nUsage from pure SQL:")
print(f"  CALL {FQ_SCHEMA}.log_event('INFO', 'my_app', 'Hello from SQL!')")

# COMMAND ----------

# DBTITLE 1,Step 6: FINAL TEST - Pure SQL call
# Step 6: THE FINAL TEST - Call from pure SQL!
# This is what end users would do - no imports, no installs, just CALL
display(spark.sql(f"CALL {FQ_SCHEMA}.log_event('INFO', 'pure_sql_test', 'Logging works from pure SQL without any package installation!')"))

# COMMAND ----------

# DBTITLE 1,Step 7: Verify logs written to Lakebase
# Step 7: Verify logs in Lakebase
import psycopg
import uuid
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
instance = w.database.get_database_instance(name=INSTANCE_NAME)
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[INSTANCE_NAME]
)
current_user = w.current_user.me()

conn = psycopg.connect(
    host=instance.read_write_dns,
    dbname="databricks_postgres",
    user=current_user.user_name,
    password=cred.token,
    sslmode="require"
)

with conn.cursor() as cur:
    cur.execute("SELECT id, log_time, log_level, source, message, extra FROM public.app_log ORDER BY id DESC LIMIT 10")
    rows = cur.fetchall()
    
conn.close()

# Display as DataFrame
import pandas as pd
df = pd.DataFrame(rows, columns=['id', 'log_time', 'log_level', 'source', 'message', 'extra'])
display(df)

# COMMAND ----------

# DBTITLE 1,Conclusions & Findings
# MAGIC %md
# MAGIC ## PoC Conclusions
# MAGIC
# MAGIC ### Test Results
# MAGIC
# MAGIC | Question | Result | Evidence |
# MAGIC | --- | --- | --- |
# MAGIC | psycopg in UDF sandbox | ✅ Works | Wheel files from UC Volume (`ENVIRONMENT` clause) |
# MAGIC | Network connection UDF → Lakebase | ✅ Works | All 3 approaches successfully wrote to DB |
# MAGIC | WorkspaceClient() without credentials | ❌ Doesn't work | `ValueError: cannot configure default credentials` |
# MAGIC | WorkspaceClient(host=..., token=...) | ✅ Works! | Approach A successfully returned `OK:id=11` |
# MAGIC | Short-term OAuth credentials | ✅ Work | 60min expiration, `generate_database_credential()` |
# MAGIC | Embedded credentials in UDF | ✅ Works | Approach A: credentials embedded in UDF body |
# MAGIC | Approach A (self-auth UDF) | ✅ Works | `OK:id=11` from pure SQL |
# MAGIC | Approach B (credential params) | ✅ Works | `OK:id=4` with parameters |
# MAGIC | Approach C (stored procedure) | ✅ Works | `OK:id=5` from CALL statement |
# MAGIC
# MAGIC ### Architecture That Works
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │  SQL client (DBSQL, notebook, BI tool)                               │
# MAGIC │  SELECT log_to_lakebase_v2('INFO','app','msg') -- Approach A         │
# MAGIC │  CALL log_event('INFO','app','msg')            -- Approach C         │
# MAGIC └─────────────────────────────────┴───────────────────────────────────┘
# MAGIC                                 │
# MAGIC                     Python UDF (LANGUAGE PYTHON)
# MAGIC                     + ENVIRONMENT (psycopg from UC Volume)
# MAGIC                     + embedded credentials (Approach A)
# MAGIC                     or credential parameters (Approach B)
# MAGIC                                 │
# MAGIC                     psycopg.connect() → Lakebase
# MAGIC                     INSERT INTO public.app_log
# MAGIC                                 │
# MAGIC                     ┌──────────────────────────┐
# MAGIC                     │  Lakebase instance        │
# MAGIC                     │  public.app_log           │
# MAGIC                     └──────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key Findings
# MAGIC
# MAGIC **WorkspaceClient works in UDF sandbox!** 
# MAGIC - `WorkspaceClient()` without parameters → ❌ cannot autodiscover credentials
# MAGIC - `WorkspaceClient(host=..., token=...)` with explicit credentials → ✅ **WORKS**
# MAGIC - Credentials can be embedded directly in UDF body at creation time
# MAGIC - UDF can generate short-term PostgreSQL credentials via SDK
# MAGIC
# MAGIC ### Requirements for Production Deployment
# MAGIC
# MAGIC **Approach A (self-auth UDF):**
# MAGIC 1. **Service Principal** credentials (not user token) with minimal permissions
# MAGIC 2. **Wheel files** on UC Volume (`/Volumes/catalog/schema/volume/psycopg*.whl`)
# MAGIC 3. **Refresh mechanism** for token before expiration (or use longer-lived credentials)
# MAGIC
# MAGIC **Approach C (stored procedure):**
# MAGIC 1. **Native Postgres role** in Lakebase with stable password
# MAGIC 2. **Databricks Secret Scope** to store PG password
# MAGIC 3. Stored procedure reads password from secrets
# MAGIC
# MAGIC ### Limitations
# MAGIC - `WorkspaceClient()` without explicit credentials doesn't work in UDF sandbox
# MAGIC - OAuth token expires in 60 min → embedded credentials have limited lifetime
# MAGIC - Ideal solution: Service Principal with long-lived credentials or native PG role
# MAGIC
# MAGIC ### Answers to Original Questions
# MAGIC - **Is this feasible in Databricks?** ✅ YES, all 3 approaches work!
# MAGIC - **Can we use psycopg?** ✅ YES, via UC Volume wheels
# MAGIC - **Does WorkspaceClient work in UDF?** ✅ YES, with explicit credentials!
# MAGIC - **Where to store secrets?** In UDF body (Approach A) or stored procedure (Approach C)
# MAGIC - **Pure SQL without installation?** ✅ YES, works perfectly!