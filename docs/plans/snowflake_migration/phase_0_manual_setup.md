# Phase 0: Manual Setup

**Scope:** Tasks 16, 18, plus key-pair generation and initial token seeding.
**Prerequisite for:** All other phases. Nothing can be coded or tested until Snowflake infrastructure exists.

---

## 0.1 — Create Snowflake Account & Run Setup SQL (Task 16)

### Steps

1. **Create Snowflake account** (if not already done) via [signup.snowflake.com](https://signup.snowflake.com).
   - Choose cloud provider/region closest to Dagster Cloud (AWS `us-east-1` recommended for latency).
   - Note the `account` identifier (e.g., `xy12345.us-east-1`).

2. **Run setup SQL** in the Snowflake web console (ACCOUNTADMIN role):

```sql
-- Database and schemas
CREATE DATABASE IF NOT EXISTS OURA;
CREATE SCHEMA IF NOT EXISTS OURA.OURA_RAW;
CREATE SCHEMA IF NOT EXISTS OURA.OURA_STAGING;
CREATE SCHEMA IF NOT EXISTS OURA.OURA_MARTS;
CREATE SCHEMA IF NOT EXISTS OURA.CONFIG;

-- Warehouse (XSMALL, auto-suspend after 60s)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Role
CREATE ROLE IF NOT EXISTS TRANSFORM;

-- Grants: warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Grants: database
GRANT USAGE ON DATABASE OURA TO ROLE TRANSFORM;

-- Grants: oura_raw (pipeline writes raw data)
GRANT USAGE ON SCHEMA OURA.OURA_RAW TO ROLE TRANSFORM;
GRANT CREATE TABLE ON SCHEMA OURA.OURA_RAW TO ROLE TRANSFORM;
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA OURA.OURA_RAW TO ROLE TRANSFORM;
GRANT SELECT, INSERT, DELETE ON FUTURE TABLES IN SCHEMA OURA.OURA_RAW TO ROLE TRANSFORM;

-- Grants: oura_staging + oura_marts (dbt full access)
GRANT ALL ON SCHEMA OURA.OURA_STAGING TO ROLE TRANSFORM;
GRANT ALL ON SCHEMA OURA.OURA_MARTS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA OURA.OURA_STAGING TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA OURA.OURA_MARTS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA OURA.OURA_STAGING TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA OURA.OURA_MARTS TO ROLE TRANSFORM;

-- Grants: config (OAuth token read/write)
GRANT USAGE ON SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
GRANT CREATE TABLE ON SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA OURA.CONFIG TO ROLE TRANSFORM;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA OURA.CONFIG TO ROLE TRANSFORM;

-- Token storage table
CREATE TABLE IF NOT EXISTS OURA.CONFIG.OAUTH_TOKENS (
    token_data VARIANT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Test database (for pytest integration tests)
CREATE DATABASE IF NOT EXISTS OURA_TEST;
CREATE SCHEMA IF NOT EXISTS OURA_TEST.OURA_RAW;
GRANT USAGE ON DATABASE OURA_TEST TO ROLE TRANSFORM;
GRANT ALL ON SCHEMA OURA_TEST.OURA_RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA OURA_TEST.OURA_RAW TO ROLE TRANSFORM;
```

### Verification

```sql
-- Confirm schemas exist
SHOW SCHEMAS IN DATABASE OURA;
-- Expect: CONFIG, OURA_RAW, OURA_STAGING, OURA_MARTS, INFORMATION_SCHEMA, PUBLIC

-- Confirm token table exists
SELECT * FROM OURA.CONFIG.OAUTH_TOKENS;
-- Expect: empty result set, no error
```

---

## 0.2 — Generate Key Pair

### Steps

1. **Generate RSA private key** (locally, never commit):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt
```

2. **Extract public key:**

```bash
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```

3. **Create Snowflake user and assign public key** (ACCOUNTADMIN role):

```sql
CREATE USER IF NOT EXISTS OURA_PIPELINE
    DEFAULT_ROLE = TRANSFORM
    DEFAULT_WAREHOUSE = COMPUTE_WH;

-- Strip the -----BEGIN PUBLIC KEY----- / -----END PUBLIC KEY----- lines
-- and paste the base64 content as a single line:
ALTER USER OURA_PIPELINE SET RSA_PUBLIC_KEY = '<paste-single-line-base64>';

GRANT ROLE TRANSFORM TO USER OURA_PIPELINE;
```

4. **Base64-encode the private key** for storage as an env var:

```bash
base64 -i snowflake_key.p8 | tr -d '\n'
```

This output becomes the value of `SNOWFLAKE_PRIVATE_KEY`.

### Verification

```bash
# Test connection locally (requires snowflake-connector-python installed)
python -c "
import snowflake.connector, base64
from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption

pem = base64.b64decode('$(base64 -i snowflake_key.p8 | tr -d '\n')')
pk = load_pem_private_key(pem, password=None)
der = pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())

con = snowflake.connector.connect(
    account='JCJFPFB-ZRB64423',
    user='OURA_PIPELINE',
    private_key=der,
    warehouse='COMPUTE_WH',
    database='OURA',
    role='TRANSFORM',
)
print('Connected:', con.get_query_status(con.execute_string('SELECT CURRENT_ROLE()')[0].sfqid))
con.close()
"
```

### Security Notes

- **Never** commit `snowflake_key.p8` to git. Add it to `.gitignore` if generated in the repo root.
- After uploading to Dagster Cloud and GitHub Actions secrets, consider deleting the local file or storing it in a password manager.

---

## 0.3 — Seed Initial OAuth Tokens

### Steps

1. **Copy current token JSON** from local `data/tokens/oura_tokens.json`.

2. **Insert into Snowflake** (via web console or SnowSQL):

```sql
USE DATABASE OURA;
USE SCHEMA CONFIG;

INSERT INTO OAUTH_TOKENS (token_data)
SELECT PARSE_JSON($$
{
  "access_token": "CQIHTVTE7USP7LCY5FI2SHEIQMHGAJLM",
  "refresh_token": "RV2BNASTD2FYHBUTMT2PSREVL67Z46XU",
  "token_type": "Bearer",
  "expires_in": 86400,
  "obtained_at": 1773452177
}
$$);
```

### Verification

```sql
SELECT token_data:access_token::varchar IS NOT NULL AS has_access_token,
       token_data:refresh_token::varchar IS NOT NULL AS has_refresh_token,
       updated_at
FROM OURA.CONFIG.OAUTH_TOKENS
ORDER BY updated_at DESC
LIMIT 1;
-- Expect: TRUE, TRUE, recent timestamp
```

---

## 0.4 — Configure Dagster Cloud Environment Variables

### Steps

Add these in the Dagster Cloud UI (Deployment Settings → Environment Variables):

| Variable                | Example Value          | Notes                   |
| ----------------------- | ---------------------- | ----------------------- |
| `SNOWFLAKE_ACCOUNT`     | `xy12345.us-east-1`    | Full account identifier |
| `SNOWFLAKE_USER`        | `OURA_PIPELINE`        | Uppercase by convention |
| `SNOWFLAKE_PRIVATE_KEY` | `<base64-encoded-PEM>` | Output from step 0.2    |
| `SNOWFLAKE_WAREHOUSE`   | `COMPUTE_WH`           |                         |
| `SNOWFLAKE_DATABASE`    | `OURA`                 |                         |
| `SNOWFLAKE_ROLE`        | `TRANSFORM`            |                         |
| `OURA_CLIENT_ID`        | _(existing)_           | Already configured      |
| `OURA_CLIENT_SECRET`    | _(existing)_           | Already configured      |

**Remove after migration is stable:**

- `DUCKDB_PATH`
- `OURA_TOKEN_PATH`

---

## 0.5 — Configure GitHub Actions Secrets (Task 18)

### Steps

Add these in GitHub repo Settings → Secrets and variables → Actions:

| Secret Name             | Purpose                                          |
| ----------------------- | ------------------------------------------------ |
| `SNOWFLAKE_ACCOUNT`     | CI `dbt parse` needs a live Snowflake connection |
| `SNOWFLAKE_USER`        |                                                  |
| `SNOWFLAKE_PRIVATE_KEY` |                                                  |
| `SNOWFLAKE_WAREHOUSE`   |                                                  |
| `SNOWFLAKE_DATABASE`    |                                                  |
| `SNOWFLAKE_ROLE`        |                                                  |

### Workflow Updates

Both `.github/workflows/deploy.yml` and `.github/workflows/branch_deployments.yml` need the env vars passed to the dbt parse step. This will be implemented in Phase 1 alongside the code changes, but the secrets must exist first.

---

## Checklist

- [ ] Snowflake account created, setup SQL executed
- [ ] Key pair generated, public key assigned to `OURA_PIPELINE` user
- [ ] Local connection test passes
- [ ] OAuth tokens seeded into `CONFIG.OAUTH_TOKENS`
- [ ] Dagster Cloud env vars configured
- [ ] GitHub Actions secrets added
- [ ] `snowflake_key.p8` secured (not in repo, stored safely)
