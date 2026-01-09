# Unity Catalog Connection and Pipeline Configuration Guide

This guide provides comprehensive documentation for creating Unity Catalog connections, configuring DLT pipelines, and deploying Lakeflow Community Connectors. It fills the gaps in existing documentation with detailed, actionable instructions for AI agents implementing connectors.

---

## Table of Contents

1. [Unity Catalog Connection Deep Dive](#1-unity-catalog-connection-deep-dive)
2. [Pipeline Configuration Architecture](#2-pipeline-configuration-architecture)
3. [The ingest.py Pattern](#3-the-ingestpy-pattern)
4. [Option Flow Architecture](#4-option-flow-architecture)
5. [Deployment Workflow](#5-deployment-workflow)
6. [Reference Examples](#6-reference-examples)

---

## 1. Unity Catalog Connection Deep Dive

### 1.1 Connection Type

All Lakeflow Community Connectors use the **`GENERIC_LAKEFLOW_CONNECT`** connection type in Unity Catalog. This is a special Databricks connection type that:

- Stores authentication credentials securely in Unity Catalog
- Provides credential resolution at runtime via `spark.read.format("lakeflow_connect")`
- Supports the `externalOptionsAllowList` security mechanism

### 1.2 Required Connection Options

Every connection must include these options:

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `sourceName` | String | Yes | The connector name (e.g., `"sap_successfactors"`, `"github"`, `"zendesk"`) - must match the directory name in `sources/` |
| `externalOptionsAllowList` | String | Yes | Comma-separated list of table-level options the connector accepts |

### 1.3 Authentication Options

Beyond the required options, each connector defines its own authentication parameters. These are specified in the `connector_spec.yaml` file. Common patterns include:

**API Token Authentication:**
```json
{
  "sourceName": "example_source",
  "api_token": "your-api-token",
  "base_url": "https://api.example.com",
  "externalOptionsAllowList": "custom_option1,custom_option2"
}
```

**OAuth Authentication:**
```json
{
  "sourceName": "example_source",
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "token_url": "https://auth.example.com/token",
  "externalOptionsAllowList": "custom_option1,custom_option2"
}
```

**Basic Authentication:**
```json
{
  "sourceName": "example_source",
  "username": "your-username",
  "password": "your-password",
  "subdomain": "your-subdomain",
  "externalOptionsAllowList": "custom_option1,custom_option2"
}
```

### 1.4 Creating Connections via Databricks CLI

**Basic Connection Creation:**
```bash
# Set your source name
SOURCE_NAME="sap_successfactors"

# Create the connection
databricks connections create \
  --json '{
    "name": "'${SOURCE_NAME}'_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
      "sourceName": "'${SOURCE_NAME}'",
      "api_key": "your-api-key",
      "company_id": "your-company-id",
      "externalOptionsAllowList": "entity_type,custom_fields"
    }
  }'
```

**List Existing Connections:**
```bash
databricks connections list
```

**Get Connection Details:**
```bash
databricks connections get "sap_successfactors_connection"
```

**Delete a Connection:**
```bash
databricks connections delete "sap_successfactors_connection"
```

### 1.5 The externalOptionsAllowList Mechanism

The `externalOptionsAllowList` is a **security mechanism** that controls which options can be passed to the connector at runtime via `table_configuration`.

**How It Works:**

1. When you create a connection, you specify allowed options:
   ```json
   "externalOptionsAllowList": "entity_type,include_deleted,page_size"
   ```

2. In your pipeline spec, you can only use these options in `table_configuration`:
   ```json
   "table_configuration": {
     "entity_type": "User",        // Allowed - in allowlist
     "include_deleted": "true",    // Allowed - in allowlist
     "malicious_option": "value"   // BLOCKED - not in allowlist
   }
   ```

3. The framework automatically adds these constant options to every allowlist:
   - `tableName` - The source table name
   - `tableNameList` - List of tables (for metadata queries)
   - `tableConfigs` - JSON-encoded table configurations
   - `isDeleteFlow` - Boolean flag for delete flow

**Determining Your Allowlist:**

Your connector's `externalOptionsAllowList` should include:
- Any options passed to `read_table()` via `table_options` parameter
- Any options used in `get_table_schema()` via `table_options` parameter
- Any options used in `read_table_metadata()` via `table_options` parameter

### 1.6 Relationship to connector_spec.yaml

The `connector_spec.yaml` file defines:
1. **Connection parameters** - What auth credentials are required
2. **External options allowlist** - What table-level options are allowed

Example `connector_spec.yaml`:
```yaml
source_name: sap_successfactors
display_name: SAP SuccessFactors

connection:
  parameters:
    - name: api_key
      display_name: API Key
      required: true
      sensitive: true
    - name: company_id
      display_name: Company ID
      required: true
      sensitive: false
    - name: api_url
      display_name: API URL
      required: false
      default: "https://api.successfactors.com"

external_options_allowlist: "entity_type,include_inactive,page_size"
```

When the CLI tool creates a connection, it:
1. Validates that required parameters are provided
2. Automatically adds `sourceName` to the options
3. Merges `external_options_allowlist` from spec with constant allowlist

---

## 2. Pipeline Configuration Architecture

### 2.1 The Three Configuration Layers

Lakeflow uses a three-layer configuration system with clear precedence:

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: CONNECTION OPTIONS                                     │
│ Stored in Unity Catalog connection                              │
│ Contains: Auth credentials, sourceName, externalOptionsAllowList│
│ Accessed by: LakeflowConnect.__init__(options)                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2: PIPELINE CONFIGURATION                                 │
│ Set in DLT pipeline "configuration" dict                        │
│ Contains: Global settings like num_tables, batch_size, flags    │
│ Accessed by: spark.conf.get("key", "default")                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 3: TABLE CONFIGURATION                                    │
│ Set per-table in pipeline_spec["objects"][*]["table_configuration"]│
│ Contains: Table-specific options (must be in allowlist)         │
│ Accessed by: read_table(table_name, start_offset, table_options)│
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 How spark.conf.get() Works in DLT

When you create a DLT pipeline with a `configuration` dict, those values become available as Spark configuration:

**Pipeline Creation (setting configuration):**
```json
{
  "name": "my_pipeline",
  "configuration": {
    "num_tables": "5",
    "batch_size": "1000",
    "include_deleted": "false"
  },
  "libraries": [{"file": {"path": "/Workspace/path/ingest.py"}}]
}
```

**In ingest.py (reading configuration):**
```python
# Read pipeline configuration values
num_tables = spark.conf.get("num_tables", "1")       # Returns "5"
batch_size = spark.conf.get("batch_size", "500")     # Returns "1000"
include_deleted = spark.conf.get("include_deleted", "true")  # Returns "false"
debug_mode = spark.conf.get("debug_mode", "false")   # Returns "false" (default)
```

**Key Points:**
- All values are strings (even numbers like `"5"`)
- Always provide a default value as the second argument
- Configuration is read-only during pipeline execution
- Changes to configuration require pipeline update + restart

### 2.3 Using Pipeline Configuration for Dynamic Behavior

Pipeline configuration enables dynamic behavior without code changes:

**Example: Dynamic Table Selection**
```python
# In ingest.py
from sources.sap_successfactors.sap_successfactors import LakeflowConnect

# Read which tables to ingest from pipeline configuration
tables_to_ingest = spark.conf.get("tables", "User,Employee").split(",")

# Or use a flag to ingest all tables
ingest_all = spark.conf.get("ingest_all_tables", "false") == "true"

if ingest_all:
    connector = LakeflowConnect({})
    tables_to_ingest = connector.list_tables()
```

**Example: Configurable Batch Sizes**
```python
# Default batch size from pipeline config
default_batch_size = spark.conf.get("default_batch_size", "1000")

# Generate table configurations with this default
table_config = {"batch_size": default_batch_size}
```

---

## 3. The ingest.py Pattern

### 3.1 Basic Static Pattern

For connectors with a fixed set of tables, use a static pipeline spec:

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "sap_successfactors"
connection_name = "sap_successfactors_connection"

# Static pipeline spec with explicit tables
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": "User",
                "table_configuration": {
                    "include_inactive": "false"
                }
            }
        },
        {
            "table": {
                "source_table": "Employee",
                "table_configuration": {
                    "include_inactive": "false"
                }
            }
        }
    ]
}

# Register the connector and run ingestion
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### 3.2 Dynamic Discovery Pattern

For connectors that support dynamic table discovery, use `list_tables()` with `spark.conf.get()`:

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function
from sources.sap_successfactors.sap_successfactors import LakeflowConnect

source_name = "sap_successfactors"
connection_name = "sap_successfactors_connection"

# Read configuration from DLT pipeline configuration
entity_filter = spark.conf.get("entity_filter", "")  # e.g., "User,Employee,Job"
include_inactive = spark.conf.get("include_inactive", "false")
batch_size = spark.conf.get("batch_size", "1000")

# Create connector instance to discover available tables
# Note: Pass minimal options needed for list_tables()
connector = LakeflowConnect({})
all_tables = connector.list_tables()

# Filter tables if entity_filter is specified
if entity_filter:
    filter_set = set(entity_filter.split(","))
    tables_to_ingest = [t for t in all_tables if t in filter_set]
else:
    tables_to_ingest = all_tables

# Default table configuration applied to all tables
default_table_config = {
    "include_inactive": include_inactive,
    "batch_size": batch_size
}

# Generate pipeline spec for all tables
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": table_name,
                "table_configuration": default_table_config
            }
        }
        for table_name in tables_to_ingest
    ]
}

# Register and run pipeline
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### 3.3 Per-Table Configuration Overrides

To apply different configurations to specific tables:

```python
# Define table-specific overrides
table_overrides = {
    "User": {"batch_size": "5000", "include_inactive": "true"},
    "AuditLog": {"batch_size": "10000"},  # Large table needs bigger batches
    # Other tables use defaults
}

# Default configuration
default_config = {
    "batch_size": spark.conf.get("batch_size", "1000"),
    "include_inactive": spark.conf.get("include_inactive", "false")
}

# Generate spec with overrides
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": table_name,
                "table_configuration": {
                    **default_config,  # Start with defaults
                    **table_overrides.get(table_name, {})  # Apply overrides
                }
            }
        }
        for table_name in tables_to_ingest
    ]
}
```

### 3.4 Destination Table Configuration

You can specify custom destination catalog, schema, and table names:

```python
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": "User",
                "destination_catalog": "main",
                "destination_schema": "sap_successfactors_raw",
                "destination_table": "sf_users",  # Optional, defaults to source_table
                "table_configuration": {}
            }
        }
    ]
}
```

**Default Behavior:**
- If `destination_catalog` and `destination_schema` are not specified, the table is created in the pipeline's default catalog and schema (set in pipeline configuration)
- If `destination_table` is not specified, it uses `source_table` as the name

---

## 4. Option Flow Architecture

### 4.1 Visual Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        CONFIGURATION SOURCES                              │
└──────────────────────────────────────────────────────────────────────────┘
         │                           │                          │
         ▼                           ▼                          ▼
┌─────────────────┐    ┌─────────────────────────┐    ┌─────────────────────┐
│ Unity Catalog   │    │ DLT Pipeline            │    │ Pipeline Spec       │
│ Connection      │    │ Configuration           │    │ (ingest.py)         │
├─────────────────┤    ├─────────────────────────┤    ├─────────────────────┤
│ sourceName      │    │ "configuration": {      │    │ "table_configuration"│
│ api_key         │    │   "batch_size": "1000", │    │ {                   │
│ company_id      │    │   "entity_filter": "..." │    │   "entity": "User"  │
│ externalOptions │    │ }                       │    │ }                   │
│ AllowList       │    │                         │    │                     │
└────────┬────────┘    └───────────┬─────────────┘    └──────────┬──────────┘
         │                         │                              │
         │                         │                              │
         │                         ▼                              │
         │              ┌─────────────────────────┐               │
         │              │ spark.conf.get()        │               │
         │              │ in ingest.py            │               │
         │              └───────────┬─────────────┘               │
         │                         │                              │
         │                         ▼                              │
         │              ┌─────────────────────────┐               │
         │              │ Dynamic pipeline_spec   │◄──────────────┘
         │              │ generation              │
         │              └───────────┬─────────────┘
         │                         │
         │                         ▼
         │              ┌─────────────────────────┐
         │              │ SpecParser              │
         │              │ - connection_name       │
         │              │ - table_list            │
         │              │ - table_configurations  │
         │              └───────────┬─────────────┘
         │                         │
         ▼                         ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ spark.read.format("lakeflow_connect")                                    │
│   .option("databricks.connection", connection_name)  ◄── UC Connection   │
│   .option("tableName", source_table)                                     │
│   .options(**table_configuration)  ◄── Table Config (filtered by allowlist)│
│   .load()                                                                │
└─────────────────────────────────────────────┬────────────────────────────┘
                                              │
                                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ LakeflowConnect                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│ __init__(options)     ◄── All options merged (UC creds + table options)  │
│ list_tables()                                                            │
│ get_table_schema(table_name, table_options)                              │
│ read_table_metadata(table_name, table_options)                           │
│ read_table(table_name, start_offset, table_options) ◄── table_configuration│
└──────────────────────────────────────────────────────────────────────────┘
```

### 4.2 What Each Method Receives

**`__init__(options: dict[str, str])`**

Receives ALL options merged together:
- Connection options from UC connection (api_key, company_id, etc.)
- `sourceName`
- The current table's `table_configuration` options

```python
def __init__(self, options: dict[str, str]) -> None:
    # Extract auth credentials (from UC connection)
    self.api_key = options.get("api_key")
    self.company_id = options.get("company_id")

    # Build API client
    self.client = SAPClient(self.api_key, self.company_id)
```

**`read_table(table_name: str, start_offset: dict, table_options: dict[str, str])`**

The `table_options` parameter contains the `table_configuration` from the pipeline spec:

```python
def read_table(self, table_name: str, start_offset: dict,
               table_options: dict[str, str]) -> tuple:
    # Extract table-specific options
    include_inactive = table_options.get("include_inactive", "false") == "true"
    batch_size = int(table_options.get("batch_size", "1000"))

    # Use options in API calls
    records = self.client.fetch_records(
        entity=table_name,
        include_inactive=include_inactive,
        limit=batch_size,
        offset=start_offset.get("offset", 0)
    )

    return iter(records), {"offset": start_offset.get("offset", 0) + len(records)}
```

### 4.3 Security: The Allowlist Filter

The `externalOptionsAllowList` acts as a security filter:

```
Pipeline Spec table_configuration:
{
    "entity_type": "User",      ─┐
    "batch_size": "1000",        │ These are checked
    "malicious_sql": "DROP..."   │ against allowlist
}                               ─┘
            │
            ▼
┌───────────────────────────────────────┐
│ externalOptionsAllowList:             │
│ "entity_type,batch_size,include_inactive"│
└───────────────────────────────────────┘
            │
            ▼
Allowed options passed to connector:
{
    "entity_type": "User",      ✓ In allowlist
    "batch_size": "1000"        ✓ In allowlist
    // "malicious_sql" BLOCKED  ✗ Not in allowlist
}
```

---

## 5. Deployment Workflow

### 5.1 Required Files and Directory Structure

```
sources/{source_name}/
├── {source_name}.py                    # Main connector implementation
├── _generated_{source_name}_python_source.py  # Generated (do not edit)
├── ingest.py                           # Pipeline entry point
├── README.md                           # User documentation
├── connector_spec.yaml                 # Connection parameter spec
├── configs/
│   └── dev_config.json                 # Dev credentials (gitignored)
└── test/
    └── test_{source_name}_lakeflow_connect.py
```

**Files Required for Deployment:**
1. `libs/source_loader.py`
2. `libs/spec_parser.py`
3. `libs/utils.py`
4. `pipeline/ingestion_pipeline.py`
5. `sources/{source_name}/_generated_{source_name}_python_source.py`
6. `sources/{source_name}/{source_name}.py` (if using dynamic discovery)
7. `sources/{source_name}/ingest.py`

### 5.2 Generating the Merged Source File

Before deployment, run the merge script:

```bash
python tools/scripts/merge_python_source.py {source_name}
```

This creates `sources/{source_name}/_generated_{source_name}_python_source.py` which:
- Combines `libs/utils.py`, `{source_name}.py`, and `pipeline/lakeflow_python_source.py`
- Wraps everything in `register_lakeflow_source(spark)` function
- Handles import deduplication

### 5.3 Using databricks sync for Fast Iteration

For rapid development, use `databricks sync` instead of git:

```bash
# Set variables
SOURCE_NAME="sap_successfactors"
USER_NAME=$(databricks current-user me --output json | jq -r '.userName')
WORKSPACE_PATH="/Workspace/Users/$USER_NAME/${SOURCE_NAME}"

# Create temp directory with required files
TEMP_DIR=$(mktemp -d)
mkdir -p "$TEMP_DIR/libs" "$TEMP_DIR/sources/$SOURCE_NAME" "$TEMP_DIR/pipeline"

# Copy required files
cp libs/source_loader.py libs/spec_parser.py libs/utils.py "$TEMP_DIR/libs/"
cp sources/$SOURCE_NAME/__init__.py "$TEMP_DIR/sources/$SOURCE_NAME/"
cp sources/$SOURCE_NAME/_generated_${SOURCE_NAME}_python_source.py "$TEMP_DIR/sources/$SOURCE_NAME/"
cp sources/$SOURCE_NAME/${SOURCE_NAME}.py "$TEMP_DIR/sources/$SOURCE_NAME/"  # For list_tables()
cp pipeline/ingestion_pipeline.py "$TEMP_DIR/pipeline/"
cp sources/$SOURCE_NAME/ingest.py "$TEMP_DIR/ingest.py"

# Sync to workspace
databricks sync "$TEMP_DIR" "$WORKSPACE_PATH"

# Cleanup
rm -rf "$TEMP_DIR"
echo "Files uploaded to: $WORKSPACE_PATH"
```

### 5.4 Creating the DLT Pipeline

```bash
# Set variables
SOURCE_NAME="sap_successfactors"
USER_NAME=$(databricks current-user me --output json | jq -r '.userName')
PIPELINE_NAME="$(echo $USER_NAME | cut -d'@' -f1 | tr '.' '_')_${SOURCE_NAME}"
WORKSPACE_PATH="/Workspace/Users/$USER_NAME/${SOURCE_NAME}"
CONNECTION_NAME="${SOURCE_NAME}_connection"

# Create schema if needed
databricks schemas create "${PIPELINE_NAME}" main 2>/dev/null || true

# Create the pipeline
databricks pipelines create \
  --json '{
    "name": "'$PIPELINE_NAME'",
    "catalog": "main",
    "schema": "'${PIPELINE_NAME}'",
    "configuration": {
      "source_name": "'$SOURCE_NAME'",
      "entity_filter": "User,Employee",
      "batch_size": "1000"
    },
    "serverless": true,
    "continuous": false,
    "development": true,
    "libraries": [
      {
        "file": {
          "path": "'$WORKSPACE_PATH'/ingest.py"
        }
      }
    ]
  }' | tee /tmp/pipeline_response.json

# Extract pipeline ID
PIPELINE_ID=$(cat /tmp/pipeline_response.json | jq -r '.pipeline_id')
echo "Pipeline ID: $PIPELINE_ID"
```

### 5.5 Running and Monitoring Pipelines

**Start a Pipeline Run:**
```bash
databricks pipelines start-update "$PIPELINE_ID"
```

**Start with Full Refresh (reprocess all data):**
```bash
databricks pipelines start-update "$PIPELINE_ID" --full-refresh
```

**Check Pipeline Status:**
```bash
databricks pipelines get "$PIPELINE_ID" --output json | jq -r '.state'
```

**View Pipeline in UI:**
```bash
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')
echo "View at: $WORKSPACE_URL/pipelines/$PIPELINE_ID"
```

### 5.6 Updating Pipeline Configuration

To change configuration without recreating the pipeline:

```bash
# Get current spec
CURRENT_SPEC=$(databricks pipelines get "$PIPELINE_ID" --output json | jq '.spec')

# Update configuration values
UPDATED_SPEC=$(echo "$CURRENT_SPEC" | jq '
  .configuration.entity_filter = "User,Employee,Job" |
  .configuration.batch_size = "2000"
')

# Apply update
databricks pipelines update "$PIPELINE_ID" --json "$UPDATED_SPEC"

# Restart to apply changes
databricks pipelines start-update "$PIPELINE_ID" --full-refresh
```

---

## 6. Reference Examples

### 6.1 Complete ingest.py with Dynamic Discovery

```python
"""
SAP SuccessFactors Connector - Ingestion Pipeline Entry Point

This script is the entry point for the DLT pipeline. It:
1. Reads configuration from DLT pipeline settings via spark.conf.get()
2. Dynamically discovers available tables via list_tables()
3. Generates a pipeline_spec based on configuration
4. Calls ingest() to create SDP flows for each table
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# Import connector for dynamic discovery
from sources.sap_successfactors.sap_successfactors import LakeflowConnect

# Constants
SOURCE_NAME = "sap_successfactors"
CONNECTION_NAME = "sap_successfactors_connection"

# ─────────────────────────────────────────────────────────────────────────────
# Read Configuration from DLT Pipeline Settings
# ─────────────────────────────────────────────────────────────────────────────

# Which entities to ingest (comma-separated, empty = all)
entity_filter = spark.conf.get("entity_filter", "")

# Default batch size for all tables
default_batch_size = spark.conf.get("batch_size", "1000")

# Whether to include inactive records
include_inactive = spark.conf.get("include_inactive", "false")

# Destination schema (if not set, uses pipeline default)
destination_schema = spark.conf.get("destination_schema", "")

# ─────────────────────────────────────────────────────────────────────────────
# Discover Tables
# ─────────────────────────────────────────────────────────────────────────────

# Create a minimal connector instance for table discovery
# Note: No auth needed for list_tables() in most connectors
connector = LakeflowConnect({})
all_tables = connector.list_tables()

# Apply entity filter if specified
if entity_filter:
    filter_set = set(e.strip() for e in entity_filter.split(","))
    tables_to_ingest = [t for t in all_tables if t in filter_set]
else:
    tables_to_ingest = all_tables

print(f"Ingesting {len(tables_to_ingest)} tables: {tables_to_ingest}")

# ─────────────────────────────────────────────────────────────────────────────
# Configure Per-Table Overrides (Optional)
# ─────────────────────────────────────────────────────────────────────────────

# Define overrides for specific tables that need different settings
table_overrides = {
    "AuditLog": {"batch_size": "5000"},  # Large table
    "User": {"include_inactive": "true"},  # Include terminated users
}

# Default configuration applied to all tables
default_config = {
    "batch_size": default_batch_size,
    "include_inactive": include_inactive,
}

# ─────────────────────────────────────────────────────────────────────────────
# Generate Pipeline Spec
# ─────────────────────────────────────────────────────────────────────────────

def build_table_spec(table_name: str) -> dict:
    """Build table specification with merged configuration."""
    # Merge default config with table-specific overrides
    table_config = {**default_config, **table_overrides.get(table_name, {})}

    spec = {
        "table": {
            "source_table": table_name,
            "table_configuration": table_config
        }
    }

    # Add destination schema if specified
    if destination_schema:
        spec["table"]["destination_catalog"] = "main"
        spec["table"]["destination_schema"] = destination_schema

    return spec

pipeline_spec = {
    "connection_name": CONNECTION_NAME,
    "objects": [build_table_spec(t) for t in tables_to_ingest]
}

# ─────────────────────────────────────────────────────────────────────────────
# Register Connector and Run Ingestion
# ─────────────────────────────────────────────────────────────────────────────

register_lakeflow_source = get_register_function(SOURCE_NAME)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### 6.2 Connection Creation Commands

**SAP SuccessFactors with API Key Auth:**
```bash
databricks connections create \
  --json '{
    "name": "sap_successfactors_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
      "sourceName": "sap_successfactors",
      "api_key": "your-api-key",
      "company_id": "your-company-id",
      "api_url": "https://api.successfactors.com",
      "externalOptionsAllowList": "entity_type,include_inactive,batch_size,start_date"
    }
  }'
```

**SAP SuccessFactors with OAuth:**
```bash
databricks connections create \
  --json '{
    "name": "sap_successfactors_oauth_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
      "sourceName": "sap_successfactors",
      "client_id": "your-client-id",
      "client_secret": "your-client-secret",
      "company_id": "your-company-id",
      "token_url": "https://auth.successfactors.com/oauth/token",
      "api_url": "https://api.successfactors.com",
      "externalOptionsAllowList": "entity_type,include_inactive,batch_size,start_date"
    }
  }'
```

### 6.3 Pipeline Creation JSON

```json
{
  "name": "john_doe_sap_successfactors",
  "catalog": "main",
  "schema": "sap_successfactors_raw",
  "configuration": {
    "source_name": "sap_successfactors",
    "entity_filter": "User,Employee,Job,Position",
    "batch_size": "1000",
    "include_inactive": "false",
    "destination_schema": "sap_successfactors_raw"
  },
  "serverless": true,
  "continuous": false,
  "development": true,
  "libraries": [
    {
      "file": {
        "path": "/Workspace/Users/john.doe@company.com/sap_successfactors/ingest.py"
      }
    }
  ]
}
```

### 6.4 Troubleshooting Common Issues

**Issue: "Connection not found"**
```bash
# Verify connection exists
databricks connections list | grep sap_successfactors

# Check connection name matches in pipeline spec
grep "connection_name" sources/sap_successfactors/ingest.py
```

**Issue: "Option not in allowlist"**
```bash
# Check current allowlist
databricks connections get "sap_successfactors_connection" \
  --output json | jq '.options.externalOptionsAllowList'

# Update allowlist
databricks connections update "sap_successfactors_connection" \
  --json '{
    "options": {
      "externalOptionsAllowList": "entity_type,include_inactive,batch_size,new_option"
    }
  }'
```

**Issue: "Import error: cannot import LakeflowConnect"**
```bash
# Ensure the connector file is uploaded
databricks workspace list "/Workspace/Users/$USER/sap_successfactors/sources/sap_successfactors/"

# Re-run merge and sync
python tools/scripts/merge_python_source.py sap_successfactors
# Then re-run databricks sync
```

**Issue: "Pipeline fails on list_tables()"**
```bash
# Ensure original connector file is uploaded (not just generated)
# The dynamic discovery pattern requires both files:
# - _generated_sap_successfactors_python_source.py (for Spark Data Source)
# - sap_successfactors.py (for list_tables() call in ingest.py)
```

---

## Summary

This guide covered the complete flow from Unity Catalog connection creation through pipeline deployment:

1. **Connections** store credentials and define which table options are allowed
2. **Pipeline configuration** provides global settings accessible via `spark.conf.get()`
3. **Table configuration** provides per-table options passed to `read_table()`
4. **ingest.py** ties everything together by generating the pipeline_spec
5. **Deployment** uses `databricks sync` for fast iteration

The key insight is that there are **three configuration layers**, and understanding how options flow through each layer is essential for building robust connectors.
