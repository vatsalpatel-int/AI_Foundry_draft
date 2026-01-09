# Azure AI Foundry Cost Pipeline

A modular, production-ready pipeline for extracting Azure cost data across multiple AI Foundry projects and storing it in Delta Lake for unified analytics.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Schema](#data-schema)
- [Scheduling](#scheduling)
- [Troubleshooting](#troubleshooting)

---

## Overview

### Problem Statement

Organizations using Azure AI Foundry often have multiple AI projects across different subscriptions or resource groups. Tracking costs across these projects requires:

- Logging into Azure Portal multiple times
- Manually exporting data from each project
- Consolidating data in spreadsheets
- No historical trend analysis

### Solution

This pipeline automates the entire process:

1. **Authenticates** with Azure using service principal credentials
2. **Extracts** cost data from multiple Azure scopes (subscriptions, resource groups, management groups)
3. **Transforms** data with metadata for lineage tracking
4. **Loads** into Delta Lake with idempotent MERGE operations
5. **Partitions** by date for efficient querying

> **Note**: This pipeline uses the Azure Cost Management **Query API** which supports all subscription types including **Pay-As-You-Go (PAYG)**, Enterprise Agreement (EA), and Microsoft Customer Agreement (MCA).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              AZURE ORGANIZATION                                  │
│                                                                                  │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│   │ AI Project 1 │  │ AI Project 2 │  │ AI Project 3 │  │ AI Project N │        │
│   │  (OpenAI)    │  │  (ML Studio) │  │ (Cognitive)  │  │   (Other)    │        │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘        │
│          │                 │                 │                 │                 │
│          └─────────────────┴────────┬────────┴─────────────────┘                 │
│                                     │                                            │
│                     ┌───────────────▼───────────────┐                            │
│                     │   Azure Cost Management API   │                            │
│                     │   (Aggregates all costs)      │                            │
│                     └───────────────┬───────────────┘                            │
└─────────────────────────────────────┼────────────────────────────────────────────┘
                                      │
                                      │ REST API (OAuth 2.0)
                                      │
┌─────────────────────────────────────▼────────────────────────────────────────────┐
│                          DATABRICKS ENVIRONMENT                                   │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                         COST PIPELINE                                        │ │
│  │                                                                              │ │
│  │   ┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌──────────────┐      │ │
│  │   │ config   │───▶│  auth    │───▶│    data      │───▶│    delta     │      │ │
│  │   │   .py    │    │   .py    │    │ extractor.py │    │  writer.py   │      │ │
│  │   └──────────┘    └──────────┘    └──────────────┘    └──────────────┘      │ │
│  │        │               │                 │                   │               │ │
│  │   Load .env      OAuth Token       Query API            MERGE to            │ │
│  │   Validate       Management        JSON → CSV           Delta Lake          │ │
│  │                                                                              │ │
│  └──────────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                            │
│                                      ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────┐ │
│  │                           DELTA LAKE (S3/ADLS/DBFS)                          │ │
│  │                                                                               │ │
│  │   ai_foundry_costs/                                                          │ │
│  │   ├── _delta_log/                    (Transaction log)                       │ │
│  │   ├── _cost_date=2026-01-05/         (Partitioned data)                      │ │
│  │   ├── _cost_date=2026-01-06/                                                 │ │
│  │   └── _cost_date=2026-01-07/                                                 │ │
│  │                                                                               │ │
│  └──────────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                            │
└──────────────────────────────────────┼────────────────────────────────────────────┘
                                       │
                                       ▼
                      ┌────────────────────────────────┐
                      │       ANALYTICS LAYER          │
                      │                                │
                      │  ┌──────────┐  ┌──────────┐   │
                      │  │Databricks│  │ Power BI │   │
                      │  │   SQL    │  │ Tableau  │   │
                      │  └──────────┘  └──────────┘   │
                      │                                │
                      └────────────────────────────────┘
```

---

## Data Flow

### Step-by-Step Process

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW DIAGRAM                                   │
└─────────────────────────────────────────────────────────────────────────────────┘

STEP 1: TRIGGER
═══════════════
  • Databricks Job Scheduler (e.g., daily at 6 AM UTC)
  • Manual execution via CLI or notebook
  • Backfill mode for historical data
                    │
                    ▼
STEP 2: CONFIGURATION
═════════════════════
  ┌─────────────────────────────────────────────────────────┐
  │  config.py                                              │
  │  ─────────                                              │
  │  • Load environment variables from .env                 │
  │  • Validate required credentials                        │
  │  • Parse multiple Azure scopes                          │
  │  • Return PipelineConfig object                         │
  └─────────────────────────────────────────────────────────┘
                    │
                    ▼
STEP 3: AUTHENTICATION
══════════════════════
  ┌─────────────────────────────────────────────────────────┐
  │  auth.py                                                │
  │  ───────                                                │
  │  • POST to Azure AD token endpoint                      │
  │  • Use client credentials (service principal)           │
  │  • Cache token with expiry tracking                     │
  │  • Auto-refresh when token expires                      │
  │                                                         │
  │  Token URL: https://login.microsoftonline.com/          │
  │             {tenant}/oauth2/token                       │
  └─────────────────────────────────────────────────────────┘
                    │
                    ▼
STEP 4: QUERY COST DATA (for each scope)
════════════════════════════════════════
  ┌─────────────────────────────────────────────────────────┐
  │  data_extractor.py                                      │
  │  ─────────────────                                      │
  │  • POST to Cost Management Query API                    │
  │  • Request: { type: "Usage", timeframe: "Custom",       │
  │               timePeriod: { from, to },                 │
  │               dataset: { granularity: "Daily" } }       │
  │  • Response: JSON with columns and rows                 │
  │                                                         │
  │  API: https://management.azure.com/{scope}/             │
  │       providers/Microsoft.CostManagement/query          │
  │       ?api-version=2025-03-01                           │
  └─────────────────────────────────────────────────────────┘
                    │
                    ▼
STEP 5: HANDLE PAGINATION
═════════════════════════
  ┌─────────────────────────────────────────────────────────┐
  │  • Check for nextLink in response                       │
  │  • If present, GET next page of results                 │
  │  • Repeat until no more pages                           │
  │  • Combine all rows into single result                  │
  └─────────────────────────────────────────────────────────┘
                    │
                    ▼
STEP 6: CONVERT TO CSV
══════════════════════
  ┌─────────────────────────────────────────────────────────┐
  │  • Extract column names from response                   │
  │  • Convert JSON rows to CSV format                      │
  │  • Store as bytes for compatibility                     │
  └─────────────────────────────────────────────────────────┘
                    │
                    ▼
STEP 7: TRANSFORM & LOAD
════════════════════════
  ┌─────────────────────────────────────────────────────────┐
  │  delta_writer.py                                        │
  │  ───────────────                                        │
  │  • Write CSV to temp file                               │
  │  • Load into Spark DataFrame                            │
  │  • Add metadata columns:                                │
  │    - _source_scope                                      │
  │    - _source_scope_name                                 │
  │    - _ingestion_timestamp                               │
  │    - _cost_date (partition key)                         │
  │  • MERGE into Delta table (upsert)                      │
  │  • Partition by _cost_date                              │
  └─────────────────────────────────────────────────────────┘
                    │
                    ▼
STEP 8: COMPLETE
════════════════
  ┌─────────────────────────────────────────────────────────┐
  │  • Log execution summary                                │
  │  • Report rows written per scope                        │
  │  • Return stats dictionary                              │
  └─────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
AI_Foundry/
│
├── 📄 .env                    # Environment variables (secrets - git ignored)
├── 📄 .gitignore              # Git ignore rules
├── 📄 __init__.py             # Package marker
├── 📄 requirements.txt        # Python dependencies
├── 📄 README.md               # This documentation
│
├── 📄 config.py               # Configuration management
│   │   • Load and validate environment variables
│   │   • Parse multiple Azure scopes
│   │   • Return typed configuration objects
│
├── 📄 auth.py                 # Azure AD authentication
│   │   • OAuth 2.0 client credentials flow
│   │   • Token caching and auto-refresh
│   │   • Authorization header generation
│
├── 📄 data_extractor.py       # Azure Cost Management Query API client
│   │   • Query cost data via REST API
│   │   • Handle pagination (nextLink)
│   │   • Convert JSON response to CSV
│   │   • Support multiple scopes
│
├── 📄 delta_writer.py         # Delta Lake operations
│   │   • CSV to Spark DataFrame conversion
│   │   • Metadata column enrichment
│   │   • Idempotent MERGE operations
│   │   • Date-based partitioning
│   │   • Table optimization (OPTIMIZE, VACUUM)
│
├── 📄 main.py                 # Main orchestrator
│   │   • CLI argument parsing
│   │   • Pipeline coordination
│   │   • Backfill support
│   │   • Execution reporting
│
└── 📁 venv/                   # Virtual environment (git ignored)
```

---

## Installation

### Prerequisites

- Python 3.9+
- Databricks Runtime 12.0+ (for production)
- Azure Service Principal with Cost Management Reader role

### Local Setup

```bash
# Clone or navigate to project directory
cd AI_Foundry

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # macOS/Linux
# or
.\venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

### Databricks Setup

Upload all `.py` files to Databricks workspace or use Repos to sync from Git.

---

## Configuration

### Environment Variables (.env)

Create a `.env` file with the following configuration:

```bash
# ═══════════════════════════════════════════════════════════════════════════
# REQUIRED: Azure AD Authentication (Service Principal)
# ═══════════════════════════════════════════════════════════════════════════

# Your Azure AD tenant ID (Directory ID)
AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# Service Principal Application (Client) ID
AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# Service Principal Client Secret
AZURE_CLIENT_SECRET=your-client-secret-here

# ═══════════════════════════════════════════════════════════════════════════
# REQUIRED: Azure Scopes (comma-separated for multiple projects)
# ═══════════════════════════════════════════════════════════════════════════
#
# Supported formats:
#   • Subscription:       subscriptions/{subscription-id}
#   • Resource Group:     subscriptions/{sub-id}/resourceGroups/{rg-name}
#   • Management Group:   providers/Microsoft.Management/managementGroups/{mg-id}
#
# Examples:
#   Single scope:
#     AZURE_SCOPES=subscriptions/12345678-1234-1234-1234-123456789012
#
#   Multiple scopes:
#     AZURE_SCOPES=subscriptions/sub-1,subscriptions/sub-2,subscriptions/sub-3
#
#   Mixed scope types:
#     AZURE_SCOPES=subscriptions/sub-1,providers/Microsoft.Management/managementGroups/org-mg

AZURE_SCOPES=subscriptions/your-subscription-id

# ═══════════════════════════════════════════════════════════════════════════
# REQUIRED: Delta Lake Storage Path
# ═══════════════════════════════════════════════════════════════════════════
#
# Supported storage:
#   • S3:    s3a://bucket-name/path/to/table
#   • ADLS:  abfss://container@account.dfs.core.windows.net/path/to/table
#   • DBFS:  dbfs:/path/to/table

DELTA_TABLE_PATH=s3a://your-bucket/warehouse/ai_foundry_costs

# ═══════════════════════════════════════════════════════════════════════════
# OPTIONAL: Pipeline Tuning
# ═══════════════════════════════════════════════════════════════════════════

# Legacy settings (kept for backward compatibility, not used by Query API)
POLL_INTERVAL=30
MAX_POLL_ATTEMPTS=60

# API request timeout in seconds (default: 60)
REQUEST_TIMEOUT=60

# Pagination request timeout in seconds (default: 300)
DOWNLOAD_TIMEOUT=300
```

### Azure Service Principal Setup

1. **Create Service Principal** in Azure AD
2. **Assign Role**: `Cost Management Reader` at the appropriate scope
3. **Note down**: Tenant ID, Client ID, Client Secret

```bash
# Azure CLI commands
az ad sp create-for-rbac --name "CostPipelineSP" --role "Cost Management Reader" \
    --scopes /subscriptions/{subscription-id}
```

---

## Usage

### Command Line Interface

```bash
# Activate virtual environment
source venv/bin/activate

# ─────────────────────────────────────────────────────────────────────────
# Run for yesterday (default)
# ─────────────────────────────────────────────────────────────────────────
python main.py

# ─────────────────────────────────────────────────────────────────────────
# Run for a specific date
# ─────────────────────────────────────────────────────────────────────────
python main.py --date 2026-01-05

# ─────────────────────────────────────────────────────────────────────────
# Backfill last N days
# ─────────────────────────────────────────────────────────────────────────
python main.py --days 7      # Last 7 days
python main.py --days 30     # Last 30 days

# ─────────────────────────────────────────────────────────────────────────
# Use APPEND instead of MERGE (faster, but may create duplicates)
# ─────────────────────────────────────────────────────────────────────────
python main.py --no-merge

# ─────────────────────────────────────────────────────────────────────────
# Combine options
# ─────────────────────────────────────────────────────────────────────────
python main.py --days 7 --no-merge
```

### Databricks Notebook

```python
# Cell 1: Import and run for yesterday
from main import run_pipeline, run_backfill

result = run_pipeline()
print(f"Rows written: {result['total_rows']}")

# Cell 2: Run for specific date
result = run_pipeline(target_dates=["2026-01-05"])

# Cell 3: Backfill last 7 days
result = run_backfill(days=7)

# Cell 4: Check Delta table stats
from delta_writer import DeltaLakeWriter
writer = DeltaLakeWriter("s3a://bucket/ai_foundry_costs")
stats = writer.get_table_stats()
print(stats)
```

### Scheduling Options

| Schedule | Command / Config | Use Case |
|----------|-----------------|----------|
| **Daily** | `python main.py` | Standard daily cost tracking |
| **Hourly** | Not recommended | Cost data updates ~daily |
| **Weekly backfill** | `python main.py --days 7` | Catch any missed days |
| **Monthly backfill** | `python main.py --days 30` | Historical analysis |
| **Specific date** | `python main.py --date 2026-01-05` | Re-process specific day |

---

## Data Schema

### Raw Data from Azure Cost Management Query API

The Query API returns JSON data which is converted to CSV format. The columns returned depend on the query configuration. With daily granularity, the response includes:

| Column | Data Type | Description |
|--------|-----------|-------------|
| `PreTaxCost` | NUMBER | Cost amount before tax |
| `UsageDate` | NUMBER | Date in YYYYMMDD format |
| `Currency` | STRING | Currency code (e.g., "USD") |

> **Note**: The Query API returns aggregated data. For detailed line-item data with all columns shown below, consider using the Exports API for scheduled exports to blob storage.

### Full Schema Reference (Exports API / Legacy)

The following columns are available when using detailed cost exports:

| Category | Column Name | Data Type | Description |
|----------|-------------|-----------|-------------|
| **Billing** | `BillingAccountId` | STRING | Billing account identifier |
| | `BillingAccountName` | STRING | Billing account name |
| | `BillingPeriodStartDate` | DATE | Start of billing period |
| | `BillingPeriodEndDate` | DATE | End of billing period |
| | `BillingProfileId` | STRING | Billing profile identifier |
| | `BillingProfileName` | STRING | Billing profile name |
| **Invoice** | `InvoiceSectionId` | STRING | Invoice section identifier |
| | `InvoiceSectionName` | STRING | Invoice section name |
| **Product** | `PartNumber` | STRING | Azure part number |
| | `ProductName` | STRING | Full product name |
| **Meter** | `MeterCategory` | STRING | Service category (e.g., "Azure OpenAI Service") |
| | `MeterSubCategory` | STRING | Sub-category (e.g., "GPT-4 Turbo") |
| | `MeterName` | STRING | Specific meter (e.g., "Input Tokens") |
| | `MeterId` | STRING | Unique meter GUID |
| | `MeterRegion` | STRING | Meter region |
| **Resource** | `ResourceLocation` | STRING | Azure region (e.g., "eastus") |
| | `ResourceGroup` | STRING | Resource group name |
| | `ResourceId` | STRING | Full Azure resource ID |
| | `ResourceName` | STRING | Resource display name |
| **Service** | `ServiceName` | STRING | Azure service name |
| | `ServiceTier` | STRING | Service tier |
| **Subscription** | `SubscriptionId` | STRING | Subscription GUID |
| | `SubscriptionName` | STRING | Subscription display name |
| **Cost Center** | `CostCenter` | STRING | Cost center (from tags) |
| **Usage** | `UnitOfMeasure` | STRING | Unit type (e.g., "1K Tokens") |
| | `Quantity` | DOUBLE | Usage quantity |
| | `EffectivePrice` | DOUBLE | Price per unit |
| | `CostInBillingCurrency` | DOUBLE | **Actual cost amount** |
| | `BillingCurrencyCode` | STRING | Currency code (e.g., "USD") |
| **Pricing** | `PricingModel` | STRING | OnDemand, Reservation, Spot |
| | `ChargeType` | STRING | Usage, Purchase, Refund |
| | `Frequency` | STRING | UsageBased, OneTime, Recurring |
| **Publisher** | `PublisherType` | STRING | Microsoft, Marketplace |
| | `PublisherName` | STRING | Publisher name |
| **Reservation** | `ReservationId` | STRING | Reservation ID (if applicable) |
| | `ReservationName` | STRING | Reservation name |
| **Tags** | `Tags` | STRING | JSON string of resource tags |
| **Date** | `Date` | DATE | Cost date |
| **Benefits** | `benefitId` | STRING | Benefit identifier |
| | `benefitName` | STRING | Benefit name |

### Additional Metadata Columns (Added by Pipeline)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `_source_scope` | STRING | Full Azure scope ID |
| `_source_scope_name` | STRING | Human-readable scope name |
| `_ingestion_timestamp` | TIMESTAMP | When data was loaded |
| `_ingestion_date` | DATE | Date of ingestion |
| `_cost_date` | DATE | **Partition key** - the cost date |

### Azure AI Foundry Specific Meters

| MeterCategory | MeterSubCategory | MeterName | UnitOfMeasure |
|---------------|------------------|-----------|---------------|
| Azure OpenAI Service | GPT-4 | Input Tokens | 1K Tokens |
| Azure OpenAI Service | GPT-4 | Output Tokens | 1K Tokens |
| Azure OpenAI Service | GPT-4 Turbo | Input Tokens | 1K Tokens |
| Azure OpenAI Service | GPT-4 Turbo | Output Tokens | 1K Tokens |
| Azure OpenAI Service | GPT-4o | Input Tokens | 1K Tokens |
| Azure OpenAI Service | GPT-4o | Output Tokens | 1K Tokens |
| Azure OpenAI Service | GPT-4o mini | Input Tokens | 1K Tokens |
| Azure OpenAI Service | GPT-4o mini | Output Tokens | 1K Tokens |
| Azure OpenAI Service | Embeddings Ada | Tokens | 1K Tokens |
| Azure OpenAI Service | DALL-E 3 | Standard Images | 1 Image |
| Azure OpenAI Service | DALL-E 3 | HD Images | 1 Image |
| Azure OpenAI Service | Whisper | Audio | 1 Hour |
| Azure OpenAI Service | Text to Speech | Characters | 1M Characters |
| Azure AI Services | Content Safety | Transactions | 1K Transactions |
| Azure Machine Learning | Compute | Various VM SKUs | 1 Hour |

### Example Record

```json
{
  "Date": "2026-01-06",
  "SubscriptionId": "12345678-1234-1234-1234-123456789012",
  "SubscriptionName": "AI Foundry Production",
  "ResourceGroup": "rg-openai-prod",
  "ResourceName": "my-openai-instance",
  "ResourceId": "/subscriptions/.../providers/Microsoft.CognitiveServices/accounts/my-openai-instance",
  "ServiceName": "Azure OpenAI Service",
  "MeterCategory": "Azure OpenAI Service",
  "MeterSubCategory": "GPT-4 Turbo",
  "MeterName": "Output Tokens",
  "MeterId": "abc12345-def6-7890-abcd-ef1234567890",
  "Quantity": 2500.0,
  "UnitOfMeasure": "1K Tokens",
  "EffectivePrice": 0.06,
  "CostInBillingCurrency": 150.00,
  "BillingCurrencyCode": "USD",
  "ChargeType": "Usage",
  "PricingModel": "OnDemand",
  "Tags": "{\"project\":\"customer-chatbot\",\"environment\":\"production\",\"team\":\"ai-platform\"}",
  
  "_source_scope": "subscriptions/12345678-1234-1234-1234-123456789012",
  "_source_scope_name": "subscription-12345678",
  "_ingestion_timestamp": "2026-01-07T06:00:00.000Z",
  "_ingestion_date": "2026-01-07",
  "_cost_date": "2026-01-06"
}
```

---

## Scheduling

### Databricks Workflows

1. **Create a Job** in Databricks Workflows
2. **Task Type**: Python script or Notebook
3. **Schedule**: Daily at 6:00 AM UTC (costs are finalized overnight)
4. **Cluster**: Use a small single-node cluster

```yaml
# Example job configuration
name: AI Foundry Cost Pipeline
schedule:
  quartz_cron_expression: "0 0 6 * * ?"  # Daily at 6 AM UTC
  timezone_id: "UTC"
tasks:
  - task_key: extract_costs
    python_wheel_task:
      package_name: ai_foundry
      entry_point: main
    cluster_spec:
      spark_version: "13.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 0  # Single node
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `ModuleNotFoundError: requests` | venv not activated | Run `source venv/bin/activate` |
| `401 Unauthorized` | Invalid credentials | Check AZURE_CLIENT_ID and AZURE_CLIENT_SECRET |
| `403 Forbidden` | Missing permissions | Assign "Cost Management Reader" role to SP |
| `400 Bad Request` | Invalid scope or date format | Check scope format, ensure dates are valid |
| `Empty response` | No cost data for date range | Verify costs exist for the specified period |
| `spark not defined` | Not in Databricks | Run in Databricks Runtime environment |

### Debugging

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Test authentication only
from config import load_config
from auth import AzureAuthenticator

config = load_config()
auth = AzureAuthenticator(config.azure)
print(f"Token: {auth.token[:50]}...")  # Print first 50 chars

# Test single scope extraction
from data_extractor import AzureCostExtractor

extractor = AzureCostExtractor(auth)
reports = extractor.extract_costs_for_date(
    scopes=["subscriptions/your-sub-id"],
    target_date="2026-01-06"
)
print(f"Extracted {len(reports)} reports")
```

### Logs Location

- **Databricks**: Driver logs in Spark UI
- **Local**: stdout/stderr or configure file handler

---

## License

Internal use only. Contact your administrator for licensing information.

---

## API Reference

This pipeline uses the **Azure Cost Management Query API**:

- **Endpoint**: `POST https://management.azure.com/{scope}/providers/Microsoft.CostManagement/query?api-version=2025-03-01`
- **Documentation**: [Query - Usage API Reference](https://learn.microsoft.com/en-gb/rest/api/cost-management/query/usage?view=rest-cost-management-2025-03-01)

### Supported Subscription Types

| Subscription Type | Supported |
|-------------------|-----------|
| Pay-As-You-Go (PAYG) | ✅ Yes |
| Enterprise Agreement (EA) | ✅ Yes |
| Microsoft Customer Agreement (MCA) | ✅ Yes |
| CSP (Cloud Solution Provider) | ✅ Yes |

---

## Support

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review [Azure Cost Management Query API documentation](https://learn.microsoft.com/en-gb/rest/api/cost-management/query/usage)
3. Contact your platform team
test
