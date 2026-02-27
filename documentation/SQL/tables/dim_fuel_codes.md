# Table Specification: public.dim_fuel_codes

## 1. Overview

### Purpose
Stores metadata about fuel types and codes.  
Acts as the authoritative reference for fuel classification, descriptions, and categories.

### Grain
One row per unique fuel code.

### Primary Key
- `fuel_code`

### Source
- Maintained manually or via ETL mapping
- Populated with standard fuel types and their business classifications

---

## 2. Columns

| Column Name | Data Type | Nullable | Default | Description | Example |
|-------------|------------|----------|---------|------------|---------|
| fuel_code | VARCHAR(10) | No | – | Unique fuel code identifier | E10 |
| fuel_type | VARCHAR(10) | No | – | Broad fuel type | Ethanol |
| premium_regular | VARCHAR(10) | No | – | Classification as premium or regular | Regular |
| description | VARCHAR(100) | No | – | Human-readable description of the fuel | Ethanol 10% blend |

---

## 3. Constraints

### Primary Key
- `fuel_codes_pkey` → (`fuel_code`)

No foreign key constraints enforced.

---

## 4. Indexes

Primary key index on:
- `fuel_code`

Optional additional indexes may be added for:
- `fuel_type` or `premium_regular` for reporting or filtering

---

## 5. Relationships

Logical flow:

- Referenced by fact table `fact_fuel_prices` via `fuelcode`
- Ensures consistent categorization in analytics and reporting

---

## 6. Data Lifecycle

### Insert Method
- Added via ETL or manual update for new fuel types

### Update Method
- Updated for changes in description or classification

### Delete Policy
- Rarely deleted; historical codes may be retained for consistency

---

## 7. Design Decisions

### Simple Reference Table
Keeps fuel type metadata separate from transactional pricing facts.

### Not Nullable Columns
Ensures all key attributes are always present for reporting and joins.

### Primary Key (`fuel_code`)
Enforces uniqueness of fuel codes across the warehouse.

### No Foreign Keys
Decouples reference table maintenance from fact table load operations.
