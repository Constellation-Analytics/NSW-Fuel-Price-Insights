# Table Specification: public.dq_issues

## 1. Overview

### Purpose
Stores all detected data quality defects across the system.  
Each row represents one specific defect instance for a specific entity record.

This table acts as:
- A defect log
- A monitoring layer
- A control gate before fact table updates

### Grain
One row per unique defect per entity key combination.

### Primary Key
- `id` (surrogate key)

### Source
- Populated by `check_data_quality()` stored procedure

---

## 2. Columns

| Column Name | Data Type | Nullable | Default | Description | Example |
|-------------|------------|----------|---------|------------|---------|
| id | INTEGER | No | nextval() | Surrogate key | 101 |
| defect_id | TEXT | No | – | Unique defect code (rule identifier) | AD_01 |
| entity | TEXT | No | – | Logical entity where defect occurred | FACT_FUEL |
| key_var | TEXT | No | – | Primary identifying key value | 12345 |
| key_var1 | TEXT | No | – | Secondary identifying key | E10 |
| key_var2 | TEXT | Yes | – | Optional third key for composite grain | 2026-02-27 |
| attribute_name | TEXT | No | – | Column where defect was found | price |
| attribute_value | TEXT | Yes | – | Value that failed validation | -1.23 |
| defect_type | TEXT | No | – | Category of defect | NULL_CHECK |
| defect_description | TEXT | No | – | Human-readable explanation | Price cannot be negative |
| created_at | TIMESTAMP | Yes | now() | When defect record was created | 2026-02-27 10:14:00 |
| is_active | BOOLEAN | Yes | TRUE | Indicates whether defect is currently active | TRUE |

---

## 3. Constraints

### Primary Key
- `dq_issues_pkey` → (`id`)

### Unique Constraint (via index)
- `uq_dq_issues_keys`
  - (`defect_id`, `entity`, `key_var`, `key_var1`, `key_var2`)

Ensures the same defect cannot be logged multiple times for the same entity record.

---

## 4. Indexes

| Index Name | Columns | Purpose |
|------------|----------|----------|
| uq_dq_issues_keys | defect_id, entity, key_var, key_var1, key_var2 | Prevents duplicate defect entries |

---

## 5. Relationships

Logical relationships (no enforced foreign keys):

- Used by `check_data_quality()`
- Referenced by `update_fact_fuel()`
- Acts as a control layer before fact updates

---

## 6. Data Lifecycle

### Insert Method
- Inserted by `check_data_quality()` procedure

### Update Method
- At start of each run, all records are marked `is_active = FALSE`
- Current defects are then reinserted or reactivated

### Delete Policy
- Can be retained historically
- May be subject to retention policy if long-term defect history is not required

---

## 7. Design Decisions

### Surrogate Key (`id`)
Simplifies internal referencing and keeps primary key stable.

### TEXT for key variables
Allows flexibility across multiple entity types and composite business keys.

### Soft Delete (`is_active`)
Supports historical defect tracking and trend analysis.

### No Foreign Keys
Avoids tight coupling and prevents pipeline failures due to referential locking.
Designed as a monitoring/control table rather than a business data table.
