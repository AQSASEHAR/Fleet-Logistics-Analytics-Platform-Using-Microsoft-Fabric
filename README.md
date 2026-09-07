# Fleet Logistics Analytics Platform Using Microsoft Fabric

## Overview

This project is an end-to-end fleet logistics data engineering and analytics solution built using Microsoft Fabric.

The solution processes operational fleet data through a Medallion architecture:

```text
Source CSV Files
       ↓
Landing Layer
       ↓
Bronze Layer
       ↓
Silver Layer
       ↓
Gold Star Schema
       ↓
Power BI Semantic Model
       ↓
Power BI Dashboards
```

The project transforms raw logistics data into clean, validated, business-ready datasets that support fleet performance, operational efficiency, safety, maintenance, and financial analysis.

---

## Business Problem

Fleet logistics companies generate large amounts of data from:

- Loads
- Trips
- Drivers
- Trucks
- Trailers
- Customers
- Routes
- Facilities
- Fuel purchases
- Maintenance activities
- Delivery events
- Safety incidents

Without a structured data platform, this information can be difficult to validate, combine, and analyze.

This project provides a centralized analytics solution that helps answer questions such as:

- How much revenue is generated?
- How many loads and trips are completed?
- How many miles does the fleet operate?
- What is the overall fuel efficiency?
- Which trucks have the highest maintenance costs?
- Which maintenance types create the most downtime?
- Which incident types occur most frequently?
- Which incidents generate the highest claims?
- How much vehicle and cargo damage is recorded?
- Which operational areas require management attention?

---

## Project Objectives

The main objectives of this project are to:

1. Ingest raw logistics CSV files into Microsoft Fabric.
2. Store source data in a Landing layer.
3. Build validated Bronze Delta tables.
4. Apply data cleaning and enrichment in the Silver layer.
5. Create a Gold dimensional star schema.
6. Implement data quality and referential integrity checks.
7. Use Delta Lake `MERGE` operations for reliable ingestion.
8. Create Power BI measures and dashboards.
9. Validate business KPIs against Gold-layer results.
10. Demonstrate an end-to-end production-style data engineering workflow.

---

## Technology Stack

- Microsoft Fabric
- Fabric Lakehouse
- Fabric Notebooks
- PySpark
- Spark SQL
- Delta Lake
- Data Pipelines
- Power BI
- GitHub

---

## Architecture

The project follows the Medallion architecture.

### Landing Layer

The Landing layer stores the original source CSV files without applying business transformations.

### Bronze Layer

The Bronze layer contains ingested and normalized Delta tables.

Main activities include:

- Explicit schema definition
- Column name standardization
- Data type conversion
- Date parsing
- Numeric conversion
- Null validation
- Duplicate validation
- Numeric validation
- Date validation
- Referential integrity checks
- Ingestion metadata
- Delta Lake `MERGE`

### Silver Layer

The Silver layer contains cleaned, standardized, deduplicated, and enriched data.

Main activities include:

- Data cleansing
- Standardized data types
- Null handling
- Deduplication
- Business-rule validation
- Derived attributes
- Referential integrity validation
- Metadata preservation

### Gold Layer

The Gold layer contains business-ready dimension and fact tables designed as a star schema for Power BI.

The Gold layer separates:

- Descriptive attributes into dimension tables
- Operational events and measurements into fact tables

### Power BI Layer

Power BI connects to the Gold tables and provides interactive dashboards for:

- Logistics overview
- Fleet operations and efficiency
- Safety and maintenance

---

## Source Datasets

The project uses a Logistics Operations Database containing the following datasets:

- Customers
- Drivers
- Trucks
- Trailers
- Facilities
- Routes
- Loads
- Trips
- Fuel Purchases
- Maintenance
- Delivery Events
- Safety Incidents

The original CSV files were uploaded to the Fabric Lakehouse Landing layer.

---

## Data Volume

The final Silver and Gold layers contain the following record counts:

| Dataset | Record Count |
|---|---:|
| Customers | 200 |
| Drivers | 150 |
| Trucks | 120 |
| Trailers | 180 |
| Facilities | 50 |
| Routes | 58 |
| Loads | 85,410 |
| Trips | 85,410 |
| Fuel Purchases | 196,442 |
| Maintenance | 2,920 |
| Delivery Events | 170,820 |
| Safety Incidents | 170 |
| Date Dimension | 4,724 |

---

# Data Engineering Implementation

## 1. Landing Layer

The Landing layer preserves the original source files.

### Purpose

- Preserve raw source data
- Support traceability
- Enable repeatable processing
- Separate source files from processed tables
- Provide a reliable starting point for Bronze ingestion

The Landing layer contains the original CSV files for all twelve source datasets.

---

## 2. Bronze Layer

The Bronze layer ingests the Landing files into Delta tables.

### Bronze Tables

```text
bronze_customers
bronze_drivers
bronze_trucks
bronze_trailers
bronze_facilities
bronze_routes
bronze_loads
bronze_trips
bronze_fuel_purchases
bronze_maintenance
bronze_delivery_events
bronze_safety_incidents
```

### Bronze Processing

The Bronze notebooks perform the following operations:

- Define explicit schemas
- Standardize column names
- Convert data types
- Parse date columns
- Convert numeric columns
- Validate required identifiers
- Check duplicate business keys
- Check invalid numeric values
- Check invalid dates
- Check foreign key relationships
- Add ingestion metadata
- Write data using Delta Lake `MERGE`

### Bronze Metadata Columns

The Bronze tables include metadata columns such as:

```text
ingestion_timestamp
source_file
```

### Delta Lake MERGE

Delta Lake `MERGE` operations were used to support reliable upsert processing.

The general pattern is:

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "bronze_table")

(
    delta_table.alias("target")
    .merge(
        source_df.alias("source"),
        "target.business_id = source.business_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

This approach helps prevent duplicate records and allows existing records to be updated when source data changes.

---

## 3. Bronze Data Quality Checks

Data quality checks were performed before writing data into the Bronze tables.

### Null Checks

Required identifiers and important business fields were checked for null values.

Examples include:

- Customer IDs
- Driver IDs
- Truck IDs
- Load IDs
- Trip IDs
- Maintenance IDs
- Incident IDs

### Duplicate Checks

Business keys were checked for duplicate records.

Examples include:

- Driver ID
- Truck ID
- Trailer ID
- Load ID
- Trip ID
- Fuel Purchase ID
- Maintenance ID
- Delivery Event ID
- Safety Incident ID

### Numeric Validation

The following types of invalid values were checked:

- Negative mileage
- Negative fuel quantities
- Negative costs
- Invalid MPG
- Invalid duration
- Invalid tank capacity
- Invalid weight
- Invalid piece counts
- Invalid downtime values

### Date Validation

Date fields were checked for invalid or inconsistent values.

Examples include:

- Date of birth
- Hire date
- Termination date
- Load date
- Dispatch date
- Purchase date
- Maintenance date
- Incident date
- Scheduled delivery date
- Actual delivery date

### Referential Integrity Validation

Relationships between source datasets were validated.

Examples include:

- Loads to customers
- Loads to routes
- Trips to loads
- Trips to drivers
- Trips to trucks
- Trips to trailers
- Fuel purchases to trips
- Fuel purchases to trucks
- Fuel purchases to drivers
- Maintenance to trucks
- Delivery events to loads
- Delivery events to trips
- Delivery events to facilities
- Safety incidents to trips
- Safety incidents to trucks
- Safety incidents to drivers

Some trip records contain missing driver, truck, or trailer assignments in the source data. These records were retained because missing assignments are valid source conditions. Non-null invalid foreign keys were checked separately.

---

# Silver Layer

The Silver layer reads from the Bronze tables and produces cleaned and enriched datasets.

## Silver Tables

```text
silver_customers
silver_drivers
silver_trucks
silver_trailers
silver_facilities
silver_routes
silver_loads
silver_trips
silver_fuel_purchases
silver_maintenance
silver_delivery_events
silver_safety_incidents
```

## Silver Transformations

The Silver notebook performs:

- Column standardization
- Data type conversion
- Date conversion
- Numeric conversion
- Null handling
- Deduplication
- Business-rule validation
- Referential integrity checks
- Derived attributes
- Metadata preservation

### Derived Customer Attribute

The Silver customer table includes:

```text
customer_tenure_years
```

This attribute is derived from the customer contract start date.

### Derived Driver Attribute

The Silver driver table includes:

```text
driver_age_years
```

This attribute is derived from the driver's date of birth.

The Silver layer contains the following validated record counts:

| Silver Table | Records |
|---|---:|
| `silver_customers` | 200 |
| `silver_drivers` | 150 |
| `silver_trucks` | 120 |
| `silver_trailers` | 180 |
| `silver_facilities` | 50 |
| `silver_routes` | 58 |
| `silver_loads` | 85,410 |
| `silver_trips` | 85,410 |
| `silver_fuel_purchases` | 196,442 |
| `silver_maintenance` | 2,920 |
| `silver_delivery_events` | 170,820 |
| `silver_safety_incidents` | 170 |

---

# Gold Layer

The Gold layer is designed as a star schema for Power BI.

The model contains:

- Seven dimension tables
- Six fact tables

---

## Gold Dimension Tables

### `gold_dim_date`

**Grain:** One row per calendar date.

Columns include:

```text
date_key
full_date
year
quarter
month
month_name
week_of_year
day_of_month
day_of_week
day_name
is_weekend
```

### `gold_dim_customer`

**Grain:** One row per customer.

Columns include:

```text
customer_key
customer_id
customer_name
customer_type
credit_terms_days
primary_freight_type
account_status
contract_start_date
customer_tenure_years
```

### `gold_dim_driver`

**Grain:** One row per driver.

Columns include:

```text
driver_key
driver_id
first_name
last_name
hire_date
termination_date
license_number
license_state
date_of_birth
home_terminal
employment_status
cdl_class
years_experience
driver_age_years
```

### `gold_dim_truck`

**Grain:** One row per truck.

Columns include:

```text
truck_key
truck_id
unit_number
make
model_year
vin
acquisition_date
acquisition_mileage
fuel_type
tank_capacity_gallons
status
home_terminal
```

### `gold_dim_trailer`

**Grain:** One row per trailer.

Columns include:

```text
trailer_key
trailer_id
trailer_number
trailer_type
length_feet
model_year
vin
acquisition_date
status
current_location
```

### `gold_dim_facility`

**Grain:** One row per facility.

Columns include:

```text
facility_key
facility_id
facility_name
facility_type
city
state
latitude
longitude
dock_doors
operating_hours
```

### `gold_dim_route`

**Grain:** One row per route.

Columns include:

```text
route_key
route_id
origin_city
origin_state
destination_city
destination_state
typical_distance_miles
base_rate_per_mile
fuel_surcharge_rate
typical_transit_days
```

---

## Gold Fact Tables

### `gold_fact_load`

**Grain:** One row per load.

Columns include:

```text
load_id
date_key
customer_key
route_key
load_date
load_type
weight_lbs
pieces
revenue
fuel_surcharge
accessorial_charges
load_status
booking_type
```

### `gold_fact_trip`

**Grain:** One row per trip.

Columns include:

```text
trip_id
load_id
date_key
driver_key
truck_key
trailer_key
dispatch_date
actual_distance_miles
actual_duration_hours
fuel_gallons_used
average_mpg
idle_time_hours
trip_status
```

Missing driver, truck, or trailer assignments are retained as null dimension keys where the source does not provide an assignment.

### `gold_fact_fuel_purchase`

**Grain:** One row per fuel purchase.

Columns include:

```text
fuel_purchase_id
trip_id
date_key
truck_key
driver_key
purchase_date
location_city
location_state
gallons
price_per_gallon
total_cost
fuel_card_number
```

### `gold_fact_maintenance`

**Grain:** One row per maintenance record.

Columns include:

```text
maintenance_id
date_key
truck_key
truck_id
maintenance_date
maintenance_type
odometer_reading
labor_hours
labor_cost
parts_cost
total_cost
facility_location
downtime_hours
service_description
```

### `gold_fact_delivery_event`

**Grain:** One row per delivery event.

Columns include:

```text
event_id
load_id
trip_id
date_key
facility_key
event_type
scheduled_datetime
actual_datetime
detention_minutes
on_time_flag
location_city
location_state
```

### `gold_fact_safety_incident`

**Grain:** One row per safety incident.

Columns include:

```text
incident_id
trip_id
date_key
driver_key
truck_key
incident_date
incident_type
location_city
location_state
at_fault_flag
injury_flag
vehicle_damage_cost
cargo_damage_cost
claim_amount
preventable_flag
description
```

---

# Power BI Data Model

The Gold tables were connected in Power BI using one-to-many relationships.

Typical relationships include:

```text
gold_dim_date[date_key]
        1
        |
        *
gold_fact_load[date_key]
```

```text
gold_dim_date[date_key]
        1
        |
        *
gold_fact_trip[date_key]
```

```text
gold_dim_date[date_key]
        1
        |
        *
gold_fact_safety_incident[date_key]
```

```text
gold_dim_date[date_key]
        1
        |
        *
gold_fact_maintenance[date_key]
```

```text
gold_dim_customer[customer_key]
        1
        |
        *
gold_fact_load[customer_key]
```

```text
gold_dim_route[route_key]
        1
        |
        *
gold_fact_load[route_key]
```

```text
gold_dim_driver[driver_key]
        1
        |
        *
gold_fact_trip[driver_key]
```

```text
gold_dim_driver[driver_key]
        1
        |
        *
gold_fact_safety_incident[driver_key]
```

```text
gold_dim_truck[truck_key]
        1
        |
        *
gold_fact_trip[truck_key]
```

```text
gold_dim_truck[truck_key]
        1
        |
        *
gold_fact_maintenance[truck_key]
```

The relationships use:

- One-to-many cardinality
- Active relationships
- Single-direction filtering from dimensions to facts

---

# Power BI Measures

## Business Measures

```DAX
Total Revenue =
SUM(gold_fact_load[revenue])
```

```DAX
Total Fuel Surcharge =
SUM(gold_fact_load[fuel_surcharge])
```

```DAX
Total Accessorial Charges =
SUM(gold_fact_load[accessorial_charges])
```

```DAX
Load Count =
DISTINCTCOUNT(gold_fact_load[load_id])
```

```DAX
Trip Count =
DISTINCTCOUNT(gold_fact_trip[trip_id])
```

## Fleet Efficiency Measures

```DAX
Total Distance Miles =
SUM(gold_fact_trip[actual_distance_miles])
```

```DAX
Total Fuel Used =
SUM(gold_fact_trip[fuel_gallons_used])
```

```DAX
Total Fuel Cost =
SUM(gold_fact_fuel_purchase[total_cost])
```

```DAX
Revenue per Load =
DIVIDE(
    [Total Revenue],
    [Load Count]
)
```

```DAX
Miles per Trip =
DIVIDE(
    [Total Distance Miles],
    [Trip Count]
)
```

```DAX
Overall MPG =
DIVIDE(
    [Total Distance Miles],
    [Total Fuel Used]
)
```

```DAX
Fuel Cost per Mile =
DIVIDE(
    [Total Fuel Cost],
    [Total Distance Miles]
)
```

```DAX
Maintenance Cost per Trip =
DIVIDE(
    [Total Maintenance Cost],
    [Trip Count]
)
```

## Safety and Maintenance Measures

```DAX
Safety Incident Count =
DISTINCTCOUNT(gold_fact_safety_incident[incident_id])
```

```DAX
Total Claims =
SUM(gold_fact_safety_incident[claim_amount])
```

```DAX
Vehicle Damage Cost =
SUM(gold_fact_safety_incident[vehicle_damage_cost])
```

```DAX
Cargo Damage Cost =
SUM(gold_fact_safety_incident[cargo_damage_cost])
```

```DAX
Total Maintenance Cost =
SUM(gold_fact_maintenance[total_cost])
```

```DAX
Maintenance Downtime =
SUM(gold_fact_maintenance[downtime_hours])
```

---

# Power BI Dashboards

## Dashboard 1 — Executive Overview

### Purpose

The first dashboard provides an executive overview of business and logistics activity.

### Main KPIs

- Total Revenue
- Load Count
- Trip Count
- Total Distance Miles
- Total Fuel Cost
- Safety Incident Count

### Main Visuals

- Revenue by month
- Total revenue by customer
- Load count by load type

  <img width="1439" height="800" alt="image" src="https://github.com/user-attachments/assets/7141f2e1-15b8-406d-b30a-b43d040ca17f" />


This dashboard provides a high-level view of transportation activity and financial performance.

---

## Dashboard 2 — Fleet Operations & Efficiency

### Purpose

The second dashboard evaluates fleet utilization, fuel efficiency, and operating performance.

### Main KPIs and Visuals

- Total Fuel Used
- Overall MPG
- Fuel Cost per Mile
- Total Maintenance Cost
- Maintenance Downtime
- Monthly fuel consumption
- Bottom 10 trucks by MPG
- Top 10 trucks by distance
- Top 10 trucks by maintenance cost
- Fleet performance detail table

<img width="1422" height="802" alt="image" src="https://github.com/user-attachments/assets/ab1f08f4-dd64-43f9-b02c-1794a2071efd" />

This dashboard helps identify inefficient trucks, fuel consumption patterns, and maintenance-heavy assets.

---

## Dashboard 3 — Safety & Maintenance

### Purpose

The third dashboard identifies safety risks, claims, damage costs, maintenance costs, and downtime.

### Main Visuals

- Safety Incidents by Type
- Injury vs Non-Injury Incidents
- Preventable vs Non-Preventable Incidents
- Claims by Incident Type
- Damage Cost by Incident Type
- Total Maintenance Cost by Type
- Safety Detail table
- Maintenance Detail table

This dashboard helps identify:

- The most frequent incident categories
- Preventable safety incidents
- Injury-related incidents
- Incident types with high claim costs
- Vehicle damage costs
- Cargo damage costs
- Maintenance categories with the highest costs
- Maintenance downtime by type

<img width="1427" height="789" alt="image" src="https://github.com/user-attachments/assets/76ac16a4-9dab-4571-8f8b-21777ba3d711" />

---

# Validation Results

The Gold layer was validated against expected record counts and aggregate KPI results.

## Gold Record Count Validation

| Gold Table | Expected Count | Actual Count | Status |
|---|---:|---:|---|
| `gold_dim_date` | 4,724 | 4,724 | Passed |
| `gold_dim_customer` | 200 | 200 | Passed |
| `gold_dim_driver` | 150 | 150 | Passed |
| `gold_dim_truck` | 120 | 120 | Passed |
| `gold_dim_trailer` | 180 | 180 | Passed |
| `gold_dim_facility` | 50 | 50 | Passed |
| `gold_dim_route` | 58 | 58 | Passed |
| `gold_fact_load` | 85,410 | 85,410 | Passed |
| `gold_fact_trip` | 85,410 | 85,410 | Passed |
| `gold_fact_fuel_purchase` | 196,442 | 196,442 | Passed |
| `gold_fact_maintenance` | 2,920 | 2,920 | Passed |
| `gold_fact_delivery_event` | 170,820 | 170,820 | Passed |
| `gold_fact_safety_incident` | 170 | 170 | Passed |

## Referential Integrity Validation

All non-null Gold foreign key checks passed.

Validated relationships included:

- Load to customer
- Load to route
- Load to date
- Trip to driver
- Trip to truck
- Trip to trailer
- Trip to date
- Fuel purchase to driver
- Fuel purchase to truck
- Fuel purchase to date
- Maintenance to truck
- Maintenance to date
- Delivery event to facility
- Delivery event to date
- Safety incident to driver
- Safety incident to truck
- Safety incident to date

All invalid non-null foreign key counts were:

```text
0
```

---

# KPI Validation Results

| KPI | Validated Result |
|---|---:|
| Total Revenue | 262,525,800.29 |
| Total Fuel Surcharge | 29,976,528.65 |
| Total Accessorial Charges | 6,119,100.00 |
| Total Load Weight | 2,346,852,874 lbs |
| Total Pieces | 1,235,928 |
| Total Distance | 122,159,201 miles |
| Total Fuel Used | 18,946,280.10 gallons |
| Total Trip Duration | 2,136,503.30 hours |
| Total Idle Time | 598,791.00 hours |
| Purchased Fuel | 24,519,037.80 gallons |
| Total Fuel Purchase Cost | 95,592,992.04 |
| Total Maintenance Cost | 5,730,573.28 |
| Maintenance Downtime | 72,230.50 hours |
| Safety Incidents | 170 |
| Total Claims | 2,653,171.82 |
| Vehicle Damage Cost | 1,603,561.11 |
| Cargo Damage Cost | 1,049,610.71 |

---

# Important Data Interpretation Notes

## Trip Fuel and Purchased Fuel

The following measures represent different business events:

- `Total Fuel Used` represents fuel consumed during trips.
- Purchased fuel represents fuel purchase transactions.

These values are not expected to match exactly because they are calculated from different operational events.

## Missing Driver, Truck, and Trailer Assignments

Some trips do not contain driver, truck, or trailer assignments in the source data.

These records were retained rather than deleted.

The data quality process checks non-null invalid foreign keys while allowing legitimate missing assignments.

## Maintenance Cost per Trip

The measure:

```DAX
Maintenance Cost per Trip =
DIVIDE(
    [Total Maintenance Cost],
    [Trip Count]
)
```

is a fleet-level ratio.

It should not be interpreted as the average cost of one individual maintenance event.

## Customer Revenue Potential

The `annual_revenue_potential` field is a customer dimension attribute.

It represents potential customer business value and is not the same as actual transportation revenue.

## Fact-to-Fact Relationships

The Power BI model primarily uses dimensions to filter fact tables.

Fact tables should not be directly connected to one another unless there is a clear business requirement and carefully controlled relationship design.

---

# Key Business Insights

The validated Gold data provides the following insights:

- The platform processes **85,410 loads**.
- The platform processes **85,410 trips**.
- Total transportation revenue is approximately **$262.53 million**.
- The fleet operates approximately **122.16 million miles**.
- Overall trip fuel efficiency is approximately **6.45 MPG**.
- Total fuel purchase cost is approximately **$95.59 million**.
- Total maintenance cost is approximately **$5.73 million**.
- Maintenance downtime totals approximately **72,230.5 hours**.
- The dataset contains **170 safety incidents**.
- Total safety claims are approximately **$2.65 million**.
- Vehicle damage cost is approximately **$1.60 million**.
- Cargo damage cost is approximately **$1.05 million**.

---

# Project Challenges and Solutions

## Challenge 1: Inconsistent Source Data Types

Some source columns were stored as strings even though they represented dates, numbers, or identifiers.

### Solution

Explicit schemas and PySpark type conversions were applied during Bronze and Silver processing.

---

## Challenge 2: Missing Foreign Key Assignments

Some trip records did not contain driver, truck, or trailer assignments.

### Solution

Missing assignments were retained as null values, while invalid non-null foreign keys were checked separately.

---

## Challenge 3: Duplicate and Incremental Data

Repeated ingestion could create duplicate records.

### Solution

Delta Lake `MERGE` was used with business keys to support reliable upsert processing.

---

## Challenge 4: Data Quality and Referential Integrity

Operational datasets contain relationships between customers, loads, trips, drivers, trucks, and other entities.

### Solution

Referential integrity checks were applied before data was promoted to the next layer.

---

## Challenge 5: Separating Business Entities from Transactions

The source data contains descriptive attributes and operational measurements.

### Solution

The Gold layer was designed as a star schema:

- Dimensions store descriptive business attributes.
- Facts store operational events and measurements.

---

## Challenge 6: Power BI Aggregation Errors

Using raw numeric columns with automatic Count or Average aggregation can produce misleading results.

### Solution

Explicit DAX measures were created for:

- Revenue
- Claims
- Damage costs
- Maintenance costs
- Fuel consumption
- Operational ratios

---

# Suggested Repository Structure

```text
fleet-logistics-fabric-project/
│
├── README.md
│
├── notebooks/
│   ├── bronze/
│   │   ├── bronze_customers
│   │   ├── bronze_drivers
│   │   ├── bronze_trucks
│   │   ├── bronze_trailers
│   │   ├── bronze_facilities
│   │   ├── bronze_routes
│   │   ├── bronze_loads
│   │   ├── bronze_trips
│   │   ├── bronze_fuel_purchases
│   │   ├── bronze_maintenance
│   │   ├── bronze_delivery_events
│   │   └── bronze_safety_incidents
│   │
│   ├── silver/
│   │   └── silver_transformations
│   │
│   └── gold/
│       └── gold_star_schema
│
├── powerbi/
│   ├── dashboard_1_logistics_overview.png
│   ├── dashboard_2_fleet_efficiency.png
│   └── dashboard_3_safety_maintenance.png
│
├── documentation/
│   ├── architecture.png
│   ├── data_model.png
│   └── validation_results.md
│
└── screenshots/
    ├── landing_layer.png
    ├── bronze_layer.png
    ├── silver_layer.png
    ├── gold_layer.png
    └── powerbi_model.png
```

---

# How to Reproduce the Project

1. Create a Microsoft Fabric workspace.
2. Create a Fabric Lakehouse.
3. Upload the source CSV files into the Landing layer.
4. Run the Bronze ingestion notebooks.
5. Validate Bronze record counts and data quality results.
6. Run the Silver transformation notebook.
7. Validate Silver tables and derived attributes.
8. Run the Gold star-schema notebook.
9. Validate Gold record counts and KPI totals.
10. Connect Power BI to the Gold tables.
11. Create the Power BI semantic model.
12. Create the dimension-to-fact relationships.
13. Create the DAX measures.
14. Build the three dashboards.
15. Validate dashboard totals against Gold-layer results.
16. Publish the report and document the final results.

---

# Final Outcome

This project demonstrates an end-to-end data engineering and analytics platform using Microsoft Fabric.

The solution combines:

- Lakehouse architecture
- Medallion data processing
- PySpark transformations
- Spark SQL
- Delta Lake upserts
- Data quality validation
- Referential integrity checks
- Dimensional modeling
- Power BI semantic modeling
- DAX measures
- Interactive dashboards

The final platform converts raw fleet logistics data into a reliable analytical model that supports business, fleet efficiency, safety, and maintenance decisions.

---

