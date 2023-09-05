{% docs  __overview__ %}
# TastyBytes Data Transformation with dbt

## Welcome to TastyBytes Data Transformation Project

### Project Overview

Welcome to the TastyBytes Data Transformation project! In this initiative, we are leveraging data from TastyBytes, a global food truck operation spanning various countries and cities. TastyBytes is renowned for its cost-effective and environmentally friendly approach to serving truck food under different themes. Our extensive dataset encompasses menus, orders, order details, locations, franchise information, trucks, customers, and much more.

Our primary mission is to construct a robust transformation layer for a dedicated Data Warehouse (DWH) that caters to the unique requirements of TastyBytes. This transformation process will give birth to Data Marts that not only address TastyBytes' reporting needs but also provide valuable insights to drive data-centric decision-making across the organization.

### Project Structure

Let's explore the structure of our project, which ensures clarity and modularity:

- **models/staging/tastybytes**: Within this directory, you'll find subdirectories corresponding to each data domain (e.g., customer, orders). These subdirectories contain the essential `sources.yml` files and staging models.

- **models/dwh/core**: Delve into this directory to discover subdirectories for each data domain. Similar to the staging area, you'll find `sources.yml` files here. However, within each domain, we house fact models (e.g., `customer_facts.sql`) and dimension models (e.g., `customer_dimensions.sql`) optimized for data warehousing.

- **models/dwh/marts**: In this section, you'll encounter mart models (e.g., `customer_mart.sql`) that serve as a direct interface with reporting tools.

- **tests**: Navigate here to access SQL files responsible for executing tests. These tests ensure the quality and consistency of our transformed data.

- **analysis**: This directory contains SQL files that facilitate ad-hoc analysis on our transformed data, enabling deeper insights.

- **documentation**: For comprehensive project documentation, including data dictionaries and schema definitions, look no further than this section. It's your reference guide to understanding the data structure and meaning.

- **reports**: To witness the power of our transformed data in action, visit this area. Sample reports and dashboards showcase how we meet TastyBytes' reporting needs.

- **macros**: Find reusable custom macros in this directory that enhance the efficiency of our dbt models.

- **snapshots**: If snapshot models are required for your project, you'll locate the relevant SQL files here.

### Getting Started

Now, let's embark on your journey with this project:

1. **Prerequisites**: Ensure you have dbt installed and properly configured for your data warehouse environment.

2. **Clone this Repository**: Begin by cloning this project repository to your local machine. It's your gateway to TastyBytes' data transformation.
```bash
   git clone https://github.com/duygugg/Snowflake_Dbt_Workshop.git
```

3. **Configure Connection**: In order to connect to your TastyBytes data source and target data warehouse, make the necessary updates to the dbt project configuration.

4. **Run dbt Models**: The heart of our project lies in the transformation models. To kickstart this process, execute the following dbt command:

```bash
   # Example commands to run dbt models
    dbt run --profiles-dir <path/to/your/profiles.yml> 
```


```bash
    #Example commands to run the elementary package for the first time
    #Which creates a testing, sources, models, alerts etc. metadata views/tables of the specific dbt project

    dbt run --select elementary --vars '{"schema_name":"schema_name","dbt_artifacts_database":"database"}' --profiles-dir <path/to/your/profiles.yml>

```


{% enddocs %}
