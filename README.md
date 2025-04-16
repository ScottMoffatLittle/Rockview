## ETL pipeline from NetSuite to BigQuery

### Project Architecture
* A Docker container, running etl_prod.py, is hosted on google cloud run
* It is triggerred every two hours by a google cloud scheduler job
* The script sends a post request to netsuite, containing a SuiteQL query to grab data modified in the last 2 days
* This is requested asynchronously in batches of 1000 (the netsuite limit)
* The data is transformed to meet the requirements of the netsuite schema
* This schema is pulled from a google sheets document, which supports the addition of new fields
	- Adding a new field will cause the script to backfill the entire column
* The data is then inserted into a BigQuery staging table
* The staging table is merged into the target table, ignoring duplicate rows
* The staging dataset, and tables within, are dropped

