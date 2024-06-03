-- Databricks notebook source
-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC # Getting started with Delta Lake
-- MAGIC
-- MAGIC ## Cluster Set-up / Changes (Required)
-- MAGIC This exercise requires a cluster with access to Unity Catalog and certain whitelisted functions which are only available on clusters with Access Mode **Single User**. To set up the right cluster for this exercise, follow the instructions based on your scenario.
-- MAGIC
-- MAGIC ### I already have a cluster
-- MAGIC 1. [Edit your cluster's configuration](https://github.com/data-derp/documentation/blob/master/databricks/edit-cluster.md)
-- MAGIC 2. Set the **Access Mode** to **Single User** and select your username
-- MAGIC
-- MAGIC ### I don't have a cluster
-- MAGIC 1. [Create a cluster](https://github.com/data-derp/documentation/blob/master/databricks/setup-cluster.md)
-- MAGIC 2. Set the **Access Mode** to **Single User** and select your username
-- MAGIC
-- MAGIC ### Change the Cluster Version to 13.3
-- MAGIC 1. Make sure that DBR Version is **13.3** while editing / creating your cluster above

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # So what is Delta Lake ?
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" style="width:200px; float: right"/>
-- MAGIC
-- MAGIC [Delta Lake](https://delta.io/) is an open storage format used to save your data in your Lakehouse. Delta provides an abstraction layer on top of files. It's the storage foundation of your Lakehouse.
-- MAGIC
-- MAGIC In this notebook, we will explore Delta Lake main capabilities, from table creation to time travel.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fdata-engineering%2Fdelta-lake%2F01-Getting-Started-With-Delta-Lake&cid=3246944839473348&uid=773523978734231">

-- COMMAND ----------

-- DBTITLE 1,Init the demo data under ${raw_data_location}/user_parquet.
-- MAGIC %run ./_resources/00-setup $reset_all_data=true

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC # Check the data
-- MAGIC
-- MAGIC You can execute the commands in CMD 6 to see the location of the data created as a result of CMD 4 execution above.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC new_raw_data_location = f"{raw_data_location}/user_parquet"
-- MAGIC print(f"Our user dataset is stored under raw_data_location={new_raw_data_location}")
-- MAGIC dbutils.widgets.text("raw_data_location", new_raw_data_location)

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC # Check the data in Databricks UI as well
-- MAGIC
-- MAGIC To check the data in the Databricks UI, created as a result of executing the commands in CMD 4 above, go to `Catalog` section of this notebook, click on the `Browse DBFS` button and go to `/Users/YOUR_USER/delta-lake-walkthrough/demo/retail_dbdemos/delta/` location. You should be able to see 2 folders `user_json` and `user_parquet` with parts files and `_SUCCESS` flag

-- COMMAND ----------

-- MAGIC %md ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Creating our first Delta Lake table
-- MAGIC
-- MAGIC Delta is the default file and table format using Databricks. You are likely already using delta without knowing it! (If you have created a table in any of the exercise before !)
-- MAGIC
-- MAGIC Let's create a few tables to see how to use Delta:

-- COMMAND ----------

-- DBTITLE 1,Create Delta table using SQL
-- Creating our first Delta table
CREATE TABLE IF NOT EXISTS user_delta (id BIGINT, creation_date TIMESTAMP, firstname STRING, lastname STRING, email STRING, address STRING, gender INT, age_group INT);

-- Let's load some data in this table
COPY INTO user_delta FROM '${raw_data_location}' FILEFORMAT = parquet;

SELECT * FROM user_delta;

-- COMMAND ----------

-- MAGIC %md-sandbox 
-- MAGIC
-- MAGIC # Check the table in dbdemos
-- MAGIC
-- MAGIC One last check before you run all the other cells of the notebook. Go to `Catalog > Catalog Explorer > dbdemos`. You should see a database created with the name `retail_YOUR_USERNAME`. Click on that database and you should be able to see the `user_delta table`. This is table has got created as a result of running CMD 8 above.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC That's it! Our Delta table is ready and you get all the Delta Benefits. 
-- MAGIC
-- MAGIC Using Delta is that simple!
-- MAGIC
-- MAGIC Let's see how we can use Python or scala API to do the same:

-- COMMAND ----------

-- DBTITLE 1,Create Delta table using python / Scala API (Since the data is already written in the "user_delta" table above, there will be no updates to the table)
-- MAGIC %python
-- MAGIC data_parquet = spark.read.parquet(raw_data_location+"/user_parquet")
-- MAGIC
-- MAGIC data_parquet.write.mode("overwrite").save(raw_data_location+"/user_delta")
-- MAGIC
-- MAGIC display(spark.read.load(raw_data_location+"/user_delta"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Upgrading an existing Parquet or Iceberg table to Delta Lake
-- MAGIC It's also very simple to migrate an existing Parquet or Iceberg table to Delta Lake. Here is an example: 
-- MAGIC
-- MAGIC ```
-- MAGIC CONVERT TO DELTA database_name.table_name; -- only for Parquet tables
-- MAGIC
-- MAGIC CONVERT TO DELTA parquet.`s3://my-bucket/path/to/table`
-- MAGIC   PARTITIONED BY (date DATE); -- if the table is partitioned
-- MAGIC
-- MAGIC CONVERT TO DELTA iceberg.`s3://my-bucket/path/to/table`; -- uses Iceberg manifest for metadata
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake for Batch and Streaming operations
-- MAGIC
-- MAGIC Delta makes it super easy to work with data stream. 
-- MAGIC
-- MAGIC In this example, we'll create a streaming query on top of our table, and add data in the table. The stream will pick the changes without any issue.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read the the data from the user_delta table in a datastream (using spark.readStream) and create a temporary view
-- MAGIC spark.readStream.option("ignoreDeletes", "true").option("ignoreChanges", "true").table("user_delta").createOrReplaceTempView("user_delta_readStream")

-- COMMAND ----------

-- MAGIC %md Now the below select queries are actually the streaming queries on the "user_delta" table via the "view" just created above.

-- COMMAND ----------

select gender, round(avg(age_group),2) from user_delta_readStream group by gender

-- COMMAND ----------

select count(*) from user_delta_readStream

-- COMMAND ----------

-- MAGIC %md **Wait** until the stream is up and running before executing the code below

-- COMMAND ----------

-- DBTITLE 1,Let's add a new user, it'll appear in our previous aggregation as the stream picks up the update
-- A few seconds after running the below query, check the output from CMD 14 and 16. The outputs should reflect the changes for the new user

insert into user_delta (id, creation_date, firstname, lastname, email, address, gender, age_group) 
  values (99999, now(), 'joe', 'smith', 'joe.smith@abc.com', 'IN', '2', 3)     


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support
-- MAGIC
-- MAGIC Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO, providing developers more controls to manage their big datasets.

-- COMMAND ----------

-- Running `UPDATE` on the Delta Lake table
UPDATE user_delta SET age_group = 5 WHERE id = 99999

-- COMMAND ----------

-- MAGIC %md Running the batch query below for the above udpate will immediately give the updated result. In a few more seconds, the updates will reflect in the in the aggregate streaming queries in CMD 14 and 15 too. 
-- MAGIC
-- MAGIC **NOTE** - The update query increases the number or records in the views created from the delta lake tables. This becomes handy for maintaining versions of data as per time travel section below. So you will observe additional records in the out of aggregate streaming queries in CMD 16 and 17 too. It will be interesting to check this out Databricks UI in the `Catalog > dbdemos > retail_<YOUR USER> > user_delta` history section!
-- MAGIC

-- COMMAND ----------

select * from user_delta where id=99999


-- COMMAND ----------

DELETE FROM user_delta WHERE id = 99999

-- COMMAND ----------

-- MAGIC %md DELETE will delete the record so just like UPDATE, if we run a batch query now, it will not give any output. But the aggregate streaming queries in CMD 14 and 15 will get affected to maintain versioning.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,UPSERT: update if exists, insert otherwise
-- Let's create a table containing a list of changes we want to apply to the user table
create table if not exists user_updates 
  (id bigint, creation_date TIMESTAMP, firstname string, lastname string, email string, address string, gender int, age_group int);
  
delete from user_updates;  

insert into user_updates values (1,     now(), 'Marco',   'polo',   'marco@polo.com',    'US', 2, 3); 
insert into user_updates values (2,     now(), 'John',    'Doe',    'john@doe.com',      'US', 2, 3);
insert into user_updates values (99999, now(), 'joe', 'smith', 'joe.smith@abc.com', 'IN', 2, 3)  ;
select * from user_updates;

-- COMMAND ----------

-- We can now MERGE the changes into our main table (note: we could also DELETE the rows based on a predicate)
MERGE INTO user_delta as d USING user_updates as m
  ON d.id = m.id
  WHEN MATCHED THEN 
    UPDATE SET *
  WHEN NOT MATCHED 
    THEN INSERT * ;
  
select * from user_delta where id in (1 ,2, 99999)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Let us quickly check a batch query now
-- MAGIC
-- MAGIC  A normal batch query will also nicely work on your `user_delta` table

-- COMMAND ----------

select * from user_delta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Enforce Data Quality with constraint
-- MAGIC
-- MAGIC Delta Lake support constraints. You can add any expression to force your table having a given field respecting this constraint. As example, let's make sure that the ID is never null.
-- MAGIC
-- MAGIC *Note: This is enforcing quality at the table level. Delta Live Tables offer much more advance quality rules and expectations in data Pipelines.*

-- COMMAND ----------

ALTER TABLE user_delta ADD CONSTRAINT id_not_null CHECK (id is not null);

-- COMMAND ----------

-- This command will fail as we insert a user with a null id::
INSERT INTO user_delta (id, creation_date, firstname, lastname, email, address, gender, age_group) 
                VALUES (null, now(), 'sample', 'user', 'sample@user.com', 'FR', '2', 3) 

-- COMMAND ----------

-- MAGIC %md ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Let's Travel back in Time!
-- MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
-- MAGIC
-- MAGIC * Audit Data Changes
-- MAGIC * Reproduce experiments & reports
-- MAGIC * Rollbacks
-- MAGIC
-- MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
-- MAGIC
-- MAGIC You can query a table by:
-- MAGIC 1. Using a timestamp
-- MAGIC 1. Using a version number
-- MAGIC
-- MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

-- COMMAND ----------

-- MAGIC %md ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Review Delta Lake Table History
-- MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

-- COMMAND ----------

DESCRIBE HISTORY user_delta

-- COMMAND ----------

-- MAGIC %md ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Time Travel via Version Number or Timestamp
-- MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

-- COMMAND ----------

-- Note that in our current version, user 99999 exists and we updated user 1 and 2
SELECT * FROM user_delta WHERE ID IN (1 ,2, 99999);

-- COMMAND ----------

-- We can request the table at version 2, before the upsert operation to get the original data:
SELECT * FROM user_delta VERSION AS OF 2 WHERE ID IN (1 ,2, 99999);

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Restore a Previous Version
-- MAGIC You can restore a Delta table to its earlier state by using the `RESTORE` command, using a timestamp or delta version:
-- MAGIC
-- MAGIC ⚠️ Databricks Runtime 7.4 and above

-- COMMAND ----------

RESTORE TABLE user_delta TO VERSION AS OF 2;

SELECT * FROM user_delta WHERE ID IN (1 ,2, 99999);

-- COMMAND ----------

-- DBTITLE 1,Purging the history with VACUUM
-- We can easily delete all modification older than 200 hours:
VACUUM user_delta RETAIN 200 HOURS;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Generated columns
-- MAGIC
-- MAGIC Delta Lake makes it easy to add auto increment columns. This is done with the `GENERATED` keyword. Generated value can also be derivated from other fields.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_delta_generated_id (
  id BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 10000 INCREMENT BY 1 ), 
  firstname STRING, 
  lastname STRING, 
  email STRING, 
  address STRING) ;

-- Note that we don't insert data for the id. The engine will handle that for us:
INSERT INTO user_delta_generated_id (firstname, lastname, email, address) SELECT
    firstname,
    lastname,
    email,
    address
  FROM user_delta;

-- The ID is automatically generated!
SELECT * from user_delta_generated_id;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC stop_all_streams()

-- COMMAND ----------

-- MAGIC %md ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Going further with Delta Lake
-- MAGIC
-- MAGIC ### Schema evolution 
-- MAGIC
-- MAGIC Delta Lake support schema evolution. You can add a new column and even more advanced operation such as [updating partition](https://docs.databricks.com/delta/delta-batch.html#change-column-type-or-name). For SQL MERGE operation you easily add new columns with `SET spark.databricks.delta.schema.autoMerge.enabled = true`
-- MAGIC
-- MAGIC More details from the [documentation](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-alter-table.html). 
-- MAGIC
-- MAGIC ### Identity columns, PK & FK 
-- MAGIC
-- MAGIC You can add auto-increment columns in your tables: `id BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 0 INCREMENT BY 1 )`, but also define Primary Keys and Foreign Keys.  
-- MAGIC
-- MAGIC For more details, check our demo `dbdemos.install('identity-pk-fk')` !
-- MAGIC
-- MAGIC ### Delta Sharing, open data sharing protocol
-- MAGIC
-- MAGIC [Delta Sharing](https://delta.io/sharing/) is an open standard to easily share your tables with external organization, using Databricks or any other system / cloud provider.
-- MAGIC
-- MAGIC For more details, check our demo `dbdemos.install('delta-sharing')` !
