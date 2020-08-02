Sparkify AWS Datawarehouse Project:
====

Summary:
----

The objective of this project is to create a datawarehouse in AWS for analyitics on the use of our service, by using a pipeline orchestrator, Airflow. More specifically, the project will consist on recovering data from multiple data files we have been collecting for some time, which are stored on S3 and inserting them in a Redshift database, which will be created with this purpose. The pipeline orchestrator will allow us to create efficient and maintainable code for the data pipeline and to manage recurrent activities. By having all the data readily available in a columnar storeage relational database we will be able to perform a great variety of queries, very efficiently, to make sense of this data and bring value to our company, by better understanding how our service is being used.

Parts of the project:
---
**DAG: udac_example_dag.py**
It contains the DAG that will be run in Airflow, defining its tasks, arguments and parameters, as well as the dependency relationships between tasks.

**Operators:**
stage_redshift.py: it is used to copy the data from S3 into the staging_events and staging_songs tables.
load_dimension.py: it is used to insert the data from the staging tables into the corresponding final fact and dimension tables. It allow to append or truncate when adding new data.
data_quality.py: it is used to perform some quality checks on the final tables.

**Helpers:**
sql_queries.py: this helper class contains the sql queries as attributes so that they can easily be accessed throughout the project.

Running the code:
---
In a suitable python environment with the corresponding project files run:
1. A shell script to start up Airflow
2. From the Airflow UI, activate the corresponding DAG

Schema Design and ETL Pipeline:
---

The source data is divided in two datasets, stored in S3, called song_data and log_data respectively. The first contains information on the songs, such as the song name, the artist name and the album, and the second one contains data on the events that have taken place within the system (song reproductions) and information on the users which perform these events, such as the users firstname, lastname and gender, the user agent, or the time of the event. The first dataset has been used to build the songs table and the artists table. The second has been used to build the rest of the tables.

Before storing the data in the database, it is loaded on two staging tables, from which the data will then be inserted in the different tables.

**Staging Tables**

These temporary tables should store the raw data, before inserting it in the final schema. This allows us to clean and transform data when performing our INSERT statement from the staging tables to the final ones. For the DISTKEYs we have chosen artist and artist_name from the events and songs staging tables respectively. The reason for this is that they will be joined on these two columns when performing the INSERT into the final schema.

**TABLE staging_events_table:** it contains all the raw data from the **log_data** file in S3, and has the same columns and formats.

**TABLE staging_songs_table:** it contains all the raw data from the **song_data** file in S3, and has the same columns and formats.

**Choice of schema**

The way this data is stored in the database has been thought in order to reduce the duplicated information, thus putting in place a star schema with one fact table (songplays) and four dimension tables (user, song, artist and time). The songplays table contains the data related to the reproduction event in the service, and the rest of the tables complement this information with data which would otherwise be recurring related with the song, the user and the artist, which tend to change less often. Timestamps for the events are stored in the time dimension table. The songplays table is connected to all the rest because it contains all of their keys: *start_time*, *user_id*, *song_id* and *artist_id*. The only other table which has a foreign key is the songs table which has the *artist_id* foreign key connecting it to the artists table.

**Dimension Tables:**

They contain mostly the data which would be recurrently stored in an event table, as they don't usually change from event to event. The column names appear in *italics*.

**TABLE users:** contains information about the users in the app such as *user_id* (which will the primary key for this table), *first_name*, *last_name*, *gender* and *level* (paid or free use of service).

**TABLE songs:** contains information about the songs contained in the music database and include *song_id* (again, the primary key), *title*, *artist_id* (foreign key for the artist table), the *year* it was recorded, and its *duration*.

**TABLE artists:** contains information on the artists in the music database: *artist_id* (primary key), *name* of the artist, *location* of the artist, *longitude* and *latitude* of this location.

**TABLE time:** contains the timestamps of the records in songplays, but broken up by time units: *start_time* (absolute time in ms of the event and primary key), *hour*, *day*. *week* of the year, *month*, *year*, *weekday*.


**Fact Table: songplays**

We have only one fact table called songplays. This table contains information from the log data associated with song plays (only those events marked with *page*=NextSong). The table has the following columns: *songplay_id* (**PK**), *start_time* (**FK**), *user_id* (**FK**), *level*, *song_id* (**FK**), *artist_id* (**FK**), *session_id*, *location* and *user_agent*. The primary key, *songplay_id* is generated when introducing the data in the table using the SERIAL type. 


About the Dataset:
---

The datasets which is used to build as tables are structured as follows:

**song_dataset:**
![song_dataset_sample](/screenshots/song_dataset_sample.png)

**log_dataset:**
![log_dataset_sample](/screenshots/log_dataset_sample.png)

**Dataset Cleaning:**

In the case of Redshift, NOT NULL and PRIMARY KEY are not automatically enforced to clean the database. Instead, when inserting data from the staging tables to the database, we need to include a WHERE statement which allows us to select only non-NULL values to insert them in the final database.
