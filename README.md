# daily_corona_dag

To check whether the api is available or not, in the user interface of airflow, a connection named as  needs to be created. The conn type must be HTTP, and the host must specifiy the api of the data.

There should be a connection named as db_sqlite in the UI of airflow. Which must include conn type as Sqlite and host as the PATH of the airflow.db inside your airflow.

For the sensor operator, there should be a connection specified in the UI with conn_type file(path) and extra {'path':'/Location_of_the_landed_data/'}
