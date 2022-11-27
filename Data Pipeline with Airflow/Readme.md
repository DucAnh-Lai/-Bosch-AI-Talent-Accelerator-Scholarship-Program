## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce 
more automation and monitoring to their data warehouse ETL pipelines and come 
to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high 
grade data pipelines that are dynamic and built from reusable tasks, can be 
monitored, and allow easy backfills. They have also noted that the data quality 
plays a big part when analyses are executed on top the data warehouse and want 
to run tests against their datasets after the ETL steps have been executed to
catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data 
warehouse in Amazon Redshift. The source datasets consist of JSON logs that 
tell about user activity in the application and JSON metadata about the songs 
the users listen to.

## Overview
Build an automated Apache Airflow process that runs hourly.  Utilize custom 
operators to load the staging tables, populate the fact table, populate the 
dimension tables, and perform data quality checks.

Configure the task dependencies so that after the dependencies are set, the
graph view displays ![Workflow DAG](https://github.com/DucAnh-Lai/Bosch-AI-Talent-Accelerator-Scholarship-Program/blob/main/Data%20Pipeline%20with%20Airflow/AE4D7C55-9B0D-4794-8581-0D011BC3CB3A.png)


## Project structure explanation
```
airflow-data-pipeline
│   README.md                    # Project description
│   docker-compose.yml           # Airflow containers description   
│   requirements.txt             # Python dependencies
|   AE4D7C55.png                 # Pipeline DAG image
│   
└───airflow                      # Airflow home
|   |               
│   └───dags                     # Jupyter notebooks
│   |   │ udac_example_dag.py    # DAG definition
|   |   |
|   └───plugins
│       │  
|       └───helpers
|       |   | sql_queries.py     # All sql queries needed
|       |
|       └───operators
|       |   | data_quality.py    # DataQualityOperator
|       |   | load_dimension.py  # LoadDimensionOperator
|       |   | load_fact.py       # LoadFactOperator
|       |   | stage_redshift.py  # StageToRedshiftOperator
```

#### Requirements

* Install [Python3](https://www.python.org/downloads/)
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* [AWS](https://aws.amazon.com/) account and [Redshift](https://aws.amazon.com/redshift/) cluster 

#### Clone repository to local machine
```
git clone https://github.com/DucAnh-Lai/Bosch-AI-Talent-Accelerator-Scholarship-Program/tree/main/Data%20Pipeline%20with%20Airflow
```

#### Change directory to local repository
```
cd airflow-data-pipeline
```

#### Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

#### Start Airflow container
Everything is configured in the docker-compose.yml file.
If you are satisfied with the default configurations you can just start the containers.
```
docker-compose up
```

#### Visit the Airflow UI
Go to http://localhost:8080

Username: user 

Password: password

#### Connect Airflow to AWS

1. Click on the Admin tab and select Connections.
![Admin tab](https://video.udacity-data.com/topher/2019/February/5c5aaca1_admin-connections/admin-connections.png)

2. Under Connections, select Create.

3. On the create connection page, enter the following values:
- Conn Id: Enter aws_credentials.
- Conn Type: Enter Amazon Web Services.
- Login: Enter your Access key ID from the IAM User credentials.
- Password: Enter your Secret access key from the IAM User credentials.
![aws_credentials](https://video.udacity-data.com/topher/2019/February/5c5aaefe_connection-aws-credentials/connection-aws-credentials.png)
Once you've entered these values, select Save and Add Another.

4. On the next create connection page, enter the following values:
- Conn Id: Enter redshift.
- Conn Type: Enter Postgres.
- Host: Enter the endpoint of your Redshift cluster, excluding the port at the end.
- Schema: Enter dev. This is the Redshift database you want to connect to.
- Login: Enter awsuser.
- Password: Enter the password you created when launching your Redshift cluster.
- Port: Enter 5439.
![redshift](https://video.udacity-data.com/topher/2019/February/5c5aaf07_connection-redshift/connection-redshift.png)
Once you've entered these values, select Save.

#### Start the DAG
Start the DAG by switching it state from OFF to ON.

Refresh the page and click on the s3_to_redshift_dag to view the current state.

The whole pipeline should take around 10 minutes to complete.

