# GoodReads Data Pipeline

## Architecture 
![Pipeline Architecture](https://github.com/san089/goodreads_etl_pipeline/blob/master/docs/images/architecture.png)

Pipeline Consists of various modules:

 - [GoodReads Python Wrapper](https://github.com/san089/goodreads)
 - ETL Jobs
 - Redshift Warehouse Module
 - Analytics Module 

#### Overview
Data is captured in real time from the goodreads API using the Goodreads Python wrapper (View usage - [Fetch Data Module](https://github.com/san089/goodreads/blob/master/example/fetchdata.py)). The data collected from the goodreads API is stored on local disk and is timely moved to the Landing Bucket on AWS S3. ETL jobs are written in spark and scheduled in airflow to run every 10 minutes.  


## Environment Setup

### Setting Up Airflow

I have written detailed instruction on how to setup Airflow using AWS CloudFormation script.  Check out - [Airflow using AWS CloudFormation](https://github.com/san089/Data_Engineering_Projects/blob/master/Airflow_Livy_Setup_CloudFormation.md)

**NOTE: This setup uses EC2 instance and a Postgres RDS instance. Make sure to check out charges before running the CloudFromation Stack.** 

Project uses `sshtunnel` to submit spark jobs using a ssh connection from the EC2 instance. This setup does not automatically install `sshtunnel` for apache airflow. You can install by running below command: 

    pip install apache-airflow[sshtunnel]

Finally, copy the dag and plugin folder to EC2 inside airflow home directory. 

### Setting up EMR
Spinning up EMR cluster is preety straight forward. You can use AWS Guide available [here](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html).

ETL jobs in the project uses [psycopg2](https://pypi.org/project/psycopg2/) to connect to Redshift cluster to run staging and warehouse queries. 
To install psycopg2 on EMR:

    sudo pip-3.6 install psycopg2

psycopg2 uses `postgresql-devel` and `postgresql-libs`, and sometimes pscopg2 installation may fail if these dependencies are not available. To install run commands:

    sudo yum install postgresql-libs
    sudo yum install postgresql-devel

ETL jobs also use [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) move files between s3 buckets. To install boto3 run:

    pip-3.6 install boto3 --user

Finally,  pyspark uses python2 as default setup on EMR. To change to python3, setup environment variables:

    export PYSPARK_DRIVER_PYTHON=python3
    export PYSPARK_PYTHON=python3

Copy the ETL scripts to EMR and we have our EMR ready to run jobs. 

### Setting up Redshift
You can follow the AWS [ Guide](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html) to run a Redshift cluster or alternatively you can use [Redshift_Cluster_IaC.py](https://github.com/san089/Data_Engineering_Projects/blob/master/Redshift_Cluster_IaC.py) Script to create cluster automatically. 

