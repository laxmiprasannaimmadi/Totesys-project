# Northcoder Data Engineering Group Project

## Contributors - Team Reveries

<p align="center">
 <a href="https://github.com/cammcburney">Cameron McBurney</a> | <a href="https://github.com/shellybelly77">Ann Shelly</a> | <a href="https://github.com/lUKEdOWNEY">Luke Downey</a> | <a href="https://github.com/Willow-Bot151">Willow Hart</a> | <a href="https://github.com/laxmiprasannaimmadi">Laxmiprasanna Immadi</a>
 </p>

 ## Project Overview

 This project involves transforming OLTP data into an OLAP format to optimise it for querying. AWS infrastructure was combined with python scripts used to ingest, process and store the data to achieve this task. A breakdown of the elements of the project are listed below:

 - CI/CD (continuous integration/continuous deployment) are used to update the project with pushes to main (test listed in yml to prevent       deployment) ,this is managed through GitHub Actions.

 - AWS infrastructure is setup with Terraform which can be seen in the terraform folder.

 - AWS Eventbridge invokes the ingestion lambda on a schedule of 10 minutes to gather new data into an s3 bucket.

 - The timestamp updating in the ingestion bucket triggers the processing lambda to run and convert a copy of the data into dataframes and store
  them in parquet format in a second s3 bucket.

 - The timestamp updating in the processing bucket triggers the warehouse lambda to extract the parquet files from the processing bucket and
 convert them back into dataframes to be inserted as rows into the relevant database tables.

 - AWS Lambdas are monitored with Cloudwatch for errors which link to an email alert.


## Set-up

To setup this project fully you would need valid AWS credentials and a connection to PQSL databases which 
follows the schema given in original_specification.md.

Without those you can still run make requirements, make dev-setup and make run-checks locally:

1. Run the following command to set up your virtual environment and install required dependencies:

```
make requirements
```

2. Run this command next to set up security and coverage modules:

```
make dev-setup
```

3. Set up your `PYTHONPATH`:

```
export PYTHONPATH=$(pwd)
```

4. Run checks for unit-tests, pep8 compliancy, coverage and security vulnerabilities:

```
make run-checks
```

5. Run Terraform to set up the AWS infrastructure:

```
cd terraform
terraform init
terraform plan
terraform apply
```
