# Indicium Tech Code Challenge

Code challenge for Software Developer with a focus on data projects.


## Context

At Indicium we have many projects where we develop the whole data pipeline for our client, spanning from data extraction of diverse data sources, up to loading the extracted data at its final destination, which varies from a data warehouse for a Business Intelligence (BI) tool to an API that integrates with third-party systems or software.

As a software developer with a focus on data projects, your mission is to plan, develop, deploy, and maintain data pipelines.


## The Challenge

Consider the Northwind business, which has most of its data in a single database, a PostgreSQL instance, here is an entity-relation (ER) diagram of the database:

![Northwind ER Diagram](https://user-images.githubusercontent.com/49417424/105997621-9666b980-608a-11eb-86fd-db6b44ece02a.png)

The database has all the company's data, apart from the details of Northwind's orders, which come from a separate e-commerce system. This system outputs all details of the orders daily as a CSV file, this is the only format and frequency the system can operate.

Furthermore, Clyde, a new Northwind data analyst, has shown the CEO some bad-ass dashboards he made using the company database. Since then, the CEO has become very fond of the information from the dashboards, such that now he is interested in seeing a panel which Clyde has determined requires the details of the orders. To this end, the CEO asked the IT team to provision a data warehouse (a secondary PostgreSQL), since he does not wish to undermine the production database with an analytical processing load.

Now Clyde needs you to join the data from the production (source) database along with the CSV file containing the details of the orders that the system outputs. He insisted it is important to do this in two steps, first, extract the data from its source into the local filesystem and then load the data into the data warehouse (destination database).

Clyde is no expert and he is open to new ideas, but he has **lots** of experience working with other data engineers in the past. He knows they mostly use something called Airflow to orchestrate the data pipelines, and they also like to use tools such as Embulk and Meltano for these extraction/loading tasks. Finally, he made this visual schematic to further clear the air if you had any doubts:

![Solution diagram](docs/diagrama_embulk_meltano.jpg)

Clyde also said it would be nice if the extraction files did not get overwritten with new data or deleted every day the piipeline runs. This would ensure an extraction backup in any event and also would help to debug any issues in the future. Finally, he managed to gather some extra links to help you get started:

- [Docker](https://www.docker.com/)
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)
- [Embulk](https://www.embulk.org) (Java Based)
- [Meltano](https://docs.meltano.com/?_gl=1*1nu14zf*_gcl_au*MTg2OTE2NDQ4Mi4xNzA2MDM5OTAz) (Python Based)
- [PostgreSQL](https://www.postgresql.org/docs/15/index.html)

Now it is up to you! You can show Clyde the output of a select query to demonstrate that the data from the details of the orders are in the provisioned data warehouse. 

## Getting started

First, ensure you have [Docker](https://www.docker.com/) installed on your system (alongside the [compose plugin](https://docs.docker.com/compose/install/linux/) - not sure it is installed? run `docker compose version`). Now you may deploy both PostgreSQL instances using:
```shell
docker compose up -d
```

This will deploy two containers representing the Northwind database and data warehouse. You are free to add further services to this specification but are not allowed to modify existing configurations.

Follow the tutorials or devise your own Airflow deployment, but remember to document the steps required to get your solution up and running on other people's hardware (document the constraints you have tested on - Windows, Linux, etc). Although it is advised to follow Clyde's guidelines, you are free to design your own solution to the Northwind issue.

Hint: inspect the docker-compose file to find useful information.

## Trivia

The actual Northwind database is a sample database provided by Microsoft for educational purposes. The actual database differs from the copy provided herein only for the `orders_details` table, which has been extracted as a CSV and provided as an external file.


## Requirements

- You **must** use a combination of the tools described above to complete the challenge.
- All tasks should be idempotent, you should be able to run the pipeline every day and, in this case, where the data is static, the output should be the same.
- Step 2 depends on both Step 1 tasks, therefore Step 2 should not run in case *any of Step 1 do not succeed*.
- You should extract all the tables from the source database, it does not matter that you will not use most of them for the final step.
- You should be able to tell exactly where the pipeline failed, so you know from where to rerun the pipeline.
- You have to provide clear instructions on how to run the whole pipeline. The easier the better.
- You must provide evidence that the process has been completed successfully, i.e. you must provide a CSV or JSON with the result of the query described above.
- You should assume that the pipeline will run for different days, every day.
- Your pipeline should be prepared to run for past days, meaning you should be able to pass an argument to the pipeline with a day from the past, and it should reprocess the data for that day. Since the data for this challenge is static, the only difference for each day of execution will be local file system paths.

### Yes, it matters...

- Clean and organized code.
- Good decisions at which step (which database, which file format..) and good arguments to back those decisions up.
- The aim of the challenge is not only to assess technical knowledge in the field but also the ability to search for information and use it to solve problems with tools that are not necessarily known to the candidate.
- Point-and-click tools are not allowed.


Thank you for participating!