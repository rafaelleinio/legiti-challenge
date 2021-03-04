

# Legiti Challenge
_A nice project solution for building and running pipelines for feature store._

![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9-brightgreen.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![aa](https://img.shields.io/badge/code%20quality-flake8-blue)](https://github.com/PyCQA/flake8)
[![pytest-coverage: 100%](https://img.shields.io/badge/pytest--coverage-100%25-green)](https://github.com/pytest-dev/pytest)

[Build Status](https://github.com/rafaelleinio/legiti-challenge/actions):

| Core     | Docker Image 
| -------- | -------- 
| ![Tests](https://github.com/rafaelleinio/legiti-challenge/workflows/Tests/badge.svg?branch=main)     | ![Build Docker Image](https://github.com/rafaelleinio/legiti-challenge/workflows/Build%20Docker%20Image/badge.svg?branch=main)    

## The Challenge and Proposed Arch for Solution
The challenge goal is to show a typical financial problem scenario where a feature store service can be implemented as a handful and scalable solution.:rocket:

#### Background on credit card fraud
Before we dive into the problem itself, it's good to introduce the term “chargeback.”. A chargeback happens when the owner of the credit card that was used in a previously fulfilled order reports a charge as fraudulent to their credit card company. This results in the order being retroactively canceled, and the credit card owner receiving their money back from the bank. However, the bank then returns to the merchant who originally approved the sale and demands that money back from them. When fraud happens, the merchant is the one who ends up having to pay that amount back to the bank. **Avoiding chargebacks while maximizing the approval rate is one of the key goals of this kinda business.**

To be able to train and deploy a machine learning model, we need to generate for each order features that will be used both during training and evaluation. Examples of features might be the **“total of orders and chargebacks associated with a user in the last X days”**.

## Proposed Solution
The problem was addressed by implementing pipelines generating features for a feature store. Before diving into code, let’s remember some feature store modeling concepts.

### Data Model Concepts

* **Entity**: Unit representing a specific business context. It can be identified by some key.
* **Feature Set**: Group of features defined under the same context, calculated over an entity key and a time reference.
* **Feature**: individual metric on an observed event.

![](https://i.imgur.com/H18Juvz.png)
_Image by Rafael Leinio_

#### Feature Store key concepts:
* Central repository and catalog for all feature sets.
* **Historical Feature Store**: all historical data in data-size optimized storage. Main purpose: base for the building of training datasets.
* **Online Feature Store**: hot/latest data stored in low latency data storage. Main purpose: serve features for model APIs predictions.

#### Common arch implementation:
![](https://i.imgur.com/cbDZ9Ga.png)
_Image by Rafael Leinio_

#### Feature store use case flows:
![](https://i.imgur.com/Wg9BRvh.png)
_Image by Rafael Leinio_

### Project Objective
This project will not focus on showing how to deploy cloud infrastructure. But rather, to set a good example on how to structure a great project for defining data pipelines.

One of the first design decisions was to use Butterfree as a base ETL engine for all pipelines. Butterfree is a Spark-based ETL library [developed by QuintoAndar.](https://github.com/quintoandar/butterfree/), one of the Brazilian companies reference in ML and Feature Stores. Main benefits from this technology decision:
- Having the same flexible ETL library for data pipelines helps maintain **great standards throughout all the code base**. :ballot_box_with_check: 
- Butterfree by-design care about **declarative feature engineering**. Feature developers should care about what they want to compute and not how to code it. Great DX!! :nerd_face: 
- Apache Spark is the state-of-the-art big data technology used in modern data platforms nowadays.:airplane:
- Spark can process batch and streams of data equivalently easily and both modes are supported with the same Butterfree's `FeaturSetPipeline` structure. :100:

Feature Set pipelines can be created in 3 easy steps:
* **Source**: declare from what sources to read the data and how to merge these sources. 
* **Feature Set**: declare what features you want to calculate and document the operations. The transform can be a SQL operation, time window, row window, custom function and so on.
* **Sink**: declare to what data stores to write the resulting dataframe.
> _Feature set implementations for this project [can be found here](https://github.com/rafaelleinio/legiti-challenge/tree/main/legiti_challenge/feature_store_pipelines/user)_

### Implemented Data Models:

So having in mind the chargeback problem described above. This project proposes the following structure:

- **Entity:** User. As we are evaluating scoring for user transactions, we have the user as the main actor for our data modeling. This means that all feature sets develop under this entity will have a key feature that can identify a specific user. In this case, we will use CPF.
- **Feature Sets**:
    - **user_chargebacks:** Aggregates the total of chargebacks from users in different time windows.
    - **user_orders:** Aggregates the total of orders from users in different time windows.

Finally, joining both feature sets with order events we can create a powerful dataset for training fraud scoring ML models :female-police-officer:.

:warning::warning::warning: We should have in mind that joining feature sets for creating historical feature evolution in a timeline is not a trivial join. We should align timestamp features and get features states for the same points in time. The following picture illustrates this:

![](https://i.imgur.com/EZqwQBr.png)
_Image from The Self-Service Data Roadmap book (Feature Store Chapter)_

This type of joining implementation for creating the dataset can be seen in the [AwesomeDatasetPipeline](https://github.com/rafaelleinio/legiti-challenge/blob/main/legiti_challenge/dataset_pipelines/awesome_dataset_pipeline.py)

## Getting started

#### Clone the project:

```bash
git clone git@github.com:rafaelleinio/legiti-challenge.git
cd legiti-challenge
```

#### Build Docker image
```bash
docker build --tag legiti-challenge .
```

#### Using the Feature Store CLI
```bash
docker run -it legiti-challenge
```

#### Notebook with Examples and details:
Please check [HERE!!!](https://github.com/rafaelleinio/legiti-challenge/blob/main/example.ipynb)

## CLI
This project provides a command line interface to interact with implemented Butterfree's data pipelines inside the library.

### Available Functions:
```bash=
❯ docker run -it legiti-challenge --help
Usage: cli.py [OPTIONS] COMMAND [ARGS]...

  All you need for running your feature store pipelines!

Options:
  --help  Show this message and exit.

Commands:
  describe        Show pipeline details and metadata.
  execute         Executes a defined pipeline.
  list-pipelines  List all available pipelines to execute.
```

### Listing available pipelines:
```bash=
❯ docker run -it legiti-challenge list-pipelines --help
Usage: cli.py list-pipelines [OPTIONS]

  List all available pipelines to execute.

Options:
  --help  Show this message and exit.
```

```bash=
❯ docker run -it legiti-challenge list-pipelines
The available pipelines are the following:
['feature_store.user_orders', 'feature_store.user_chargebacks', 'dataset.awesome_dataset']
```

### Describing a pipeline
```bash=
❯ docker run -it legiti-challenge describe --help
Usage: cli.py describe [OPTIONS] PIPELINE_NAME

  Show pipeline details and metadata.

Options:
  --help  Show this message and exit.
```

Let's discover metadata details about the construction of the Awesome dataset pipeline (the main goal for this project).
```bash=
❯ docker run -it legiti-challenge describe dataset.awesome_dataset
Pipeline definition:
[{'description': 'Dataset enriching orders events with aggregated features on '
                 'total of orders and chargebacks by user.',
  'feature_set': 'awesome_dataset',
  'features': [{'column_name': 'order_id',
                'data_type': 'StringType',
                'description': 'Orders unique identifier.'},
               {'column_name': 'timestamp',
                'data_type': 'TimestampType',
                'description': 'Time tag for the state of all features.'},
               {'column_name': 'chargeback_timestamp',
                'data_type': 'TimestampType',
                'description': 'Timestamp for the order creation.'},
               {'column_name': 'cpf',
                'data_type': 'StringType',
                'description': 'User unique identifier, user entity key.'},
               {'column_name': 'cpf_orders__count_over_3_days_rolling_windows',
                'data_type': 'IntegerType',
                'description': 'Count of orders over 3 days rolling windows '
                               'group by user (identified by CPF)'},
               {'column_name': 'cpf_orders__count_over_7_days_rolling_windows',
                'data_type': 'IntegerType',
                'description': 'Count of orders over 7 days rolling windows '
                               'group by user (identified by CPF)'},
               {'column_name': 'cpf_orders__count_over_30_days_rolling_windows',
                'data_type': 'IntegerType',
                'description': 'Count of orders over 30 days rolling windows '
                               'group by user (identified by CPF)'},
               {'column_name': 'cpf_chargebacks__count_over_3_days_rolling_windows',
                'data_type': 'IntegerType',
                'description': 'Count of chargebacks over 3 days rolling '
                               'windows group by user (identified by CPF)'},
               {'column_name': 'cpf_chargebacks__count_over_7_days_rolling_windows',
                'data_type': 'IntegerType',
                'description': 'Count of chargebacks over 7 days rolling '
                               'windows group by user (identified by CPF)'},
               {'column_name': 'cpf_chargebacks__count_over_30_days_rolling_windows',
                'data_type': 'IntegerType',
                'description': 'Count of chargebacks over 30 days rolling '
                               'windows group by user (identified by CPF)'}],
  'sink': [{'writer': 'Dataset Writer'}],
  'source': [{'location': 'data/order_events/input.csv',
              'reader': 'File Reader'},
             {'location': 'data/feature_store/historical/user/user_chargebacks',
              'reader': 'File Reader'},
             {'location': 'data/feature_store/historical/user/user_orders',
              'reader': 'File Reader'}]}]
```
We can see here that this pipeline depends on having available the feature sets `user_chargebacks` and `user_orders`. So let's build them!!

### Executing Pipelines

```bash=
❯ docker run -it legiti-challenge execute --help
Usage: cli.py execute [OPTIONS] PIPELINE_NAME

  Executes a defined pipeline.

Options:
  --start-date TEXT  Lower time bound reference for the execution.
  --end-date TEXT    Upper time bound reference for the execution.
  --help             Show this message and exit.
```

#### Building user_orders pipeline
```bash=
❯ docker run -v $(pwd)/data:/legiti-challenge/data -it legiti-challenge execute feature_store.user_orders --end-date 2020-07-17
>>> feature_store.user_orders pipeline execution initiated...
>>> Pipeline execution finished!!!                                              
>>> Virtual Online Feature Store result:
+---+-------------------+---------------------------------------------+---------------------------------------------+----------------------------------------------+
|cpf|          timestamp|cpf_orders__count_over_3_days_rolling_windows|cpf_orders__count_over_7_days_rolling_windows|cpf_orders__count_over_30_days_rolling_windows|
+---+-------------------+---------------------------------------------+---------------------------------------------+----------------------------------------------+
|  y|2020-05-13 00:00:00|                                            0|                                            0|                                             0|
|  z|2020-06-11 00:00:00|                                            0|                                            0|                                             0|
|  x|2020-07-17 00:00:00|                                            2|                                            2|                                             2|
+---+-------------------+---------------------------------------------+---------------------------------------------+----------------------------------------------+

>>> Local Historical Feature Store results at data/feature_store/historical/user/user_orders
```

Please notice here the usage of the argument `--end-date` as all `AggregatedFeatureSet` on Butterfree need to be executed with at least the end timepoint reference. `--start-date` can be used as well.

Butterfree has an awesome feature that we call "Idempotent Interval Run". This means that we can run pipelines for specific time ranges and overwriting just these exactly daily partitions on Historical Feature Store on datalake. With this feature we can, for example, implement performative daily incremental runs or backfilling "holes" in your data timeline. Cool right? :sunglasses:
> _this feature is well show in the [example notebook](https://github.com/rafaelleinio/legiti-challenge/blob/main/example.ipynb)_

:warning: **Important!** As shown in the command we should **remember to use the volume option** in docker run so we keep the results in our local file system too, inside the repository.

_____________

#### Building user_chargebacks pipeline:
Just run the same for user_chargebacks, with
```bash
docker run -v $(pwd)/data:/legiti-challenge/data -it legiti-challenge execute feature_store.user_chargebacks --end-date 2020-07-17
```
_____________
#### Building the Awsome Dataset:
Now with both feature sets created on historical feature store, we have all data dependencies finished. So we can create the `awesome_dataset`!!! :rocket:

```bash=
❯ docker run -v $(pwd)/data:/legiti-challenge/data -it legiti-challenge execute dataset.awesome_dataset
>>> dataset.awesome_dataset pipeline execution initiated...
>>> Pipeline execution finished!!!                                           
>>> Dataset results at data/datasets/awesome_dataset

```
_____________
#### TL;DR?
We can run all three previous executions with a single command too! :sparkles:

```bash=
make awesome-dataset
```
> _Please note that this can take some minutes as Spark is kind of slow for local runs and small files, besides, this commands is creating and running each spark job sequentially._

_____________

#### Data files
A the end of all executions, we will have under `data/` the following created files:
![](https://i.imgur.com/JxCsKYj.png)
> _historical feature store tables are partitioned by year, month, day as a requirement for idempotent runs :male-detective:_

### About Online Feature Store
As it is possible to check at pipeline definitions we are using the `OnlineFeatureStore` with `debug_mode=True`. This means that the online feature set tables remain only in in-memory tables. 

I even created the cassandra docker and make file commands (`make cassandra-up`, `make cassandra-down`, `make cqlsh`) to make service up and running with initial migration DDLs. However, the integration with Spark is not finished. Since it requires external plugins for this connection.

After, with a little more development time, I can finish implementing the connection from Spark to Cassandra with datastax plugin to **show locally** the Online Feature Store in Cassandra using a docker container. (SORRY!! :sweat_smile:)

## Development
From repository root directory:

Available make utility commands:
```bash=
❯ make help
Available rules:

apply-style         fix stylistic errors with black and isort 
awesome-dataset     create both user feature sets and awesome dataset 
cassandra-down      make cassandra container down 
cassandra-up        make cassandra up and running on container 
checks              run all code checks 
ci-install          command to install on CI pipeline 
clean               clean unused artifacts 
cqlsh               connect to cassandre container interactive shell 
integration-tests   run integration tests with coverage report 
quality-check       run code quality checks with flake8 
requirements        install all requirements 
requirements-dev    install dev requirements 
requirements-minimum install user requirements 
style-check         run code style checks with black 
tests               run unit and integration tests with coverage report 
unit-tests          run unit tests with coverage report 

```

### Install dependencies

```bash
make requirements
```

### Code Style
Check code style:
```bash
make style-check
```
Apply code style with black
```bash
make apply-style
```

Check code quality with flake8
```bash
make quality-check
```

### Testing and Coverage
Unit tests:
```bash
make unit-tests
```
Integration tests:
```bash
make integration-tests
```
All tests:
```bash
make tests
```

## Next Steps
Obviously this project is just a simplification. A real production operation for managing feature store data pipelines in a huge company environment has much more complexity and datails.

In this repository [issues tab](https://github.com/rafaelleinio/legiti-challenge/issues) I list some top-of-mind ideas for improving this toy project :chart_with_upwards_trend:
