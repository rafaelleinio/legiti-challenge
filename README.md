# Meli Challenge
_A nice Graph and Spark based solution for the Characters Interactions problem._

![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8-brightgreen.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![aa](https://img.shields.io/badge/code%20quality-flake8-blue)](https://github.com/PyCQA/flake8)

Build Status:

| Core     | Docker Image 
| -------- | -------- 
| ![Tests](https://github.com/rafaelleinio/meli-challenge/workflows/Tests/badge.svg?branch=main)     | ![Build Docker Image](https://github.com/rafaelleinio/meli-challenge/workflows/Build%20Docker%20Image/badge.svg?branch=main)    

## The Challenge and Proposed Arch for Solution
The objective of the challenges is to analyze networks of interaction between the characters in the books of the saga A Song of Ice and Fire, written by George R. R. Martin.

A dataset (`data/dataset.csv`) was provided informing data about all interactions for every character in the first 3 books.

The challenges basically are:
- **1st Challenge**: compute and display the total of interactions for all characters, for each book in the knowledge base.
- **2nd Challenge**: compute and display all of the mutual friendships between all pairs of characters with more than one friend in common.
- **3rd Challenge**: create an API to register new relations and query mutual friends between two characters.

In the course of this readme more in-depth details will be provided about the challenges and the expected results for each one of them.

## Proposed Solution

The problem was addressed by modeling the data as a graph structure. As it was about characters and the relationships between them, it was easy to model this structure using the characters as the vertices and the interactions the edges.

The technology used to be thee processing engine for the data is [Apache Spark](https://spark.apache.org/). Spark it is a fast and big-data-ready technology for data processing and has several extension for different data domains.
Here in this repository I use the [Graphframes](https://github.com/graphframes/graphframes) Spark extension, which is a Graph processing engine mounted on top of the powerful Dataframe API from Spark.

Main benefits from this technology decision:
- Apache Spark is the state-of-the-art big data technology used in modern data platforms nowadays.
- The same `core` code here used to process some KBs of data in a single machine can be used for multi-TBs of data in a huge cluster of instances with little to no changes.
- Spark can process batch and streams of data equivalently easy and both modes are used and available in the `core` module.
    - This means we can maintain a in-memory **Graph representation updating in real-time** from streaming knowledge base dataset.


`Core` module simple representation:
![](https://i.imgur.com/DsXIv9r.png)


## Getting started

#### Clone the project:

```bash
git clone git@github.com:rafaelleinio/meli-challenge.git
cd meli-challenge
```

#### Build Docker image
```bash
docker build --tag meli-challenge .
```

#### Get in container context
```bash
docker --network host run -it meli-challenge
```

## CLI
Command line client to interact with the graph knowledge base and print the summarized interactions and mutual friendships between characters.

> Run commands from container context:

```bash
python meli_challenge/cli.py --help
```
Output:
```
usage: cli.py [-h] {summarize_interactions,summarize_mutual_friendships} ...

positional arguments:
  {summarize_interactions,summarize_mutual_friendships}
                        Desired action to perform
    summarize_interactions
                        Display the sum of interactions over defined books for
                        all characters.
    summarize_mutual_friendships
                        Display mutual friendships between all pair of
                        characters.

optional arguments:
  -h, --help            show this help message and exit

```

There are two executions modes:
- **summarize_interactions**: compute and print the aggregated sum of interactions over defined books from all characters. **(Challenge 1)** 
- **summarize_mutual_friendships**: compute and print the array aggregation of all mutual friends from all characters. **(Challenge 2)**

#### summarize_interactions
```bash
python meli_challenge/cli.py summarize_interactions --help
```
Output:
```bash
usage: cli.py summarize_interactions [-h] --csv CSV_PATH --books BOOKS
                                     [BOOKS ...]

optional arguments:
  -h, --help            show this help message and exit
  --csv CSV_PATH        Knowledge base input on CSV format.
  --books BOOKS [BOOKS ...]
                        Book numbers to query for.

```

Args:
- **csv**: path to csv file to input the knowledge base for building the graph.
- **books**: books to aggregate over.

Example:
```bash
python meli_challenge/cli.py summarize_interactions --csv data/dataset.csv --books 1 2 3
```
Output:
```
Tyrion-Lannister	650,829,782,2261
Jon-Snow	784,360,756,1900
Joffrey-Baratheon	422,629,598,1649
Eddard-Stark	1284,169,94,1547
Sansa-Stark	545,313,532,1390
...
```
#### summarize_mutual_friendships

```bash
python meli_challenge/cli.py summarize_mutual_friendships --help
```
Output:
```
usage: cli.py summarize_mutual_friendships [-h] --csv CSV_PATH

optional arguments:
  -h, --help      show this help message and exit
  --csv CSV_PATH  Knowledge base input on CSV format.

```

Args:
- **csv**: path to csv file to input the knowledge base for building the graph.

Example:
```bash
python meli_challenge/cli.py summarize_mutual_friendships --csv data/dataset.csv
```

Output:
```
Addam-Marbrand	Kevan-Lannister	Tywin-Lannister,Tyrion-Lannister,Varys,Joffrey-Baratheon,Jaime-Lannister
Alayaya	Mandon-Moore	Cersei-Lannister,Tyrion-Lannister,Bronn
Alyn	Maron-Greyjoy	Eddard-Stark
Amabel	Chiswyck	Arya-Stark
Arthur-Dayne	Lewyn-Martell	Gerold-Hightower
...
```

## API


> Run command from container context:

```bash
python meli_challenge/server.py
```

This command will start up the server listening the request on port 5000 (default Flask)


There are 2 endpoints:

#### HTTP POST `/interaction`
which will receive as a parameter a JSON with the interaction between 2 characters, with the following format:
```json
{
    "source": "Character Name 1",
    "target": "Character Name 2",
    "weight": "Number of interactions between the two characters in 1 particular book ",
    "book": "Number of the book in the saga where the interaction took place"
}
```

#### HTTP GET `/common-friends​`
From: `/common-friends?source=P1_NAME&target=P2_NAME`

- P1_NAME: refers to character 1
- P2_NAME: refers to character 2

In response to success, the API will return status code 200 followed by the list of
mutual friends between the two characters, in the following format:
```json
{
    "common_friends": ["Cersei-Lannister", "Arya-Stark"]
}
```

### Example:
From another shell tab, that can be outside container context, you'll be able to perform the requests.

#### Adding new connection:
```bash
curl -i -X POST -H "Content-Type: application/json" \
	-d '{"source":"c1","target":"c2","weight":3,"book":4}' \
	'http://localhost:5000/interaction'
```
Output:
```
HTTP/1.0 201 CREATED
Content-Type: application/json
Content-Length: 2
Server: Werkzeug/1.0.1 Python/3.7.8
Date: Thu, 01 Oct 2020 14:13:40 GMT

Ok
```

Adding more connections:
```bash
curl -i -X POST -H "Content-Type: application/json" \
	-d '{"source":"c3","target":"c2","weight":3,"book":4}' \
	'http://localhost:5000/interaction'

curl -i -X POST -H "Content-Type: application/json" \
	-d '{"source":"c1","target":"c4","weight":3,"book":4}' \
	'http://localhost:5000/interaction'

curl -i -X POST -H "Content-Type: application/json" \
	-d '{"source":"c3","target":"c4","weight":3,"book":4}' \
	'http://localhost:5000/interaction'
```

Query the Graph for mutual connections between c1 and c3:
```bash
curl -i -X GET -H "Content-Type: application/json" \
	'http://localhost:5000/common-friends?source=c1&target=c3'
```


Output:
```
HTTP/1.0 200 OK
Content-Type: application/json
Content-Length: 31
Server: Werkzeug/1.0.1 Python/3.7.8
Date: Thu, 01 Oct 2020 14:15:04 GMT

{"common_friends":["c2","c4"]}
```



## Development
From repository root directory:

#### Install dependencies

```bash
make requirements
```

#### Code Style
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

#### Testing and Coverage
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
