# Citation Network Graph DataBase
[![wakatime](https://wakatime.com/badge/user/018c27a8-f9f9-40a7-b4b2-4508370458bd/project/31cc6135-e8ab-4bb5-9325-ddc17baf2244.svg)](https://wakatime.com/badge/user/018c27a8-f9f9-40a7-b4b2-4508370458bd/project/31cc6135-e8ab-4bb5-9325-ddc17baf2244)

## About
This project focuses on implementing a graph-oriented database using Neo4j to explore and analyze citation networks.


## Dataset

The dataset used in this project is a network of articles and their related information.

The citation data is extracted from DBLP, ACM, MAG (Microsoft Academic Graph), and other sources. Each paper is associated with abstract, authors, year, venue, and title.

The data set can be used for clustering with network and side information, studying influence in the citation network, finding the most influential papers, topic modeling analysis, etc.

[DBLP-Citation-network V12](https://www.kaggle.com/datasets/mathurinache/citation-network-dataset): 4,894,081 papers and 45,564,149 citation relationships (2020-04-09)

## Deployment
For deployment, the project uses a Neo4j database to store the data. The database can be deployed locally or on a cloud service.
For local deployment, download and install [Neo4j Desktop](https://neo4j.com/deployment-center/#:~:text=Visit-,Neo4j%20Desktop,-Neo4j%20Desktop%20is).


### Database Configuration
Create a new database in Neo4j Desktop and set the following configurations:
- **Database Name**: citation-network
- **Password**: <db_pass>
- **User**: <db_user>
- **Port**: 7687
- **URI**: localhost


### Download the Dataset
Download the dataset from [Kaggle](https://www.kaggle.com/datasets/mathurinache/citation-network-dataset) and extract 'dblp.v12.json' in `./dataset` folder.


### Setting the Environment
Create an `.env` file in the project's root folder and add the following variables with the corresponding values:
```.env
DB_URI="localhost:7687"

DB_NAME="citation-network"
DB_PASS="<db_pass>"
DB_USER="<db_user>"

# Optional. If you want to use a test database.
TEST_DB_NAME="citation-network-test"
TEST_DB_PASS="<test_db_pass>"
TEST_DB_USER="<test_db_user>"

BATCH_SIZE_PAPER_NODES=5000     # Optional. The file will default if not set.
BATCH_SIZE_REQUIRED_NODES=10000 # Optional. The file will default if not set.
```


### Virtual environment
Open a terminal in the project's root folder and run:
```bash
python -m venv .venv
```  

Activate virtual environment:
```bash
.venv\Scripts\activate
```  


### Install Dependencies
Install the required dependencies by running:
```bash
pip install -r requirements.pip
```  


### Apply Constraints and Relations into the Database
Apply labels in the database by running: 
```bash
cd database/utils
install_labels.bat
```


## Useful Links
#### Websites
 - [Neo4j](https://neo4j.com/)
 - [Neomodel](https://github.com/neo4j-contrib/neomodel)

#### Documentation
 - [Neo4j Documentation](https://neo4j.com/docs/getting-started/)
 - [Neo4j Python Driver](https://neo4j.com/docs/python-manual/current/)
 - [Neomodel Documentation](https://neomodel.readthedocs.io/en/latest/index.html)

#### Downloads
 - [Neo4j Desktop](https://neo4j.com/deployment-center/#:~:text=Visit-,Neo4j%20Desktop,-Neo4j%20Desktop%20is)
 - [Citation Network Dataset](https://www.kaggle.com/datasets/mathurinache/citation-network-dataset)

#### Courses
 - [Graph Academy (Neo4j)](https://graphacademy.neo4j.com/)
