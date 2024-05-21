
# Citation Network Graph DataBase

This project focuses on implementing a graph-oriented database using Neo4j to explore and analyze citation networks.


## Dataset

The dataset used in this project is a network of articles and their related information.

The citation data is extracted from DBLP, ACM, MAG (Microsoft Academic Graph), and other sources. Each paper is associated with abstract, authors, year, venue, and title.

The data set can be used for clustering with network and side information, studying influence in the citation network, finding the most influential papers, topic modeling analysis, etc.

[DBLP-Citation-network V12](https://www.kaggle.com/datasets/mathurinache/citation-network-dataset): 4,894,081 papers and 45,564,149 citation relationships (2020-04-09)

## Deployment
Configuration to deploy this project.

### Virtual environment
Open a terminal in the project's root folder and run:
```bash
python -v venv .venv
```  

Activate virtual environment
```bash
.venv\Scripts\activate
```  

### Install Dependencies
Install necessary libraries
```bash
pip install -r requirements.pip
```  

### Apply Constraints and Relations on DB
Apply labels in the DataBase
```bash
neomodel_install_labels --db bolt://<user>:<db_password>@<ip_address>:<port>/<db_name> <someapp.models/app.py> <someapp.models/app.py>
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