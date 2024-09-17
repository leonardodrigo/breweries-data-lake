# Open Brewery Datalake

The primary objective of this project was to develop a data pipeline that ingests brewery-related data from the Open Brewery Database API, processes it, and stores it in various layers of a data lake using Medallion Architecture. This was achieved with a stack of technologies including Airflow for orchestrating workflows, Spark for processing data, Docker and Docker Compose for containerization and orchestration, and Azure for cloud storage.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

The things you need before installing the project.

* **Git**
  [Documentation](https://git-scm.com/doc)
* **Docker Desktop**  
  (Includes Docker Compose and Docker Engine)  
  [Installation Guide](https://docs.docker.com/desktop/install/mac-install/)
* **Docker Engine**  
  (Only needed if Docker Desktop is not installed)  
  [Installation Guide](https://docs.docker.com/engine/install/)
* **Azure Cloud Account**  
  [Create an Account](https://azure.microsoft.com/en-us)

### Installation

With prerequisites already installed and setup, clone this repository in your machine.
```
$ git clone https://github.com/leonardodrigo/breweries-data-lake.git
```

Enter in the project directory.

For MacOS and Linux
```
$ cd /breweries-data-lake
```

For Windows
```
$ dir /breweries-data-lake
```

Now, pull the docker images defined in ```docker-compose.yaml``` and build the containers. In the first time, it may take a few minutes to complete.
```
$ docker compose build
```

Initialize all Airflow and Spark containers and define the number of Spark workers we want in the cluster. The command below initializes 3 workers with 1GB of memory and 1 CPU core, but fell free to set up more or less workers according to your machine resources. It is possible to change worker configuration as well.
```
$ docker compose up --scale spark-worker=3 -d
```
