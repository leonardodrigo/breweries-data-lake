# Open Brewery Datalake

The primary objective of this project was to develop a data pipeline that ingests brewery-related data from the [Open Brewery DB API](https://www.openbrewerydb.org/), processes it, and stores it in various layers of a data lake using Medallion Architecture. This was achieved with a stack of technologies including Airflow for orchestrating workflows, Spark for processing data, Docker and Docker Compose for containerization and orchestration, and Azure for cloud storage.

![](/img/project_diagram.png)

## About the Data

The Open Brewery DB provides an API with public information on breweries, cideries, brewpubs, and bottleshops worldwide. For this project, we utilize the `/breweries` endpoint to retrieve data specifically about breweries. This is the only type of data that will be processed by this project.

### Sample Data

Here is an example of the data returned by the `/breweries` endpoint:

```json
{
    "id": "34e8c68b-6146-453f-a4b9-1f6cd99a5ada",
    "name": "1 of Us Brewing Company",
    "brewery_type": "micro",
    "address_1": "8100 Washington Ave",
    "address_2": null,
    "address_3": null,
    "city": "Mount Pleasant",
    "state_province": "Wisconsin",
    "postal_code": "53406-3920",
    "country": "United States",
    "longitude": "-87.88336350209435",
    "latitude": "42.72010826899558",
    "phone": "262-484-7553",
    "website_url": "https://www.1ofusbrewing.com",
    "state": "Wisconsin",
    "street": "8100 Washington Ave"
}

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

The things you need before installing the project.

* **Git**  
  [Documentation](https://git-scm.com/doc)
* **Docker Desktop**  
  Includes Docker Compose and Docker Engine  
  [Installation Guide](https://docs.docker.com/desktop/install/mac-install/)
* **Docker Engine**  
  Only needed if Docker Desktop is not installed  
  [Installation Guide](https://docs.docker.com/engine/install/)
* **Azure Cloud Account**  
  [Create an Account](https://azure.microsoft.com/en-us)

### Azure Setup

For storing the data of this project, we chose Azure to create a cloud-based data lake. The only Azure resource used in this project is Azure Data Lake Storage, which is configured within an Azure Storage Account. To set up Azure Data Lake Storage, follow these steps:

1. **Create an Azure Storage Account:**
   - Sign in to the [Azure Portal](https://portal.azure.com).
   - Navigate to **"Storage accounts"** and click **"Create"**.
   - Choose the appropriate subscription and resource group. If you don't have a resource group, you can create a new one.
   - Provide a unique name for the storage account.
   - Select the **"Region"** where you want your data to be stored.
   - Choose **"StorageV2"** for the **"Performance"** and **"Replication"** options based on your requirements.
   - Click **"Review + create"** and then **"Create"** to provision the storage account.

2. **Configure Azure Data Lake Storage:**
   - Once the storage account is created, navigate to it in the Azure Portal.
   - Go to the **"Containers"** section and click **"Add container"** to create a new container for your data lake. Set permissions (private, blob, or container) based on your access requirements.
   - To enable hierarchical namespace (necessary for Data Lake Storage Gen2 features), go to **"Configuration"** under the storage account settings, and switch on **"Hierarchical namespace"**.

3. **Set Up Access Controls:**
   - Configure access permissions to your storage account and containers using **"Access control (IAM)"**. Assign roles such as **"Storage Blob Data Contributor"** or **"Storage Blob Data Reader"** to users or service principals.
   - Additionally, you may set up Shared Access Signatures (SAS) or Azure Active Directory (AAD) for fine-grained access control.

In this storage account, was created only 3 containers, each one correspondig to is datalake layer (bronze, silver and gold)

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
