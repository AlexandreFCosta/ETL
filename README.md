# BEES Data Engineering – Breweries Case

## Objective

The objective of this project is to demonstrate skills in consuming data from an API, transforming it, and persisting it in a data lake following the medallion architecture, with three layers: raw data, curated data partitioned by location, and an aggregated analytical layer.

## Architecture and Tools Used

- **API**: Open Brewery DB for brewery listings. Endpoint used:
  ```
  https://api.openbrewerydb.org/breweries
  ```

- **Orchestration Tool**: I chose **Airflow** to build the data pipeline due to its ability to handle scheduling, retries, and error handling efficiently.

- **Language**: I used **Python** for data requests and transformation, integrating **PySpark** for large-scale data processing.

- **Containerization**: The project was modularized using **Docker**, facilitating the development environment and running pipelines in isolated containers.

- **Data Lake Layers**:
- Jobs/Python/
  - **data_quality**: Data quality layer to guarantee data extraction and integrity.
  - **unity_tests**: Unit testing layer to guarantee that the extraction will take place correctly.
  - **Bronze (Raw Data)**: API data is stored in JSON format without transformations.
  - **Silver (Curated Data)**: Data is transformed and partitioned by location, stored in Parquet format.
  - **Gold (Analytical Layer)**: An aggregated view containing the number of breweries by type and location.

## Execution

### Prerequisites

1. **Docker**: Make sure Docker is installed. If not, you can install it following the instructions [here](https://docs.docker.com/get-docker/).
2. **Docker Compose**: Installed along with Docker Desktop.

### How to Run the Project

1. Clone this repository:
   ```bash
   git clone -b master https://github.com/AlexandreFCosta/ETL.git
   cd ETL
   ```

3. Start the Docker environment:
   ```bash
   python controller.py
   ```

4. Access the Airflow interface:
   - Open your browser and go to `http://localhost:8080`
   - Use the default credentials `airflow` to log in = username: alexa and password: 1234.

5. Run the pipeline directly from the Airflow interface.

### Monitoring and Alerts

- **Monitoring**: I used Airflow for monitoring the DAGs, configuring automatic retries to handle potential pipeline failures.:
<img height="300em" src="https://github.com/AlexandreFCosta/ETL/blob/master/Documentation/images/monitoring.png"/>

- **Alerts**  : For notifications of failures or critical errors in the pipeline, I set up alerts via **Slack**, which sends an message on my channel "desafio-bees" in case of failure. If you want to log in to Workspace to see the alerts, go to <a href="https://join.slack.com/t/tc-tech-workspace/shared_invite/zt-2uavr9nvo-sXE_Sq5GNt2brn6Hdz8fmw">Slack Worspace </a>:
<img height="300em" src="https://github.com/AlexandreFCosta/ETL/blob/master/Documentation/images/slack.png"/>

### Testing

Tests were included to validate:
- API consumption

## Final Considerations

The pipeline was designed with a focus on modularity and scalability. Additionally, resilience measures were implemented, such as automatic retries and Slack-configured alerts.

In the load_data.py file you can see that I had to forcefully delete the gold layer files and write them with “append” instead of simply using “overwrite”, because I couldn't use it on my machine.
<img height="300em" src="https://github.com/AlexandreFCosta/ETL/blob/master/Documentation/images/overwrite.png"/>
I encountered the error several times:
```
FileUtil: Failed to delete file or dir [/opt/***/gold_layer]: it still exists.
```
Even though I had increased the permissions and had the correct, compatible versions of all the libraries, the error persisted. As I realized that it could be a fault caused by some limitation of my machine and I didn't want to compromise the delivery of the challenge because of this, I opted to continue development using the clear_gold_layer() function to clear the directory so that we could write the data. However, in a situation with more time, I would like to mitigate the problem from the root.
