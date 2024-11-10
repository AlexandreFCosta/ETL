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
   git clone https://github.com/AlexandreFCosta/desafio_abinbev.git
   cd desafio_abinbev
   ```

2. Create the `.env` file to define the required environment variables, such as Gmail credentials for sending alerts.

3. Start the Docker environment:
   ```bash
   docker-compose up
   ```

4. Access the Airflow interface:
   - Open your browser and go to `http://localhost:8080`
   - Use the default credentials `airflow` to log in.

5. Run the pipeline directly from the Airflow interface.

### Monitoring and Alerts

- **Monitoring**: I used Airflow for monitoring the DAGs, configuring automatic retries to handle potential pipeline failures.
- **Alerts**: For notifications of failures or critical errors in the pipeline, I set up alerts via **Gmail**, which sends an email in case of failure.

### Testing

Tests were included to validate:
- API consumption
- Data transformation and partitioning
- Aggregations for the Gold layer

## Final Considerations

The pipeline was designed with a focus on modularity and scalability. Additionally, resilience measures were implemented, such as automatic retries and Gmail-configured alerts.
