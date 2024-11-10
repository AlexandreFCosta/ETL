from pyspark.sql import SparkSession
import requests
import json
import os

class DataExtractor:
    API_URL = 'https://api.openbrewerydb.org/breweries'
    # BRONZE_PATH = '/opt/airflow/bronze_layer/breweries_raw.json'
    BRONZE_PATH = '\\bronze_layer'

    def __init__(self, spark_session):
        self.spark = spark_session

    def extract_data(self):
        print("Iniciando extração de dados da API.")
        try:
            response = requests.get(self.API_URL)
            response.raise_for_status()  # Levanta uma exceção para códigos de status HTTP de erro
            data = response.json()
            self._save_data(data)
        except requests.RequestException as e:
            print(f"Erro ao buscar dados da API: {e}")

    def _save_data(self, data):
        os.makedirs(os.path.dirname(self.BRONZE_PATH), exist_ok=True)
        with open(self.BRONZE_PATH, 'w') as f:
            json.dump(data, f)
        print(f"Dados salvos em {self.BRONZE_PATH}")

def main():
    spark = SparkSession.builder.appName("ExtractData").getOrCreate()
    data_extractor = DataExtractor(spark)

    data_extractor.extract_data()

    spark.stop()

if __name__ == "__main__":
    main()