from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import json
from typing import List, Dict

class SparkSessionManager:
    """Classe responsável pela criação e gerenciamento da SparkSession."""
    @staticmethod
    def get_spark_session(app_name: str = "DataQuality") -> SparkSession:
        return SparkSession.builder.appName(app_name).getOrCreate()

class DataQualityChecker:
    """Classe que encapsula as funções de qualidade de dados para um DataFrame Spark e JSON."""
    
    def __init__(self, df: DataFrame):
        self.df = df

    def check_nulls(self, columns: List[str]) -> None:
        """Verifica se há valores nulos em colunas específicas do DataFrame."""
        null_counts = self.df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in columns]).collect()[0]
        nulls = {col: count for col, count in zip(columns, null_counts) if count > 0}
        if nulls:
            raise ValueError(f"Null values found: {nulls}")
        print("Sem linhas Null ✅")

    def check_duplicates(self, columns: List[str]) -> None:
        """Verifica se há duplicatas com base nas colunas especificadas."""
        duplicate_count = self.df.groupBy(columns).count().filter("count > 1").count()
        if duplicate_count > 0:
            raise ValueError(f"Found {duplicate_count} duplicate rows.")
        print("Sem duplicatas ✅")

class JsonQualityChecker:
    """Classe que encapsula as funções de qualidade de dados para validação de JSON."""
    
    def __init__(self, data: List[Dict]):
        self.data = data

    def check_not_empty(self) -> None:
        """Verifica se o JSON não está vazio."""
        if not self.data:
            raise ValueError("O JSON retornado está vazio, não contém entradas.")
        print("Json Não Vazio ✅")

    def check_structure(self, expected_keys: List[str]) -> None:
        """Verifica se o JSON contém as chaves esperadas."""
        for entry in self.data:
            if not all(key in entry for key in expected_keys):
                raise ValueError(f"JSON structure mismatch: {entry}")
        print("Formato Json Válido ✅")

class DataQualityPipeline:
    """Classe principal que executa as verificações de qualidade de dados."""
    
    def __init__(self, bronze_path: str, expected_json_keys: List[str]):
        self.bronze_path = bronze_path
        self.expected_json_keys = expected_json_keys
        self.spark = SparkSessionManager.get_spark_session()

    def load_json_data(self) -> List[Dict]:
        """Carrega o arquivo JSON e retorna como uma lista de dicionários."""
        with open(self.bronze_path, 'r') as f:
            return json.load(f)

    def load_dataframe(self) -> DataFrame:
        """Carrega o arquivo JSON em um DataFrame Spark."""
        return self.spark.read.json(self.bronze_path)

    def run_checks(self) -> None:
        """Executa todas as verificações de qualidade de dados."""
        try:
            # Carregar e validar o JSON
            data = self.load_json_data()
            json_checker = JsonQualityChecker(data)
            json_checker.check_not_empty()
            json_checker.check_structure(self.expected_json_keys)

            # Carregar e validar o DataFrame
            df = self.load_dataframe()
            df_checker = DataQualityChecker(df)
            df_checker.check_nulls(["name", "city", "state"])
            df_checker.check_duplicates(["id"])

        except Exception as e:
            print(f"Data Quality Check Failed: {e}")
            raise

    def stop_spark(self) -> None:
        """Finaliza a SparkSession."""
        self.spark.stop()

# Configurações e execução da pipeline de qualidade de dados
if __name__ == "__main__":
    bronze_path = '/opt/airflow/bronze_layer/breweries_raw.json'
    expected_json_keys = [
        "id", "name", "brewery_type", "address_1", "address_2", "address_3",
        "city", "state_province", "postal_code", "country", "longitude",
        "latitude", "phone", "website_url", "state", "street"
    ]

    data_quality_pipeline = DataQualityPipeline(bronze_path, expected_json_keys)
    data_quality_pipeline.run_checks()
    data_quality_pipeline.stop_spark()
