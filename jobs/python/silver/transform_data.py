import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Optional


class SparkConfig:
    """Classe responsável pela configuração do Spark."""

    def __init__(self, python_path: Optional[str] = None):
        # Tenta obter o caminho do Python a partir da variável de ambiente ou usa o padrão do sistema.
        self.python_path = python_path or os.getenv("PYTHON_PATH", None)
        
        if not self.python_path:
            # Se a variável PYTHON_PATH não estiver definida, tenta detectar o Python no ambiente.
            self.python_path = self._detect_python_path()

    def _detect_python_path(self) -> str:
        """Detecta o caminho do Python no sistema, se não for fornecido."""
        python_path = os.getenv("PYTHON", None)
        if python_path:
            return python_path
        
        # Caso o PYTHON_PATH não esteja configurado nem o PYTHON, usa o python do sistema.
        return "python"  # 'python' vai usar o Python do PATH do sistema

    def create_spark_session(self) -> SparkSession:
        """Cria e retorna uma sessão Spark configurada."""
        return SparkSession.builder \
            .appName("DataProcessorApp") \
            .config("spark.pyspark.python", self.python_path) \
            .config("spark.pyspark.driver.python", self.python_path) \
            .getOrCreate()


class DataProcessor:
    """Classe responsável pelo processamento dos dados."""

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

    def read_data(self) -> DataFrame:
        """Lê os dados do arquivo JSON no caminho bronze."""
        logging.info(f"Lendo dados de {self.bronze_path}")
        return self.spark.read.json(self.bronze_path)

    def transform_and_save_data(self, df: DataFrame) -> None:
        """Transforma os dados e os salva no formato Parquet."""
        logging.info(f"Iniciando transformação e salvamento dos dados em {self.silver_path}")
        df.write.mode('append') \
            .partitionBy('state', 'country') \
            .parquet(self.silver_path)
        logging.info(f"Dados transformados e salvos em {self.silver_path}")


def main():
    """Função principal para controlar o fluxo do processo de dados."""
    logging.basicConfig(level=logging.INFO)  # Configurando o logger

    # Definindo caminhos
    bronze_path = '/opt/airflow/bronze_layer/breweries_raw.json'
    silver_path = '/opt/airflow/silver_layer'

    # Criando a configuração do Spark
    spark_config = SparkConfig()
    spark = spark_config.create_spark_session()

    # Criando a instância de DataProcessor
    processor = DataProcessor(spark, bronze_path, silver_path)

    # Lendo, transformando e salvando os dados
    df = processor.read_data()
    processor.transform_and_save_data(df)

    # Finalizando a sessão Spark
    spark.stop()


if __name__ == "__main__":
    main()
