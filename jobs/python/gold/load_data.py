import os
import logging
from pyspark.sql import SparkSession, DataFrame
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
            .appName("DataAggregatorApp") \
            .config("spark.pyspark.python", self.python_path) \
            .config("spark.pyspark.driver.python", self.python_path) \
            .getOrCreate()


class DataAggregator:
    """Classe responsável pela agregação dos dados."""

    def __init__(self, spark: SparkSession, silver_path: str, gold_path: str):
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path

    def load_silver_data(self) -> DataFrame:
        """Carrega os dados da camada silver (parquet)."""
        logging.info(f"Carregando dados de {self.silver_path}")
        return self.spark.read.parquet(self.silver_path)

    def aggregate_data(self, df: DataFrame) -> DataFrame:
        """Agrupa os dados por estado e tipo de cervejaria e retorna o DataFrame agregado."""
        logging.info("Agrupando os dados por estado e tipo de cervejaria.")
        return df.groupBy("state", "brewery_type").count()

    def write_gold_data(self, df: DataFrame) -> None:
        """Cria uma visão agregada com a quantidade de cervejarias por tipo e localização e escreve na camada gold."""
        # Chamando a função de agregação para obter o DataFrame agregado
        aggregated_df = self.aggregate_data(df)

        logging.info(f"Escrevendo dados agregados na camada Gold em {self.gold_path}")
        
        # Gravando os dados agregados na camada Gold em formato Parquet
        aggregated_df.write.mode('append').parquet(self.gold_path)
        
        logging.info(f"Dados agregados e salvos em {self.gold_path}")

    def clear_gold_layer(self) -> None:
        """Limpa a camada gold antes de gravar novos dados."""
        if os.path.exists(self.gold_path):
            os.system(f"rm -rf {self.gold_path}/*")
            logging.info("Camada Gold limpa.")


def main():
    """Função principal para controlar o fluxo do processo de agregação de dados."""
    logging.basicConfig(level=logging.INFO)  # Configurando o logger

    # Definindo caminhos
    silver_path = '/opt/airflow/silver_layer'
    gold_path = '/opt/airflow/gold_layer'

    # Criando a configuração do Spark
    spark_config = SparkConfig()
    spark = spark_config.create_spark_session()

    # Criando a instância de DataAggregator
    aggregator = DataAggregator(spark, silver_path, gold_path)

    # Limpeza da camada Gold
    aggregator.clear_gold_layer()

    # Carregando e salvando os dados agregados na camada Gold
    df_silver = aggregator.load_silver_data()
    aggregator.write_gold_data(df_silver)

    # Finalizando a sessão Spark
    spark.stop()


if __name__ == "__main__":
    main()
