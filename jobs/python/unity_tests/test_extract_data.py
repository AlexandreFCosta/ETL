import os
import sys
import json
import unittest
import requests
from unittest.mock import patch, MagicMock, mock_open
from pyspark.sql import SparkSession

# Adiciona o caminho do diretório raiz do projeto ao PATH do sistema
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../.."))
sys.path.append(root_dir)

from jobs.python.bronze.extract_data import DataExtractor

class TestDataExtractor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Inicia uma SparkSession apenas uma vez para ser usada em todos os testes."""
        cls.spark = SparkSession.builder.master("local").appName("TestExtractData").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Finaliza a SparkSession ao terminar todos os testes."""
        cls.spark.stop()

    def setUp(self):
        """Instancia a classe DataExtractor para cada teste."""
        self.data_extractor = DataExtractor(self.spark)

    @patch("jobs.python.bronze.extract_data.requests.get")  # Caminho completo do requests.get
    def test_extract_data_success(self, mock_get):
        """Testa o sucesso da extração de dados ao simular uma resposta da API."""
        # Simula uma resposta de sucesso da API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{
            "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
            "name": "(405) Brewing Co",
            "brewery_type": "micro",
            "address_1": "1716 Topeka St",
            "address_2": None,
            "address_3": None,
            "city": "Norman",
            "state_province": "Oklahoma",
            "postal_code": "73069-8224",
            "country": "United States",
            "longitude": "-97.46818222",
            "latitude": "35.25738891",
            "phone": "4058160490",
            "website_url": "http://www.405brewing.com",
            "state": "Oklahoma",
            "street": "1716 Topeka St"
        }]
        mock_get.return_value = mock_response

        # Mock do _save_data para verificar se é chamado com os dados corretos
        with patch.object(self.data_extractor, "_save_data") as mock_save_data:
            self.data_extractor.extract_data()
            mock_get.assert_called_once_with(self.data_extractor.API_URL)
            mock_save_data.assert_called_once_with(mock_response.json.return_value)

    @patch("jobs.python.bronze.extract_data.requests.get")
    def test_extract_data_api_failure(self, mock_get):
        """Testa o comportamento quando ocorre uma falha na chamada da API."""
        # Simula uma falha na chamada da API
        mock_get.side_effect = requests.RequestException("API error")

        # Capturar o log de erro
        with self.assertLogs("jobs.python.bronze.extract_data", level="ERROR") as log:
            self.data_extractor.extract_data()
            mock_get.assert_called_once_with(self.data_extractor.API_URL)
            
            # Verificar se a mensagem de erro correta está nos logs
            self.assertIn("Erro ao buscar dados da API: API error", log.output[0])

if __name__ == "__main__":
    unittest.main()
