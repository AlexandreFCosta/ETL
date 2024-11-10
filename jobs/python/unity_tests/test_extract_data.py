import pytest
import requests
import json
from unittest.mock import patch, mock_open
from jobs.python.bronze.extract_data import extract_data

# Função auxiliar para simular uma resposta JSON
def mock_requests_get(*args, **kwargs):
    class MockResponse:
        def json(self):
            # Exemplo de dados de retorno da API
            return [{"id": "5128df48-79fc-4f0f-8b52-d06be54d0cec", "name": "(405) Brewing Co", "brewery_type": "micro", "address_1": "1716 Topeka St", "address_2": null, "address_3": null, "city": "Norman", "state_province": "Oklahoma", "postal_code": "73069-8224", "country": "United States", "longitude": "-97.46818222", "latitude": "35.25738891", "phone": "4058160490", "website_url": "http://www.405brewing.com", "state": "Oklahoma", "street": "1716 Topeka St"}]
    return MockResponse()

@patch('requests.get', side_effect=mock_requests_get)
@patch('builtins.open', new_callable=mock_open)
def test_extract_data(mock_file, mock_requests):
    # Executa a função que está sendo testada
    extract_data()
    
    # Verifica se a URL correta foi chamada
    mock_requests.assert_called_once_with('https://api.openbrewerydb.org/breweries')
    
    # Verifica se o arquivo foi aberto no caminho correto
    bronze_path = '/opt/airflow/bronze_layer/breweries_raw.json'
    mock_file.assert_called_once_with(bronze_path, 'w')
    
    # Verifica se os dados foram salvos corretamente no arquivo
    expected_data = [{"id": "5128df48-79fc-4f0f-8b52-d06be54d0cec", "name": "(405) Brewing Co", "brewery_type": "micro", "address_1": "1716 Topeka St", "address_2": null, "address_3": null, "city": "Norman", "state_province": "Oklahoma", "postal_code": "73069-8224", "country": "United States", "longitude": "-97.46818222", "latitude": "35.25738891", "phone": "4058160490", "website_url": "http://www.405brewing.com", "state": "Oklahoma", "street": "1716 Topeka St"}]
    
    mock_file().write.assert_called_once_with(json.dumps(expected_data))

