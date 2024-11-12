import sys
import os
import pytest

# Caminho para o diretório raiz do projeto
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../.."))
sys.path.append(root_dir)


def run_tests():
    # Define o caminho absoluto para o arquivo de teste
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_file_path = os.path.join(current_dir, "test_extract_data.py")
    
    print("Rodando test_extract_data...")
    pytest.main([test_file_path])

if __name__ == "__main__":
    print("Iniciando a execução do teste ...")
    run_tests()
    print("Todos os testes foram executados.")
