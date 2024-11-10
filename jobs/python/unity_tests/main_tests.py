import pytest

def run_tests():
    print("Rodando test_load_data...")
    pytest.main(["test_load_data.py"])

if __name__ == "__main__":
    print("Iniciando a execução do teste ...")
    run_tests()
    print("Todos os testes foram executados.")
