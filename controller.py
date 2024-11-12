import subprocess

def run_tests():
    try:
        print("Executando testes...")
        subprocess.run(["python", "./jobs/python/unity_tests/main_tests.py"], check=True)
        print("Testes concluídos com sucesso!")
    except subprocess.CalledProcessError:
        print("Erro ao executar os testes.")

def start_docker():
    try:
        print("Iniciando Docker Compose...")
        subprocess.run(["docker-compose", "up", "-d", "--build"], check=True)
        print("Docker Compose iniciado com sucesso!")
    except subprocess.CalledProcessError:
        print("Erro ao iniciar o Docker Compose.")

if __name__ == "__main__":
    run_tests()
    start_docker()
