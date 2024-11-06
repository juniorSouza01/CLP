import os
import csv
import json
import firebase_admin
from firebase_admin import credentials, firestore

# Inicialize o Firebase
cred = credentials.Certificate("./credentials.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

def process_and_store_csv(file_path):
    """Processa o arquivo CSV e armazena os dados no Firebase."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                # Organize os dados em um formato adequado para o Firebase
                data = {key: value for key, value in row.items() if key and value}

                # Verifique se todos os componentes são strings ou valores válidos
                if all(isinstance(key, str) and key.strip() and isinstance(value, str) and value.strip() for key, value in data.items()):
                    # Salve os dados no Firestore em uma coleção
                    db.collection("collection").add(data) #trocar o nome depois
                    print(f"Dados armazenados com sucesso: {data}")
                else:
                    print(f"Dados inválidos encontrados e descartados: {data}")
        print("Todos os dados foram armazenados no Firebase.")
    except Exception as e:
        print(f"Erro ao processar o arquivo CSV: {e}")

def run_csv_to_firebase_job():
    """Job que processa todos os arquivos CSV na pasta e armazena no Firebase."""
    try:
        for file_name in os.listdir("downloads_csv"):
            if file_name.endswith(".csv"):
                file_path = os.path.join("downloads_csv", file_name)
                process_and_store_csv(file_path)
    except Exception as e:
        print(f"Erro ao executar o job: {e}")

if __name__ == "__main__":
    run_csv_to_firebase_job()
