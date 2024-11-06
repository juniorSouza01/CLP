import os
import csv
import json
import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("./credentials.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

def process_and_store_csv(file_path):
    """Processa o arquivo CSV e armazena os dados no Firebase."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data = {key: value for key, value in row.items() if key and value}


                if all(isinstance(key, str) and key.strip() and isinstance(value, str) and value.strip() for key, value in data.items()):
                    db.collection("collection").add(data) #trocar o nome depois
                    print(f"Dados armazenados com sucesso: {data}")
                else:
                    print(f"Dados inválidos encontrados e descartados: {data}")
        print("Todos os dados foram armazenados no Firebase.")
    except Exception as e:
        print(f"Erro ao processar o arquivo CSV: {e}")

def run_csv_to_firebase_job():
    try:
        for file_name in os.listdir("downloads_csv"):
            if file_name.endswith(".csv"):
                file_path = os.path.join("downloads_csv", file_name)
                process_and_store_csv(file_path)
    except Exception as e:
        print(f"Erro ao executar o job: {e}")

if __name__ == "__main__":
    run_csv_to_firebase_job()
