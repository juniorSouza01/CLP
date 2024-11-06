import os
import time
import logging
import joblib
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import schedule
import urllib3
import random
import re

# Desativar avisos de verificação de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações gerais
URL = " "
OUTPUT_FOLDER = "downloads_csv"
AUTH = ('usuario', 'senha')
MAX_RETRIES = 4
TIMEOUT = 30
THREADS = 5

chrome_options = Options()
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--incognito")
chrome_options.add_argument("start-maximized")

chrome_prefs = {
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "download.default_directory": os.path.abspath(OUTPUT_FOLDER),
    "profile.default_content_settings.popups": 0,
    "download.extensions_to_open": "csv",
    "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
}
chrome_options.add_experimental_option("prefs", chrome_prefs)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def initialize_driver():
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)
    return driver

def create_http_session():
    """Configura a sessão HTTP com retentativas e tratamento de erro."""
    session = requests.Session()
    retries = Retry(total=MAX_RETRIES, backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504],
                    allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.auth = AUTH
    session.verify = False
    return session

def navigate_and_collect_csv_links(driver):
    """
    Navega até a URL especificada, ignora o aviso de SSL,
    e coleta links de arquivos CSV simulando cliques.
    """
    try:
        driver.get(URL)
        wait = WebDriverWait(driver, 2)

        try:
            advanced_button = wait.until(EC.element_to_be_clickable((By.ID, "details-button")))
            advanced_button.click()
            proceed_link = wait.until(EC.element_to_be_clickable((By.ID, "proceed-link")))
            proceed_link.click()
            logging.info("Aviso de SSL ignorado com sucesso.")
        except Exception:
            logging.info("Nenhum aviso de SSL encontrado para ignorar.")

        # Coletar links para arquivos CSV e seus títulos, com simulação de cliques
        csv_links = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@href, '.csv')]")))
        if not csv_links:
            logging.warning("Nenhum arquivo CSV encontrado na página.")
            return []

        csv_data = []
        for link in csv_links:
            try:
                link_text = link.text.strip()
                link_url = link.get_attribute('href')
                if link_url:
                    # Simular clique no link para download
                    driver.execute_script("arguments[0].click();", link)
                    time.sleep(1)
                    csv_data.append({'url': link_url, 'title': link_text})
                    logging.info(f"Coletado link: {link_url} com título: {link_text}")
            except Exception as e:
                logging.error(f"Erro ao coletar o link: {e}")

        return csv_data
    except Exception as e:
        logging.error(f"Erro ao navegar pela página: {e}")
        return []

def is_valid_csv(content):
    try:
        decoded_content = content.decode('utf-8')
        if decoded_content.startswith('<!DOCTYPE html>'):
            logging.error("O conteúdo baixado parece ser HTML, não CSV.")
            return False
        if re.search(r'\b<html\b', decoded_content, re.IGNORECASE):
            return False
        return True
    except Exception as e:
        logging.error(f"Erro ao validar o conteúdo: {e}")
        return False

def download_csv_file(csv_info):
    """Baixa um arquivo CSV a partir de uma URL, mantendo o nome original."""
    url, title = csv_info['url'], csv_info['title']
    file_name = f"{title}"
    file_path = os.path.join(OUTPUT_FOLDER, file_name)
    
    session = create_http_session()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = session.get(url, headers=headers, timeout=TIMEOUT)
            if response.status_code == 200 and is_valid_csv(response.content):
                if not os.path.exists(OUTPUT_FOLDER):
                    os.makedirs(OUTPUT_FOLDER)
                with open(file_path, 'wb') as file:
                    file.write(response.content)
                logging.info(f"Arquivo '{title}' salvo em: {file_path}")
                return {"status": "success", "file": file_path}
            else:
                logging.warning(f"Falha ao acessar ou validar o arquivo CSV. Status code: {response.status_code}")
                return {"status": "failed", "file": file_name}
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao baixar o arquivo '{title}': {e}")
            if attempt == MAX_RETRIES:
                return {"status": "failed", "file": file_name}
            time.sleep(random.uniform(1))

def download_all_csvs():
    """Função principal para realizar o download de todos os arquivos CSV disponíveis na página."""
    driver = initialize_driver()
    success_files, failed_files = [], []
    try:
        csv_data = navigate_and_collect_csv_links(driver)
        if csv_data:
            with ThreadPoolExecutor(max_workers=THREADS) as executor:
                futures = [executor.submit(download_csv_file, csv_info) for csv_info in csv_data]
                for future in as_completed(futures):
                    result = future.result()
                    if result["status"] == "success":
                        success_files.append(result["file"])
                    else:
                        failed_files.append(result["file"])

            # Log final dos arquivos baixados com sucesso e falhas
            logging.info(f"Arquivos baixados com sucesso: {len(success_files)}")
            for file in success_files:
                logging.info(f"  - {file}")
            logging.info(f"Falhas ao baixar arquivos: {len(failed_files)}")
            for file in failed_files:
                logging.info(f"  - {file}")
        else:
            logging.warning("Nenhum link de CSV foi encontrado.")
    finally:
        driver.quit()

def setup_job():
    schedule.every().day.at("17:13").do(download_all_csvs)
    logging.info("Job agendado para baixar arquivos diariamente às 13:40")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    try:
        setup_job()
    except KeyboardInterrupt:
        logging.info("Job interrompido pelo usuário.")
