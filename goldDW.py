from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import tarfile
import csv
import shutil

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from lxml import etree

# Define the path for the input and output files
destination_path = '/home/xuanquang/project/airflow'
# Function to invest xau usa data
def invest_xauusd_data():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get("https://uk.investing.com/currencies/xau-usd-historical-data")

    html = driver.page_source
    driver.quit()


    tree = etree.HTML(html)


    table = tree.xpath('//table[contains(@class, "freeze-column-w-1")]')
    if not table:
        raise Exception("Not found table!")
    table = table[0]

    header_elements = table.xpath('.//thead//tr/th//span[1]')
    headers = [elem.text.strip() for elem in header_elements if elem.text]
    print("Headers:", headers)

    rows = table.xpath('.//tbody//tr')
    data = []
    for row in rows:
        cells = row.xpath('./td')
        row_data = []
        for cell in cells:
            texts = cell.xpath('.//text()')
            cell_text = ' '.join(t.strip() for t in texts if t.strip())
            row_data.append(cell_text)
        data.append(row_data)

    for d in data:
        print(d)

    columns_to_remove = []
    for i, header in enumerate(headers):
        col_empty = True
        for row in data:
            if len(row) > i and row[i] != '':
                col_empty = False
                break
        if col_empty:
            columns_to_remove.append(i)
    for col_index in reversed(columns_to_remove):
        del headers[col_index]
        for row in data:
            if len(row) > col_index:
                del row[col_index]
    for d in data:
        print(d)

    with open("XAUUSD.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)
# Function to invest usd vnd data
def invest_usdvnd_data():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get("https://vn.investing.com/currencies/usd-vnd-historical-data")

    html = driver.page_source
    driver.quit()


    tree = etree.HTML(html)


    table = tree.xpath('//table[contains(@class, "freeze-column-w-1")]')
    if not table:
        raise Exception("Not found table!")
    table = table[0]

    header_elements = table.xpath('.//thead//tr/th//span[1]')
    headers = [elem.text.strip() for elem in header_elements if elem.text]
    print("Headers:", headers)

    rows = table.xpath('.//tbody//tr')
    data = []
    for row in rows:
        cells = row.xpath('./td')
        row_data = []
        for cell in cells:
            texts = cell.xpath('.//text()')
            cell_text = ' '.join(t.strip() for t in texts if t.strip())
            row_data.append(cell_text)
        data.append(row_data)

    for d in data:
        print(d)

    columns_to_remove = []
    for i, header in enumerate(headers):
        col_empty = True
        for row in data:
            if len(row) > i and row[i] != '':
                col_empty = False
                break
        if col_empty:
            columns_to_remove.append(i)
    for col_index in reversed(columns_to_remove):
        del headers[col_index]
        for row in data:
            if len(row) > col_index:
                del row[col_index]
    for d in data:
        print(d)

    with open("USDVND.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)
# Function to invest sjc data
def invest_sjc_data():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get("https://uk.investing.com/equities/phu-nhuan-jewelry-jsc-historical-data")

    html = driver.page_source
    driver.quit()


    tree = etree.HTML(html)


    table = tree.xpath('//table[contains(@class, "freeze-column-w-1")]')
    if not table:
        raise Exception("Not found table!")
    table = table[0]

    header_elements = table.xpath('.//thead//tr/th//span[1]')
    headers = [elem.text.strip() for elem in header_elements if elem.text]
    print("Headers:", headers)

    rows = table.xpath('.//tbody//tr')
    data = []
    for row in rows:
        cells = row.xpath('./td')
        row_data = []
        for cell in cells:
            texts = cell.xpath('.//text()')
            cell_text = ' '.join(t.strip() for t in texts if t.strip())
            row_data.append(cell_text)
        data.append(row_data)

    for d in data:
        print(d)

    columns_to_remove = []
    for i, header in enumerate(headers):
        col_empty = True
        for row in data:
            if len(row) > i and row[i] != '':
                col_empty = False
                break
        if col_empty:
            columns_to_remove.append(i)
    for col_index in reversed(columns_to_remove):
        del headers[col_index]
        for row in data:
            if len(row) > col_index:
                del row[col_index]
    for d in data:
        print(d)

    with open("SJC.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)
# Function to extract sjc data from CSV
def extract_sjc_data_from_csv():
    input_file = f"{destination_path}/SJC.csv"
    output_file = f"{destination_path}/sjcdata.csv"

    with open(input_file, 'r', encoding='utf-8-sig', newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader, None)
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['Date', 'Price'])
            for row in reader:
                writer.writerow([row[0], row[1]])
# Function to extract xauusd data from CSV
def extract_xauusd_data_from_csv():
    input_file = f"{destination_path}/XAUUSD.csv"
    output_file = f"{destination_path}/xauusddata.csv"

    with open(input_file, 'r', encoding='utf-8-sig', newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader, None)
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['Date', 'Price'])
            for row in reader:
                writer.writerow([row[0], row[1]])
# Function to extract usdvnd data from CSV
def extract_usdvnd_data_from_csv():
    input_file = f"{destination_path}/USDVND.csv"
    output_file = f"{destination_path}/usdvnddata.csv"

    with open(input_file, 'r', encoding='utf-8-sig', newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader, None)
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['Date', 'Price'])
            for row in reader:
                writer.writerow([row[0], row[1]])

# Function to consolidate data
def consolidate_and_compare_data():
    input_file1 = f"{destination_path}/sjcdata.csv"
    input_file2 = f"{destination_path}/xauusddata.csv"
    input_file3 = f"{destination_path}/usdvnddata.csv"

    df_sjc = pd.read_csv(input_file1, usecols=["Date", "Price"], encoding='utf-8-sig')
    df_xau = pd.read_csv(input_file2, usecols=["Date", "Price"], encoding='utf-8-sig')
    df_usd = pd.read_csv(input_file3, usecols=["Date", "Price"], encoding='utf-8-sig')

    df_sjc.columns = ["Date", "SJC_Price"]
    df_xau.columns = ["Date", "XAU_Price"]
    df_usd.columns = ["Date", "USD_Price"]

    df_merged = df_sjc.merge(df_xau, on="Date", how="outer")
    df_merged = df_merged.merge(df_usd, on="Date", how="outer")

    df_merged["XAU_Price"] = df_merged["XAU_Price"].str.replace(',', '', regex=False)
    df_merged["USD_Price"] = df_merged["USD_Price"].str.replace(',', '', regex=False)
    df_merged["SJC_Price"] = df_merged["SJC_Price"].str.replace(',', '', regex=False)

    df_merged["XAU_Price"] = pd.to_numeric(df_merged["XAU_Price"], errors='coerce')
    df_merged["USD_Price"] = pd.to_numeric(df_merged["USD_Price"], errors='coerce')
    df_merged["SJC_Price"] = pd.to_numeric(df_merged["SJC_Price"], errors='coerce')

    df_merged["XAUUSD_Price"] = df_merged["XAU_Price"] * df_merged["USD_Price"]

    df_merged["Compare"] = (df_merged["XAUUSD_Price"] / df_merged["SJC_Price"]*1000)*100


    output_file = f"{destination_path}/golddata.csv"
    df_merged.to_csv(output_file, index=False, encoding='utf-8-sig')

# Default arguments for the DAG
default_args = {
    'owner': 'Your name',
    'start_date': days_ago(0),
    'email': ['your email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'Gold_price_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Define the tasks
invest_sjc_task = PythonOperator(
    task_id='invest_sjc_data',
    python_callable=invest_sjc_data,
    dag=dag,
)
invest_xauusd_task = PythonOperator(
    task_id='invest_xauusd_data',
    python_callable=invest_xauusd_data,
    dag=dag,
)
invest_usdvnd_task = PythonOperator(
    task_id='invest_usdvnd_data',
    python_callable=invest_usdvnd_data,
    dag=dag,
)
extract_sjc_task = PythonOperator(
    task_id='extract_sjc_data_from_csv',
    python_callable=extract_sjc_data_from_csv,
    dag=dag,
)

extract_xauusd_task = PythonOperator(
    task_id='extract_xauusd_data_from_csv',
    python_callable=extract_xauusd_data_from_csv,
    dag=dag,
)

extract_usdvnd_task = PythonOperator(
    task_id='extract_usdvnd_data_from_csv',
    python_callable=extract_usdvnd_data_from_csv,
    dag=dag,
)

consolidate_and_compare_task = PythonOperator(
    task_id='consolidate_and_compare_data',
    python_callable=consolidate_and_compare_data,
    dag=dag,
)
# Set the task dependencies
[invest_sjc_task, invest_xauusd_task, invest_usdvnd_task] >> [extract_sjc_task, extract_xauusd_task, extract_usdvnd_task] >> consolidate_and_compare_task
