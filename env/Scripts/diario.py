import pandas as pd
from yahooquery import Ticker
import datetime
import schedule
import time
from flask import Flask, request, jsonify
from flask_mysqldb import MySQL
from sqlalchemy import create_engine, text
import requests

app = Flask(__name__)

# Configurações do banco de dados MySQL
engine = create_engine('mysql+mysqlconnector://root:@localhost/basefinance')

mysql = MySQL(app)

with engine.connect() as connection:
    # Verificar a conexão com o banco de dados
    if connection:
        print("Conexão com o banco de dados 'basefinance' estabelecida com sucesso.")
    else:
        print("Erro: Não foi possível estabelecer a conexão com o banco de dados 'basefinance'.")

def download_b3_quotes(symbols, start_date, end_date):
    """
    Baixa o histórico de cotações de uma ou mais ações na B3 (Bovespa) usando a API do Yahoo Finance.

    Args:
        symbols (list): Lista de símbolos das ações na B3.
        start_date (str): Data de início no formato 'YYYY-MM-DD'.
        end_date (str): Data de término no formato 'YYYY-MM-DD'.

    Returns:
        dict: Dicionário contendo os históricos de cotações para cada ação.
    """
    # Criando um dicionário para armazenar os DataFrames de histórico de cotações
    historical_quotes = {}

    # Iterando sobre cada símbolo de ação na lista de símbolos
    for symbol in symbols:
        # # Criando um objeto Ticker com o símbolo da ação
        ticker = Ticker(symbol + '.SA')

        # Obtendo os dados do histórico de cotações para o símbolo atual
        data = ticker.history(start=start_date, end=end_date)

        # Renomeando as colunas conforme especificado
        data = data.rename(columns={'date':'data','open': 'abertura', 'high': 'alta', 'low': 'baixa',
                                     'close': 'fechamento', 'volume': 'volume', 'adjclose': 'adjclose',
                                     'dividends': 'dividendo'})

        # Adicionando o DataFrame do histórico de cotações ao dicionário
        historical_quotes[symbol] = data.to_dict(orient='records')
    print(historical_quotes)
    return historical_quotes
    


def inserir_dados(symbol, quotes):
    # Itera sobre os dados e insere no banco de dados
    #print(quotes)
    for quote in quotes:
        cur = mysql.connection.cursor()
        cur.execute("INSERT INTO tabela (date, open, high, low, close, volume, adjclose, dividends) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (quote['date'], quote['abertura'], quote['alta'], quote['baixa'], quote['fechamento'], quote['volume'], quote['adjclose'], quote['dividendo']))
        mysql.connection.commit()
        cur.close()
    
    return 'Dados inseridos com sucesso'

def job():
    # Verifica se o dia atual é sábado ou domingo
    today = datetime.datetime.now().weekday()
    if today == 5 or today == 6:
        print("Não é um dia útil. Dados não serão baixados.")
        return
    
    # Símbolos das ações na B3
    symbols = ['TOTS3', 'BPAC11', 'MRVE3']

    # Data de início e data de término do histórico de cotações (hoje)
    start_date = '2024-03-18'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # Baixando o histórico de cotações para as ações especificadas
    historical_quotes = download_b3_quotes(symbols, start_date, end_date)

    # Enviando os dados para a API Flask
    for symbol, quotes in historical_quotes.items():
        response = inserir_dados(symbol, quotes)
        print(response)

@app.route('/api/inserir_dados', methods=['POST'])
def api_inserir_dados():
    data = request.json  # Dados enviados para a API
    symbol = data['symbol']
    quotes = data['quotes']
    
    response = inserir_dados(symbol, quotes)
    return response

if __name__ == '__main__':
    # Agendando a execução do job diariamente às 20h
    schedule.every().day.at("20:21").do(job)

    # Loop principal para executar a agenda
    while True:
        schedule.run_pending()
        time.sleep(60)  # Espera 60 segundos antes de verificar novamente
