import pandas as pd
from yahooquery import Ticker

def download_b3_quotes(symbols, start_date, end_date):
    """
    Baixa o histórico de cotações de uma ou mais ações na B3 (Bovespa) usando a API do Yahoo Finance.

    Args:
        symbols (list): Lista de símbolos das ações na B3.
        start_date (str): Data de início no formato 'YYYY-MM-DD'.
        end_date (str): Data de término no formato 'YYYY-MM-DD'.

    Returns:
        DataFrame: DataFrame contendo o histórico de cotações para cada ação.
    """
    # Criando um dicionário para armazenar os DataFrames de histórico de cotações
    historical_quotes = {}

    # Iterando sobre cada símbolo de ação na lista de símbolos
    for symbol in symbols:
        # Criando um objeto Ticker com o símbolo da ação
        ticker = Ticker(symbol + '.SA')

        # Obtendo os dados do histórico de cotações para o símbolo atual
        data = ticker.history(start=start_date, end=end_date)

        # Renomeando as colunas conforme especificado
        data = data.rename(columns={'date':'data','open': 'abertura', 'high': 'alta', 'low': 'baixa',
                                     'close': 'fechamento', 'volume': 'volume', 'adjclose': 'adjclose',
                                     'dividends': 'dividendo'})

        # Adicionando o DataFrame do histórico de cotações ao dicionário
        historical_quotes[symbol] = data

    return historical_quotes

# Símbolos das ações na B3
symbols = ['TOTS3', 'BPAC11', 'MRVE3']

# Data de início e data de término do histórico de cotações
start_date = '2024-03-18'
end_date = '2024-03-18'

# Baixando o histórico de cotações para as ações especificadas
historical_quotes = download_b3_quotes(symbols, start_date, end_date)

# Concatenando os DataFrames em um único DataFrame
concatenated_quotes = pd.concat(historical_quotes.values())

# Convertendo o índice para a coluna 'date' no formato 'YYYY-MM-DD'
concatenated_quotes['date'] = concatenated_quotes.index.get_level_values(1).strftime('%Y-%m-%d')

# Resetando o índice
concatenated_quotes.reset_index(drop=True, inplace=True)

# Reordenando as colunas
concatenated_quotes = concatenated_quotes[['symbol', 'date', 'abertura', 'alta', 'baixa', 'fechamento', 'volume', 'adjclose', 'dividendo']]

# Exibindo o DataFrame concatenado
print(concatenated_quotes)

# Salvando o DataFrame em um arquivo CSV
concatenated_quotes.to_csv('historico_cotacoes.csv', index=False)
