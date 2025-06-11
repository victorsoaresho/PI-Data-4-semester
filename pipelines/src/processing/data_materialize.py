import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- Funções de Plotagem ---

def plot_correlation_heatmap(df: pd.DataFrame, output_path: str):
    """Gera e salva um heatmap da matriz de correlação."""
    print("🎨 Gerando gráfico de correlação...")
    plt.figure(figsize=(10, 8))
    # Seleciona apenas colunas numéricas para a correlação
    numeric_df = df.select_dtypes(include=np.number)
    sns.heatmap(numeric_df.corr(), annot=True, cmap='coolwarm', fmt=".2f")
    plt.title('Matriz de Correlação das Variáveis Climáticas')
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    print(f"✅ Gráfico de correlação salvo em: {output_path}")

def plot_weather_code_distribution(df: pd.DataFrame, output_path: str):
    """Gera e salva um gráfico de barras da distribuição dos códigos de clima."""
    print("🎨 Gerando gráfico de distribuição de clima...")
    plt.figure(figsize=(10, 8))
    # Ordena as probabilidades para melhor visualização
    plot_data = df.sort_values('probabilidade_percent', ascending=False)
    sns.barplot(x='probabilidade_percent', y='codigo_clima', data=plot_data, orient='h', palette='viridis')
    plt.title('Probabilidade de Ocorrência por Código de Clima')
    plt.xlabel('Probabilidade (%)')
    plt.ylabel('Código do Clima')
    # Adiciona os valores nas barras
    for index, value in enumerate(plot_data['probabilidade_percent']):
        plt.text(value, index, f' {value:.2f}%')
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    print(f"✅ Gráfico de distribuição salvo em: {output_path}")

def plot_temperature_forecast(hist_df: pd.DataFrame, forecast_df: pd.DataFrame, output_path: str):
    """Gera e salva um gráfico da série histórica com a previsão de temperatura."""
    print("🎨 Gerando gráfico de previsão de temperatura...")
    plt.figure(figsize=(12, 6))
    plt.plot(hist_df['dia'], hist_df['temperatura_max_c'], label='Temperatura Máxima Histórica', marker='o')
    plt.plot(forecast_df['dia_previsto'], forecast_df['temperatura_max_prevista_c'], label='Previsão', linestyle='--', marker='x', color='red')
    plt.title('Previsão de Temperatura Máxima')
    plt.xlabel('Data')
    plt.ylabel('Temperatura Máxima (°C)')
    plt.legend()
    plt.grid(True)
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    print(f"✅ Gráfico de previsão salvo em: {output_path}")

# --- Funções de Análise ---

def calculate_descriptive_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula estatísticas descritivas para colunas numéricas."""
    print("📊 Calculando estatísticas descritivas...")
    # Garante que colunas importantes sejam numéricas, tratando erros
    numeric_cols = ['temperatura_max_c', 'temperatura_min_c', 'umidade_media_percent', 'indice_uv_max']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove linhas onde as colunas numéricas principais são nulas para evitar erros nos cálculos
    df.dropna(subset=['temperatura_max_c', 'temperatura_min_c'], inplace=True)

    desc_stats = df[numeric_cols].describe().transpose()
    # Adiciona moda, mediana e assimetria
    desc_stats['mode'] = df[numeric_cols].mode().iloc[0]
    desc_stats['median'] = df[numeric_cols].median()
    desc_stats['skew'] = df[numeric_cols].skew()
    
    stats_df = desc_stats.reset_index().rename(columns={'index': 'metrica'})
    print("👍 Estatísticas calculadas.")
    return stats_df

def calculate_weather_code_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula a probabilidade de ocorrência de cada código de clima."""
    print("📊 Calculando probabilidades dos códigos de clima...")
    probs = df['codigo_clima'].value_counts(normalize=True) * 100
    probs_df = probs.reset_index()
    probs_df.columns = ['codigo_clima', 'probabilidade_percent']
    # Garante que o tipo de dado seja amigável para JSON/Mongo
    probs_df['codigo_clima'] = probs_df['codigo_clima'].astype(str)
    print("👍 Probabilidades calculadas.")
    return probs_df

def forecast_temperature(df: pd.DataFrame, days_to_predict: int = 7) -> pd.DataFrame:
    """Realiza uma previsão de temperatura máxima usando Regressão Linear."""
    print(f"📈 Gerando previsão de temperatura para os próximos {days_to_predict} dias...")
    
    df_copy = df.copy()
    df_copy['dia'] = pd.to_datetime(df_copy['dia'])
    
    # Prepara os dados para o modelo (dias ordinais)
    df_copy.sort_values('dia', inplace=True)
    df_copy.dropna(subset=['temperatura_max_c'], inplace=True)
    df_copy['dias_ordinais'] = (df_copy['dia'] - df_copy['dia'].min()).dt.days
    X = df_copy[['dias_ordinais']]
    y = df_copy['temperatura_max_c']
    
    model = LinearRegression()
    model.fit(X, y)
    
    last_ordinal_day = X['dias_ordinais'].max()
    future_ordinal_days = np.arange(last_ordinal_day + 1, last_ordinal_day + 1 + days_to_predict).reshape(-1, 1)
    
    future_predictions = model.predict(future_ordinal_days)
    
    last_date = df_copy['dia'].max()
    future_dates = [last_date + pd.Timedelta(days=i) for i in range(1, days_to_predict + 1)]
    
    forecast_df = pd.DataFrame({
        'dia_previsto': future_dates,
        'temperatura_max_prevista_c': np.round(future_predictions, 2)
    })
    
    print("👍 Previsão gerada.")
    return forecast_df