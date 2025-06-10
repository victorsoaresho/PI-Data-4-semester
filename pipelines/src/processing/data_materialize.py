import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression


def calculate_descriptive_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula estatísticas descritivas para colunas de temperatura."""
    print("📊 Calculando estatísticas descritivas...")
    
    # Seleciona apenas as colunas de interesse
    temp_df = df[['temperatura_max_c', 'temperatura_min_c']]
    
    stats = {
        'Média': temp_df.mean(),
        'Moda': temp_df.mode().iloc[0], # Pega o primeiro valor da moda, caso haja mais de um
        'Mediana': temp_df.median(),
        'Desvio Padrão': temp_df.std()
    }
    
    stats_df = pd.DataFrame(stats).reset_index().rename(columns={'index': 'metrica_temperatura'})
    stats_df['tipo_analise'] = 'estatisticas_descritivas'
    
    print("👍 Estatísticas calculadas.")
    return stats_df

def forecast_temperature(df: pd.DataFrame, days_to_predict: int = 5) -> pd.DataFrame:
    """
    Realiza uma previsão de temperatura máxima para os próximos dias
    usando Regressão Linear.
    
    AVISO: Este é um modelo simples para fins de demonstração. A precisão
    com poucos dados é extremamente limitada.
    """
    print(f"📈 Gerando previsão de temperatura para os próximos {days_to_predict} dias...")
    
    df_copy = df.copy()
    df_copy['dia'] = pd.to_datetime(df_copy['dia'])
    
    # Prepara os dados para o modelo
    df_copy['dias_ordinais'] = (df_copy['dia'] - df_copy['dia'].min()).dt.days
    X = df_copy[['dias_ordinais']]
    y = df_copy['temperatura_max_c']
    
    # Treina o modelo de Regressão Linear
    model = LinearRegression()
    model.fit(X, y)
    
    # Cria os próximos dias para prever
    last_ordinal_day = X['dias_ordinais'].max()
    future_ordinal_days = np.arange(last_ordinal_day + 1, last_ordinal_day + 1 + days_to_predict).reshape(-1, 1)
    
    # Realiza a previsão
    future_predictions = model.predict(future_ordinal_days)
    
    # Formata o resultado em um DataFrame
    last_date = df_copy['dia'].max()
    future_dates = pd.to_datetime([last_date + pd.Timedelta(days=i) for i in range(1, days_to_predict + 1)])
    
    forecast_df = pd.DataFrame({
        'dia_previsto': future_dates,
        'temperatura_max_prevista_c': future_predictions.round(2)
    })
    
    forecast_df['tipo_analise'] = 'previsao_temperatura'
    
    print("👍 Previsão gerada.")
    return forecast_df
