import pandas as pd
import os
from src.config import MONGO_CONNECTION_STRING, MONGO_DB_NAME, MONGO_COLLECTION_NAME,  MONGO_COLLECTION_INPUT
from src.database.mongo_handler import MongoHandler
from src.processing.data_materialize import (
    calculate_descriptive_stats,
    calculate_weather_code_probabilities,
    forecast_temperature,
    plot_correlation_heatmap,
    plot_weather_code_distribution,
    plot_temperature_forecast
)

def main():
    """
    Função principal para executar o pipeline de análise de dados climáticos.
    1. Carrega dados do MongoDB.
    2. Realiza análises (estatísticas, probabilidade, previsão).
    3. Gera e salva gráficos.
    4. Salva as métricas calculadas em uma nova coleção no MongoDB.
    """
    print("--- Iniciando pipeline de análise de dados climáticos ---")

    # Garante que o diretório para salvar os gráficos exista
    output_dir = "output/charts"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # 1. CARREGAR DADOS DE ENTRADA
        with MongoHandler(MONGO_CONNECTION_STRING, MONGO_DB_NAME) as mongo:
            df_original = mongo.find_all(MONGO_COLLECTION_INPUT)
        
        if df_original.empty:
            print("❌ Processo interrompido: Nenhum dado encontrado na coleção de origem.")
            return

        # Converte a coluna 'dia' para datetime
        df_original['dia'] = pd.to_datetime(df_original['dia'])

        # 2. REALIZAR ANÁLISES E GERAR GRÁFICOS
        
        # Estatísticas Descritivas e Correlação
        stats_df = calculate_descriptive_stats(df_original.copy())
        
        # A correlação é apenas para visualização, então geramos o gráfico diretamente
        plot_correlation_heatmap(df_original, os.path.join(output_dir, "correlation_heatmap.png"))

        # Probabilidades dos Códigos de Clima
        probs_df = calculate_weather_code_probabilities(df_original.copy())
        plot_weather_code_distribution(probs_df, os.path.join(output_dir, "weather_code_distribution.png"))

        # Previsão de Temperatura para 7 dias
        forecast_df = forecast_temperature(df_original.copy(), days_to_predict=7)
        plot_temperature_forecast(df_original, forecast_df, os.path.join(output_dir, "temperature_forecast.png"))

        # 3. ESTRUTURAR E SALVAR MÉTRICAS NO MONGODB
        # Criamos documentos separados para cada tipo de métrica para maior clareza
        metrics_to_store = [
            {
                "metric_type": "descriptive_statistics",
                "data": stats_df.to_dict('records')
            },
            {
                "metric_type": "weather_code_probability",
                "data": probs_df.to_dict('records')
            },
            {
                "metric_type": "temperature_forecast",
                "data": forecast_df.to_dict('records')
            }
        ]
        
        # Convertendo a lista de documentos para um DataFrame para usar o método existente
        # Isso garante que cada item da lista se torne uma linha no DataFrame
        final_df_to_mongo = pd.DataFrame(metrics_to_store)
        
        # Conversão de timestamps para objetos que o MongoDB entende
        if 'dia_previsto' in forecast_df.columns:
            forecast_df['dia_previsto'] = forecast_df['dia_previsto'].astype(object).where(forecast_df['dia_previsto'].notna(), None)

        NEW_COLLECTION_NAME = "climate_metrics"
        with MongoHandler(MONGO_CONNECTION_STRING, MONGO_DB_NAME) as mongo:
            mongo.overwrite_collection(MONGO_COLLECTION_NAME, final_df_to_mongo)
            
        print(f"\n✅ Pipeline concluído! Métricas salvas em '{MONGO_COLLECTION_NAME}' e gráficos em '{output_dir}'.")

    except Exception as e:
        print(f"❌ Ocorreu um erro fatal durante a execução do pipeline: {e}")

if __name__ == "__main__":
    main()