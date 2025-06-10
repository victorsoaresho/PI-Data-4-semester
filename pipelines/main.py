from src.config import (
    MONGO_CONNECTION_STRING, MONGO_DB_NAME, MONGO_COLLECTION_NAME
)
from src.database.mongo_handler import MongoHandler
from src.processing.data_materialize import calculate_descriptive_stats, forecast_temperature

import pandas as pd

def main():
    """Função principal para executar a análise e salvar no MongoDB."""
    
    # 1. DADOS DE ENTRADA (O SEU DATAFRAME)
    with MongoHandler(MONGO_CONNECTION_STRING, MONGO_DB_NAME) as mongo:
        data = mongo.find_all(MONGO_COLLECTION_NAME)
    
    df_original = pd.DataFrame(data)
    df_original['dia'] = pd.to_datetime(df_original['dia'])

    # 2. CÁLCULO E PREVISÃO
    stats_df = calculate_descriptive_stats(df_original)
    forecast_df = forecast_temperature(df_original, days_to_predict=5)

    # 3. COMBINA OS RESULTADOS PARA EXPORTAÇÃO
    # pd.concat é a forma idiomática de unir DataFrames.
    # O resultado é o mesmo que o método original, mas o código fica mais limpo.
    final_df_to_mongo = pd.concat([stats_df, forecast_df], ignore_index=True)
    
    # --- INÍCIO DA CORREÇÃO ---
    # O erro ocorre porque as linhas de estatísticas não têm 'dia_previsto',
    # resultando em valores NaT (Not a Time) nessa coluna.
    # Convertemos NaT para None, que o PyMongo sabe como lidar (salva como null).
    if 'dia_previsto' in final_df_to_mongo.columns:
        final_df_to_mongo['dia_previsto'] = final_df_to_mongo['dia_previsto'].astype(object).where(
            final_df_to_mongo['dia_previsto'].notna(), None
        )
    # --- FIM DA CORREÇÃO ---

    NEW_COLLECTION_NAME = "climate_metrics"

    try:
        with MongoHandler(MONGO_CONNECTION_STRING, MONGO_DB_NAME) as mongo:
            # O método overwrite_collection agora recebe um DataFrame sem valores NaT
            mongo.overwrite_collection(NEW_COLLECTION_NAME, final_df_to_mongo)
            print(f"✅ Dados salvos com sucesso na coleção: {NEW_COLLECTION_NAME}")
            
    except Exception as e:
        print(f"❌ Ocorreu um erro durante a execução: {e}")


if __name__ == "__main__":
    # Para rodar o script, você precisa instalar as dependências:
    # pip install pandas "pymongo[srv]" scikit-learn
    main()