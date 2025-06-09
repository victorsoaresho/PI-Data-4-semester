import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env para o ambiente do sistema
# É crucial chamar esta função antes de tentar acessar as variáveis
load_dotenv()

# --- Configuração do MongoDB ---
# Agora, em vez de escrever o valor aqui, pegamos da variável de ambiente
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# --- Configuração da API Open-Meteo ---
# Valores que não são secretos podem continuar aqui ou ir para o .env também
FRANCA_LATITUDE = -20.53
FRANCA_LONGITUDE = -47.40

# Verificação para garantir que as variáveis essenciais foram carregadas
if not MONGO_CONNECTION_STRING:
    raise ValueError("A variável de ambiente MONGO_CONNECTION_STRING não foi definida. Crie um arquivo .env.")