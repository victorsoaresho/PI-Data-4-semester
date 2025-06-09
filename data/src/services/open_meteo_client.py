import requests
from typing import Dict, Any

class OpenMeteoClient:
    """Classe para interagir com a API Open-Meteo."""
    def __init__(self, latitude: float, longitude: float):
        self._base_url = "https://api.open-meteo.com/v1/forecast"
        self.latitude = latitude
        self.longitude = longitude
        self.params = {
            "past_days": 90,
            "daily": "weathercode,temperature_2m_max,temperature_2m_min,relative_humidity_2m_mean,uv_index_max",
            "timezone": "America/Sao_Paulo"
        }

    def get_daily_forecast(self) -> Dict[str, Any] | None:
        """Busca os dados diários da API Open-Meteo."""
        print("Buscando dados da API Open-Meteo...")
        
        request_params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            **self.params 
        }
        
        try:
            response = requests.get(self._base_url, params=request_params)
            response.raise_for_status() 
            print("Dados recebidos com sucesso!")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados da API: {e}")
            return None