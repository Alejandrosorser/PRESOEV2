# -*- coding: utf-8 -*-
"""
PRESOE · Actualización automática de datos

Este script se encarga de:
1. Buscar información actualizada sobre los casos de corrupción del PSOE
2. Actualizar el archivo datos.js con la nueva información
3. Hacer un commit y push de los cambios

Se ejecuta automáticamente cada día a través de GitHub Actions.
"""
import json
import requests
from datetime import datetime

# Constantes
DATOS_JS_PATH = "../datos.js"  # Ruta al archivo datos.js
GOOGLE_NEWS_API_KEY = "TU_API_KEY"  # Reemplaza con tu API key de Google News
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]  # La clave de la API de Anthropic se configura como secreto en el repositorio

def actualiza_datos():
    # Cargar el JSON actual de datos.js
    with open(DATOS_JS_PATH, "r") as f:
        datos = json.load(f)

    # Buscar noticias actualizadas
    noticias = busca_noticias()

    # Actualizar datos con las nuevas noticias
    datos = actualiza_datos_con_noticias(datos, noticias)

    # Guardar el JSON actualizado en datos.js
    with open(DATOS_JS_PATH, "w") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)

    # Hacer commit y push de los cambios
    subprocess.run(["git", "add", DATOS_JS_PATH])
    subprocess.run(["git", "commit", "-m", f"Actualiza datos - {datetime.now().strftime('%Y-%m-%d')}"])
    subprocess.run(["git", "push"])

    print(f"Datos actualizados a {datetime.now().strftime('%Y-%m-%d')}")

def busca_noticias():
    # Usa la API de Google News para buscar noticias recientes sobre los casos
    url = f"https://newsapi.org/v2/everything?q=PSOE+corrupción&apiKey={GOOGLE_NEWS_API_KEY}&language=es&sortBy=publishedAt"
    response = requests.get(url)
    response.raise_for_status()

    # Analiza las noticias con la API de Anthropic para extraer información relevante
    prompts = [f"Extrae los datos relevantes sobre casos de corrupción del PSOE de esta noticia:\n\n{noticia['title']}\n\n{noticia['description']}" for noticia in response.json()["articles"]]
    respuestas = [anthropic_completion(prompt, model="claude-v1.3") for prompt in prompts]

    # Formatea la información extraída
    noticias = [{"titulo": noticia["title"], "fecha": noticia["publishedAt"], "datos": respuesta} for noticia, respuesta in zip(response.json()["articles"], respuestas)]

    return noticias

def actualiza_datos_con_noticias(datos, noticias):
    # Itera por las noticias y actualiza los datos según corresponda
    # (Lógica para integrar los nuevos datos en el JSON existente)
    # ...

    return datos

def anthropic_completion(prompt, model="claude-v1.3"):
    # Wrapper para la API de completions de Anthropic
    headers = {"Content-Type": "application/json", "X-API-Key": ANTHROPIC_API_KEY}
    payload = {"prompt": prompt, "model": model, "max_tokens_to_sample": 1000}
    response = requests.post("https://api.anthropic.com/v1/complete", json=payload, headers=headers)
    response.raise_for_status()

    return response.json()["completion"]

if __name__ == "__main__":
    actualiza_datos()
