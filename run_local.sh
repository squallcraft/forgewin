#!/bin/bash
# ForgeWin - Ejecutar en local
# Uso: ./run_local.sh

set -e
cd "$(dirname "$0")"

# Activar venv si existe
if [ -d "venv" ]; then
  source venv/bin/activate
fi

# Instalar dependencias si hace falta
pip install -q -r requirements.txt 2>/dev/null || true

# Comprobar .env
if [ ! -f .env ]; then
  echo "⚠️  No existe .env. Copiando desde .env.example..."
  cp .env.example .env
  echo "   Edita .env y añade tus API keys (XAI_API_KEY, GEMINI_API_KEY, etc.)"
  echo ""
fi

echo "🚀 Iniciando ForgeWin en http://localhost:8501"
echo "   Pulsa Ctrl+C para detener"
echo ""
streamlit run app.py --server.headless true
