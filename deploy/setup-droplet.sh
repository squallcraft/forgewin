#!/bin/bash
# Script de configuración inicial del Droplet ForgeWin
# Ejecutar como root: bash setup-droplet.sh

set -e

echo "=== ForgeWin Droplet Setup ==="

# 1. Sistema
apt update && apt upgrade -y
apt install -y python3 python3-pip python3-venv git nginx certbot python3-certbot-nginx

# 2. Usuario de la app
id -u forgewin &>/dev/null || useradd --system --create-home --shell /bin/bash forgewin
mkdir -p /opt/forgewin
chown forgewin:forgewin /opt/forgewin

# 3. Clonar repositorio
sudo -u forgewin git clone https://github.com/squallcraft/forgewin.git /opt/forgewin || \
    (cd /opt/forgewin && sudo -u forgewin git pull)

# 4. Entorno virtual y dependencias
cd /opt/forgewin
sudo -u forgewin python3 -m venv venv
sudo -u forgewin venv/bin/pip install --upgrade pip
sudo -u forgewin venv/bin/pip install -r requirements.txt

# 5. Logs
mkdir -p /opt/forgewin/logs
chown forgewin:forgewin /opt/forgewin/logs

# 6. Servicios systemd
cp deploy/forgewin-app.service /etc/systemd/system/
cp deploy/forgewin-webhook.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable forgewin-app forgewin-webhook

echo ""
echo "=== SIGUIENTE PASO ==="
echo "1. Crea /opt/forgewin/.env con tus variables de entorno"
echo "2. Edita /etc/nginx/sites-available/forgewin con tu dominio"
echo "3. Ejecuta: systemctl start forgewin-app forgewin-webhook"
echo "4. Ejecuta: certbot --nginx -d tudominio.com -d www.tudominio.com"
