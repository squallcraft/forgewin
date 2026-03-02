"""
Envío de la propuesta en PDF por correo al usuario.
Usa variables de entorno: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, EMAIL_FROM (opcionales).
"""

import os
from typing import Tuple
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


def send_proposal_email(
    to_email: str,
    proposal_id: str,
    pdf_path: str,
    body: str = "",
) -> Tuple[bool, str]:
    """
    Envía el PDF de la propuesta al correo del usuario.
    Devuelve (éxito, mensaje).
    """
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    from_addr = os.getenv("EMAIL_FROM", user or "noreply@local").strip()

    if not host or not user or not password:
        return False, "Configura SMTP_HOST, SMTP_USER y SMTP_PASSWORD en .env para enviar correos."

    if not to_email or "@" not in to_email:
        return False, "Email del destinatario no válido."

    if not os.path.isfile(pdf_path):
        return False, "No se encuentra el archivo PDF."

    msg = MIMEMultipart()
    msg["Subject"] = f"Propuesta de análisis - {proposal_id}"
    msg["From"] = from_addr
    msg["To"] = to_email
    msg.attach(MIMEText(body or f"Adjunto propuesta de análisis (ID: {proposal_id}).", "plain", "utf-8"))

    with open(pdf_path, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="pdf")
        part.add_header("Content-Disposition", "attachment", filename=Path(pdf_path).name)
        msg.attach(part)

    try:
        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(user, password)
            server.sendmail(from_addr, [to_email], msg.as_string())
        return True, "Correo enviado correctamente."
    except Exception as e:
        return False, str(e)
