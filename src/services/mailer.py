import smtplib
from email.message import EmailMessage
import streamlit as st


def send_pdf_mail(to_email: str, pdf_bytes: bytes, filename: str):

    host = st.secrets.get("SMTP_HOST")
    port = int(st.secrets.get("SMTP_PORT", 587))
    user = st.secrets.get("SMTP_USER")
    password = st.secrets.get("SMTP_PASS")
    sender = st.secrets.get("MAIL_FROM", user)

    msg = EmailMessage()
    msg["Subject"] = "CBAM / ETS Raporu"
    msg["From"] = sender
    msg["To"] = to_email

    msg.set_content("Rapor ektedir.")

    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=filename,
    )

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(msg)
