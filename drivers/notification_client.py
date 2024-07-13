# Send notification
import logging
import os
import smtplib

# Load environment variables
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SENDER_EMAIL = os.getenv('SENDER_EMAIL')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

def send_notification(playlist_name, change_in_count):
    logging.info(f"Sending notification for new tracks in {playlist_name}")
    sender_email = SENDER_EMAIL
    receiver_email = SENDER_EMAIL  # assuming you're sending the email to yourself
    password = EMAIL_PASSWORD
    subject = f"New Tracks in {playlist_name}"
    body = f"Change in number of tracks for {playlist_name}: {change_in_count}"
    message = f"Subject: {subject}\n\n{body}"
    message = message.encode('utf-8')

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)