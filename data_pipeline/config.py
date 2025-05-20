"""
Configuration settings for the Census data pipeline
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Census API configuration
BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("CENSUS_API_KEY")

# Database configuration
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Data refresh settings
REFRESH_INTERVAL_DAYS = 30  # How often to refresh the data

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "logs/census_pipeline.log")

# Notification settings
NOTIFICATION_EMAIL = os.getenv("NOTIFICATION_EMAIL", "")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "")