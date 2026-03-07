"""
Configuration management for the Cold Email System.
Reads from .env file and provides defaults.
"""
import os
import hashlib
from dotenv import load_dotenv

load_dotenv()

# ----- Paths -----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "leads.db")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# ----- SMTP -----
SMTP_HOST = os.getenv("SMTP_HOST", "mail.spacemail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").lower() == "true"

# ----- Sender -----
FROM_NAME = os.getenv("FROM_NAME", "Rayen")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)
PORTFOLIO_LINK = os.getenv("PORTFOLIO_LINK", "https://rayenlazizi.tech")

# ----- Google Places API -----
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

# ----- Unsubscribe -----
UNSUBSCRIBE_SECRET = os.getenv("UNSUBSCRIBE_SECRET", "clientengine-unsub-secret")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:5000")

# ----- Scoring Thresholds -----
MIN_QUALIFICATION_SCORE = 60
TIER_1_THRESHOLD = 90
TIER_2_THRESHOLD = 75

# ----- Email Delays -----
DELAY_MIN_SECONDS = int(os.getenv("DELAY_MIN_SECONDS", "60"))
DELAY_MAX_SECONDS = int(os.getenv("DELAY_MAX_SECONDS", "180"))

# ----- Sequence Timing (days between emails) -----
SEQUENCE_DELAYS = [0, 3, 4, 7, 7]  # Day 0, Day 3, Day 7, Day 14, Day 21

# ----- Spam Detection -----
SPAM_TRIGGER_WORDS = [
    "free", "act now", "limited time", "click here", "buy now",
    "urgent", "winner", "congratulations", "no obligation",
    "risk free", "100%", "guarantee", "credit card",
    "order now", "subscribe", "unsubscribe", "deal",
    "lowest price", "cash", "earn money", "make money",
]

# ----- Flask -----
SECRET_KEY = os.getenv("SECRET_KEY", "cold-email-system-secret-key-change-me")
DEBUG = os.getenv("DEBUG", "true").lower() == "true"
PORT = int(os.getenv("PORT", "5000"))

# ----- Pipeline Debug / Timeouts -----
PIPELINE_DEBUG = os.getenv("PIPELINE_DEBUG", "false").lower() == "true"
SEARCH_TIMEOUT_SECONDS = int(os.getenv("SEARCH_TIMEOUT_SECONDS", "900"))
DISCOVERY_TIMEOUT_SECONDS = int(os.getenv("DISCOVERY_TIMEOUT_SECONDS", "180"))
LEAD_TIMEOUT_SECONDS = int(os.getenv("LEAD_TIMEOUT_SECONDS", "45"))
LEAD_FETCH_CONNECT_TIMEOUT_SECONDS = float(os.getenv("LEAD_FETCH_CONNECT_TIMEOUT_SECONDS", "4"))
LEAD_FETCH_READ_TIMEOUT_SECONDS = float(os.getenv("LEAD_FETCH_READ_TIMEOUT_SECONDS", "7"))
SEARCH_MAX_RESULTS = int(os.getenv("SEARCH_MAX_RESULTS", "80"))

# ----- Business Types -----
BUSINESS_TYPES = {
    "restaurant": {
        "label": "Restaurant / Bar / Café",
        "keywords": ["ristorante", "restaurant", "bar", "café", "cafe", "pizzeria",
                     "trattoria", "osteria", "bistro", "pub", "taverna", "gelateria",
                     "pasticceria", "bakery", "food"],
        "high_value": True,
    },
    "hotel": {
        "label": "Hotel / Accommodation",
        "keywords": ["hotel", "albergo", "b&b", "bed and breakfast", "hostel",
                     "resort", "agriturismo", "guest house", "motel", "lodge",
                     "accommodation", "villa", "apartment rental"],
        "high_value": True,
    },
    "service": {
        "label": "Service Business (Salon, Gym, Clinic)",
        "keywords": ["salon", "salone", "parrucchiere", "barber", "spa", "gym",
                     "palestra", "clinic", "clinica", "dentist", "dentista",
                     "physio", "massage", "beauty", "estetica", "wellness",
                     "yoga", "pilates", "veterinario"],
        "high_value": True,
    },
    "ecommerce": {
        "label": "E-commerce / Retail",
        "keywords": ["shop", "negozio", "store", "boutique", "ecommerce",
                     "e-commerce", "retail", "fashion", "moda", "abbigliamento",
                     "jewelry", "gioielleria", "shoes", "calzature"],
        "high_value": True,
    },
    "b2b": {
        "label": "B2B / Professional Services",
        "keywords": ["consulting", "consulenza", "agency", "agenzia", "studio",
                     "law", "avvocato", "accounting", "commercialista",
                     "architect", "architetto", "engineering", "marketing",
                     "design", "software", "IT"],
        "high_value": False,
    },
    "local_service": {
        "label": "Local Services (Plumber, Contractor)",
        "keywords": ["plumber", "idraulico", "electrician", "elettricista",
                     "contractor", "impresa edile", "cleaning", "pulizie",
                     "mechanic", "meccanico", "painter", "imbianchino",
                     "gardener", "giardiniere", "mover", "traslochi",
                     "locksmith", "fabbro", "repair", "riparazione"],
        "high_value": False,
    },
}
