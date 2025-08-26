# config/settings.py
import os
from decouple import config

class RedTideSettings:
    """Configuration settings for Red Tide automation"""
    
    # WordPress Settings
    WORDPRESS_SITE_URL = config('WORDPRESS_SITE_URL', default='')
    WORDPRESS_USERNAME = config('WORDPRESS_USERNAME', default='')
    WORDPRESS_APP_PASSWORD = config('WORDPRESS_APP_PASSWORD', default='')
    
    # Google Sheets Settings
    GOOGLE_SERVICE_ACCOUNT = config('GOOGLE_SERVICE_ACCOUNT', default='')
    GOOGLE_SHEET_ID = config('GOOGLE_SHEET_ID', default='')
    
    # FWC API Settings
    FWC_HAB_API_URL = "https://atoll.floridamarine.org/arcgis/rest/services/FWC_GIS/OpenData_HAB/MapServer/9/query"
    FWC_REQUEST_TIMEOUT = 30
    FWC_MAX_RETRIES = 3
    
    # Rate Limiting
    SHEETS_API_DELAY = 1.5  # seconds between requests
    WORDPRESS_API_DELAY = 2.0  # seconds between requests
    MAX_CONCURRENT_REQUESTS = 3
    
    # Data Processing
    CONFIDENCE_THRESHOLD = 30  # minimum confidence score to publish
    MAX_SAMPLE_AGE_DAYS = 14   # ignore samples older than this
    DISTANCE_WEIGHTS = {
        1.0: 1.0,    # Full weight within 1 mile
        3.0: 0.7,    # 70% weight 1-3 miles  
        10.0: 0.4,   # 40% weight 3-10 miles
        float('inf'): 0.2  # 20% weight beyond 10 miles
    }
    
    # Status Thresholds (cells/L)
    STATUS_THRESHOLDS = {
        'safe': 0,
        'caution': 10000,
        'avoid': 100000
    }
    
    # Logging
    LOG_LEVEL = config('LOG_LEVEL', default='INFO')
    VERBOSE_LOGGING = config('VERBOSE_LOGGING', default=False, cast=bool)
    
    # Testing
    TEST_MODE = config('TEST_MODE', default=False, cast=bool)
    TEST_LIMIT = config('TEST_LIMIT', default=10, cast=int)