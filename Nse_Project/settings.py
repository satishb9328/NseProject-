"""
Django settings for MyProject2 project.
"""

import os
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# ==========================================
# SECURITY SETTINGS (Dynamic for Render/Local)
# ==========================================
# Uses the secret key from Render if available, otherwise uses the local default
SECRET_KEY = os.getenv('SECRET_KEY', 'django-insecure-gh^o_^vogsp_l+80778wk!do0fr0)#$k$!n12plcg)h4^kyun_')

# DEBUG is True locally, but can be turned off in Render by setting Environment Variable DEBUG=False
DEBUG = os.getenv('DEBUG', 'True') == 'True'

ALLOWED_HOSTS = ['*']


# ==========================================
# APPLICATION DEFINITION
# ==========================================
INSTALLED_APPS = [
    'daphne',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'channels',
    'Nse_Project.Preopen',
    'Nse_Project.Swing_ML.apps.SwingMlConfig',
    'Nse_Project.Live_dashboard',
    'Nse_Project.GAMMA.apps.GammaConfig',
    'Nse_Project.funda_fetch',
]

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'swing_ml_cache',
    }
}

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'Nse_Project.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'Nse_Project' / 'Master_Temp'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'Nse_Project.funda_fetch.context_processors.sidebar_scanner_counts',
            ],
        },
    },
]

WSGI_APPLICATION = 'Nse_Project.wsgi.application'
ASGI_APPLICATION = 'Nse_Project.asgi.application'


# ==========================================
# WEBSOCKETS & REDIS CONFIGURATION
# ==========================================
# Dynamically fetches the Redis URL from Render, or uses localhost for local machine
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [
                {
                    "address": REDIS_URL,
                    "health_check_interval": 30,
                    "socket_timeout": 10,
                    "socket_connect_timeout": 5,
                    "socket_keepalive": True,
                },
            ],
            "expiry": 60,
            "group_expiry": 86400,
            "capacity": 100,
        },
    },
}


# ==========================================
# DATABASE SETTINGS
# ==========================================
# Note: SQLite data resets on every Render deployment.
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}


# ==========================================
# PASSWORD VALIDATION
# ==========================================
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# ==========================================
# INTERNATIONALIZATION
# ==========================================
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True


# ==========================================
# STATIC FILES (CSS, JavaScript, Images)
# ==========================================
STATIC_URL = 'static/'
# STATIC_ROOT is required by Render to collect and serve static files properly
STATIC_ROOT = BASE_DIR / 'staticfiles'