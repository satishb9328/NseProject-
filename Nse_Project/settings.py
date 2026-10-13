"""
Django settings for MyProject2 project.
"""

from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-gh^o_^vogsp_l+80778wk!do0fr0)#$k$!n12plcg)h4^kyun_'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['*']

# Application definition
# Application definition
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

# NOTE: If your inner configuration folder is named 'Nse_Project' instead of 'MyProject2', 
# you will need to change 'MyProject2' to 'Nse_Project' in the two lines below!
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

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [
                {
                    "address": "redis://127.0.0.1:6379/0",
                    # PING an idle connection before reuse so a silently-closed
                    # socket is detected and replaced instead of timing out.
                    "health_check_interval": 30,
                    # MUST be > channels_redis's 5s BRPOP/BZPOPMIN poll timeout,
                    # otherwise the socket read races the server poll and raises
                    # redis.exceptions.TimeoutError ("Timeout reading from ...").
                    "socket_timeout": 10,
                    "socket_connect_timeout": 5,
                    "socket_keepalive": True,
                },
            ],
            # channels_redis defaults — kept explicit and self-documenting.
            "expiry": 60,
            "group_expiry": 86400,
            "capacity": 100,
        },
    },
}

# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

# Password validation
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

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = 'static/'