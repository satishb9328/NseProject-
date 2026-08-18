"""
URL configuration for MyProject2 project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView  # 🔴 ఇది కొత్తగా ఇంపోర్ట్ చేయండి

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # 🔴 హోమ్ పేజీ (Root URL) ఓపెన్ చేయగానే ఆటోమేటిక్ గా live-dashboard/ కి వెళ్తుంది
    path('', RedirectView.as_view(url='/live-dashboard/', permanent=False)),
    
    path('swing-ml/', include('Nse_Project.Swing_ML.urls')),
    path('live-dashboard/', include('Nse_Project.Live_dashboard.urls')),
]