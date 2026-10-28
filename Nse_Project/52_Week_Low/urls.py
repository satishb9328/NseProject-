from django.urls import path

from . import views

# NOTE: This app directory is named ``52_Week_Low`` (leading digit), so it is
# wired into the project via ``include('Nse_Project.52_Week_Low.urls')`` in the
# root ``Nse_Project/urls.py``. Keeping this urlconf *un-namespaced* lets the
# watchlist URL resolve without an application namespace.
urlpatterns = [
    path("watchlist/", views.watchlist_view, name="watchlist"),
]
