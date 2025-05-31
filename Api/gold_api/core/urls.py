from django.urls import path
from .views import login
from .views import run_sql_query


urlpatterns = [
    path('login/', login),
    path('query/', run_sql_query),
]
