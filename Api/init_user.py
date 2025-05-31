import os
import django
from django.contrib.auth.hashers import make_password

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gold_api.settings')
django.setup()

from core.models import User

if not User.objects.filter(username='amineamar').exists():
    user = User.objects.create(
        username='amineamar',
        password=make_password('admin123'),
    )
    print(" Utilisateur créé avec token :", user.token)
else:
    print("ℹ Utilisateur déjà présent.")
