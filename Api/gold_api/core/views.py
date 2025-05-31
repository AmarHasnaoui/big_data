import uuid
from django.contrib.auth.hashers import check_password, make_password
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from .models import User
import os
import csv
import datetime
from django.db import connection
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from .utils import token_required
from .models import QueryLog


@api_view(['POST'])
def login(request):
    username = request.data.get('username')
    password = request.data.get('password')

    if username != 'aminamar':
        return Response({'error': "Vous n'avez pas accès."}, status=403)

    # Création auto si nécessaire
    user, created = User.objects.get_or_create(username='aminamar', defaults={
        'password': make_password('admin123'),
    })

    if not check_password(password, user.password):
        return Response({'error': 'Mot de passe incorrect'}, status=401)

    # Nouveau token
    user.token = uuid.uuid4()
    user.save()

    return Response({'message': 'Connexion réussie', 'token': str(user.token)}, status=200)



import logging
import os

# Configure le dossier et le fichier de logs
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "access.log")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

@api_view(['POST'])
@token_required
def run_sql_query(request):
    query = request.data.get("query")
    user = request.user  

    if not query:
        msg = f"Requête vide - User: {user.username}"
        logging.warning(msg)
        return Response({'error': 'Missing SQL query'}, status=400)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
        
        # Sauvegarde CSV
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"query_results/query_{timestamp}.csv"
        os.makedirs("query_results", exist_ok=True)
        with open(filename, "w", newline="", encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(results)
            
        return Response({
            "message": "Query executed and saved to CSV successfully.",
            "results": results,
            "file": filename,
        }, status=200)

        #  Log success
        logging.info(f"User: {user.username} | Query: {query} | Status: SUCCESS | File: {filename}")

    except Exception as e:
        logging.error(f"User: {user.username} | Query: {query} | Status: ERROR | Message: {str(e)}")
        return Response({'error': str(e)}, status=400)
