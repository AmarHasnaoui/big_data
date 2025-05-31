from rest_framework.response import Response
from rest_framework import status
from .models import User
from functools import wraps

def token_required(view_func):
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        auth_header = request.headers.get('Authorization')

        if not auth_header or not auth_header.startswith('Bearer '):
            return Response({'error': 'Missing or invalid Authorization header'}, status=status.HTTP_401_UNAUTHORIZED)

        token = auth_header.split(' ')[1]

        try:
            user = User.objects.get(token=token)
            request.user = user
        except User.DoesNotExist:
            return Response({'error': 'Invalid or expired token'}, status=status.HTTP_401_UNAUTHORIZED)

        return view_func(request, *args, **kwargs)
    return wrapper
