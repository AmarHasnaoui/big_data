import uuid
from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager



class QueryLog(models.Model):
    token = models.CharField(max_length=255)
    query = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=10)  # 'succès' ou 'échec'
    error_message = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"{self.timestamp} | {self.status}"


class CustomUserManager(BaseUserManager):
    def create_user(self, username, password=None):
        if not username:
            raise ValueError('Username is required')
        user = self.model(username=username)
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, username, password=None):
        user = self.create_user(username, password)
        return user


class User(AbstractBaseUser):
    username = models.CharField(max_length=150, unique=True)
    password = models.CharField(max_length=128)
    token = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)

    objects = CustomUserManager()

    USERNAME_FIELD = 'username'
    REQUIRED_FIELDS = []

    def __str__(self):
        return self.username
    
    
class QueryLog(models.Model):
    token = models.CharField(max_length=255)
    query = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=10)  # 'succès' ou 'échec'
    error_message = models.TextField(blank=True, null=True)

    def __str__(self):
        return f"{self.timestamp} | {self.status}"
