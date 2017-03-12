from django.db import models
from django.utils import timezone

# Create your models here.
class Tweet(models.Model):
    tweet = models.TextField()
    #lat =
    #lon =