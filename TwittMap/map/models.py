from __future__ import unicode_literals

from django.db import models

class New_Tweets(models.Model):
	id = models.CharField(max_length=255, primary_key=True)
	tweet = models.CharField(max_length=255)
	lat = models.FloatField()
	lon = models.FloatField()
	score = models.FloatField()
	sentiment = models.CharField(max_length=50)
	
	class Meta:
		managed = False
		db_table = "new_tweets"                        