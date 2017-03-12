from django.conf.urls import url
from . import views

urlpatterns = [
	url(r'^$', views.index, name='index'),
    url(r'^pull_stream/',views.pull_stream, name = "pull_stream")
	#url(r'^search/$', views.pull_stream),
 ]