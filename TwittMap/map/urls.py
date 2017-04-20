from django.conf.urls import url
from . import views

urlpatterns = [
	url(r'^$', views.index, name='index'),
    url(r'^pull_stream/',views.pull_stream, name = "pull_stream"),
    url(r'^sns_test_handler/', views.sns_test_handler, name = "sns_test_handler"),
    url(r'^poll_data/', views.poll_data, name = "poll_data")
 ]