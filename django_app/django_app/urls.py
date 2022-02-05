"""django_app URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from django.conf.urls import include, url

from . import views


api_v1 = [
    url("fetch", views.codeFeaturesSender.as_view(), name="fetch"),
    url("live", views.codeLiveFeaturesSender.as_view(), name="live"),
    url("update", views.globalFeaturesUpdater.as_view(), name="update"),
    url("all_codes", views.allCodesSender.as_view(), name="all_codes"),
    url("code_name", views.codeNameMapping.as_view(), name="code_name"),
    url("pool_feature", views.poolFeaturePicker.as_view(), name="pool_feature"),
    url("update_news", views.newsAffiliatedCodeUpdater.as_view(), name="update_news"),
    url(
        "affiliated_codes",
        views.newsAffiliatedCodeSender.as_view(),
        name="affiliated_codes",
    ),
]


urlpatterns = [
    # path('admin/', admin.site.urls),
    path("api_v1/", include(api_v1)),
]
