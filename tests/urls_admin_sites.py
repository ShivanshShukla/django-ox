"""
Two extra admin sites beside the default one: `ops` registers OxTaskAdmin,
`plain` registers the task model with a plain ModelAdmin, the way a project
that brings its own admin class does.
"""

from django.contrib import admin
from django.urls import path

from django_ox.admin import OxTaskAdmin
from django_ox.models import OxTask

ops_site = admin.AdminSite(name="ops")
ops_site.register(OxTask, OxTaskAdmin)

plain_site = admin.AdminSite(name="plain")
plain_site.register(OxTask)

urlpatterns = [
    path("admin/", admin.site.urls),
    path("ops/", ops_site.urls),
    path("plain/", plain_site.urls),
]
