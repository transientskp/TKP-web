from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns(
    '',
    url(r'^database/', include('tkpweb.apps.database.urls', namespace='database')),
    url(r'^dataset/', include('tkpweb.apps.dataset.urls', namespace='dataset')),
    url(r'^account/', include('tkpweb.apps.account.urls', namespace='account')),
    # url(r'^admin/', include(admin.site.urls)),
    url(r'^', include('tkpweb.apps.main.urls', namespace='main')),
)
