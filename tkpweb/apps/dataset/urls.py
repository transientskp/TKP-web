from django.conf.urls.defaults import patterns, include, url
from .views import DatasetsView
from .views import DatasetView
from .views import ImagesView
from .views import ImageView
from .views import ImagePlotView
from .views import ExtractedSourcesView
from .views import ExtractedSourceView
from .views import SourceLightcurveView
from .views import SourcesView
from .views import SourceView
from .views import TransientLightcurveView
from .views import TransientsView
from .views import TransientView
from .views import MonitoringListView
from .views import TransientLightsurfaceView


urlpatterns = patterns(
   'tkpweb.apps.dataset.views',
   url(r'^(?P<dataset>\d+)/monitoringlist/$', view=MonitoringListView.as_view(), name='monitoringlist'),
   url(r'^(?P<dataset>\d+)/image/(?P<id>\d+)/image$', view=ImagePlotView.as_view(), name='image-single'),
   url(r'^(?P<dataset>\d+)/image/(?P<id>\d+)/$', view=ImageView.as_view(), name='image'),
   url(r'^(?P<dataset>\d+)/image/$', view=ImagesView.as_view(), name='images'),
   url(r'^(?P<dataset>\d+)/transient/(?P<id>\d+)/lightcurve/$', view=TransientLightcurveView.as_view(), name='transient-lightcurve'),
   url(r'^(?P<dataset>\d+)/transient/(?P<id>\d+)/lightsurface/$', view=TransientLightsurfaceView.as_view(), name='transient-lightsurface'),
   url(r'^(?P<dataset>\d+)/transient/(?P<id>\d+)/$', view=TransientView.as_view(), name='transient'),
   url(r'^(?P<dataset>\d+)/transient/$', view=TransientsView.as_view(), name='transients'),
   url(r'^(?P<dataset>\d+)/source/(?P<runcat>\d+)/lightcurve/$', view=SourceLightcurveView.as_view(), name='source-lightcurve'),
   url(r'^(?P<dataset>\d+)/source/(?P<runcat>\d+)/$', view=SourceView.as_view(), name='source'),
   url(r'^(?P<dataset>\d+)/source/$', view=SourcesView.as_view(), name='sources'),
   url(r'^(?P<dataset>\d+)/extractedsource/(?P<id>\d+)/$', view=ExtractedSourceView.as_view(), name='extractedsource'),
   url(r'^(?P<dataset>\d+)/extractedsource/$', view=ExtractedSourcesView.as_view(), name='extractedsources'),
   url(r'^(?P<id>\d+)/$', view=DatasetView.as_view(), name='dataset'),
   url(r'^$', view=DatasetsView.as_view(), name='datasets'),
   )
