from django.views.generic import View
from django.views.generic import TemplateView
from django.views.generic.edit import FormMixin
from django.core.urlresolvers import reverse
from django.http import Http404
from django.http import HttpResponse
from django.http import HttpResponseForbidden
from django.http import HttpResponseRedirect
from django.shortcuts import redirect
from .tools import dbase
from .tools import plot
from .tools import quality
from .forms import MonitoringListForm
from tkp.database import DataBase

import tkp.database.utils as tkpdbutils
from tkp.classification.transient import Transient
from tkp.classification.transient import Position
from scipy.stats import chisqprob
import numpy
import datetime


class BaseView(TemplateView):

    def get_context_data(self, **kwargs):
        context = super(BaseView, self).get_context_data(**kwargs)
        self.database = self.get_database(self.request.session.get('dblogin', None))
        return context

    def get_database(self, dblogin=None):
        try:
            return self.database
        except AttributeError:
            return dbase.DataBase(dblogin=dblogin)

    def set_template(self, name):
        format = self.request.GET.get('format', 'html')
        if not format in ['html', 'csv']:
            raise Http404
        self.template_name = "dataset/%s.%s" % (name, format)


class DatasetsView(BaseView):
    def get_context_data(self, **kwargs):
        """List all available datasets, together with a bit of
        extra information"""
        context = super(DatasetsView, self).get_context_data(**kwargs)
        self.set_template('datasets')
        context['datasets'] = self.database.dataset(extra_info=["ntransients"])
        return context


class DatasetView(BaseView):
    def get_context_data(self, **kwargs):
        """List details for single dataset"""

        context = super(DatasetView, self).get_context_data(**kwargs)
        self.set_template('dataset')
        dsid = int(kwargs['id'])
        dataset = self.database.dataset(id=dsid, extra_info=[
            "ntransients", "nimages", "nsources", "ntotalsources"])
        if not dataset:
            raise Http404
        else:
            dataset = dataset[0]
        context['dataset'] = dataset
        context['rmsplot'] = quality.plot_rms_distance_from_fieldcentre(
            self.database, dsid)
        context['histimageplot'] = quality.HistSourcesPerImagePlot().render(
            self.database, dsid)
        context['scattallplot'] = quality.ScatterPosAllCounterpartsPlot().render(
            self.database, dsid)

        return context


class ImagesView(BaseView):
    def get_context_data(self, **kwargs):
        context = super(ImagesView, self).get_context_data(**kwargs)
        self.set_template('images')
        context['images'] = self.database.image(dataset=kwargs['dataset'], extra_info=['ntotalsources', 'reject'])
        context['dataset'] = self.database.dataset(id=kwargs['dataset'])[0]
        return context


class ImageView(BaseView):
    def get_context_data(self, **kwargs):
        context = super(ImageView, self).get_context_data(**kwargs)
        self.set_template('image')
        image = self.database.image(id=kwargs['id'], dataset=kwargs['dataset'], extra_info=['ntotalsources'])
        if not image:
            raise Http404
        else:
            image = image[0]
        image['png'] = plot.ImagePlot().render(image, database=self.database)
        dataset = self.database.dataset(id=kwargs['dataset'])[0]
        extractedsources = self.database.extractedsource(image=image['id'])


        image['extractedsources'] = plot.ImagePlot().render(image, plotsources=extractedsources)
        context['image'] = image
        context['extractedsources'] = extractedsources
        context['dataset'] = dataset
        return context



class TransientsView(BaseView):
    def get_context_data(self,  **kwargs):
        context = super(TransientsView, self).get_context_data(**kwargs)
        self.set_template('transients')
        dataset = kwargs['dataset']
        try:
            context['dataset'] = self.database.dataset(id=dataset)[0]
        except IndexError:
            raise Http404
        context['transients'] = self.database.transient(dataset=dataset)
        return context


class TransientView(BaseView):
    def get_context_data(self, **kwargs):
        context = super(TransientView, self).get_context_data(**kwargs)
        self.set_template('transient')
        id = kwargs['id']
        dataset = kwargs['dataset']
        transient = self.database.transient(id=id, dataset=dataset)
        if not transient:
            raise Http404
        else:
            transient = transient[0]
        images = self.database.image_times(dataset=dataset)
        lightcurve = self.database.lightcurve(int(transient['trigger_xtrsrc']))
        trigger_index = [i for i, lc in enumerate(lightcurve)
                         if lc[4] == transient['trigger_xtrsrc']][0]
        context['lightcurve'] = {
            'plot': plot.LightcurvePlot().render(
                lightcurve, images=images, trigger_index=trigger_index),
            'data': lightcurve
            }
        context['dataset'] = self.database.dataset(id=dataset)[0]
        context['transient'] = transient
        context['lightcurve']['thumbnails'] = []
        for point in context['lightcurve']['data']:
            ra, dec, filename = self.database.thumbnail(point[4])
            context['lightcurve']['thumbnails'].append(
                plot.ThumbnailPlot(size=(4, 4)).render(filename, (ra, dec)))
        return context


class SourcesView(BaseView):
    def get_context_data(self, **kwargs):
        context = super(SourcesView, self).get_context_data(**kwargs)
        self.set_template('sources')
        context['sources'] = self.database.source(dataset=kwargs['dataset'])
        context['dataset'] = self.database.dataset(id=kwargs['dataset'])[0]
        return context


class SourceView(BaseView):
    def get_context_data(self, **kwargs):
        context = super(SourceView, self).get_context_data(**kwargs)
        self.set_template('source')
        source = self.database.source(runcat=kwargs['runcat'], dataset=kwargs['dataset'])
        if not source:
            raise Http404
        else:
            source = source[0]
        images = self.database.image_times(dataset=kwargs['dataset'])
        lightcurve = self.database.lightcurve(int(source['xtrsrc']))
        context['lightcurve'] = {
            'plot': plot.LightcurvePlot().render(lightcurve, images=images),
            'data': lightcurve
            }
        context['source'] = source
        context['dataset'] = self.database.dataset(id=kwargs['dataset'])[0]
        return context


class ExtractedSourcesView(BaseView):
    def get_context_data(self, **kwargs):
        context = super(ExtractedSourcesView, self).get_context_data(**kwargs)
        self.set_template('extractedsources')
        context['extractedsources'] = self.database.extractedsource(dataset=kwargs['dataset'])
        context['dataset'] = self.database.dataset(id=kwargs['dataset'])[0]
        return context


class ExtractedSourceView(BaseView):
    def get_context_data(self, **kwargs):
        context = super(ExtractedSourceView, self).get_context_data(**kwargs)
        self.set_template('extractedsource')
        extractedsource = self.database.extractedsource(id=kwargs['id'], dataset=kwargs['dataset'])
        if not extractedsource:
            raise Http404
        else:
            extractedsource = extractedsource[0]
        context['extractedsource'] = extractedsource
        context['dataset'] = self.database.dataset(id=kwargs['dataset'])[0]
        return context


class MonitoringListView(BaseView, FormMixin):
    form_class = MonitoringListForm
    form_class = MonitoringListForm
    initial = {}

    def form_valid(self, form):
        self.database.update_monitoringlist(
            form.cleaned_data['ra'], form.cleaned_data['dec'], self.dataset_id)
        return HttpResponseRedirect(self.get_succes_url())

    def form_invalid(self, form):
        return HttpResponseRedirect(self.get_succes_url())

    def get_succes_url(self):
        return reverse('dataset:monitoringlist',
                       kwargs={'dataset': self.dataset_id})

    def get(self, request, *args, **kwargs):
        self.dataset_id = kwargs['dataset']
        self.database = self.get_database(self.request.session.get('dblogin', None))
        form_class = self.get_form_class()
        form = self.get_form(form_class)
        return self.render_to_response(
            self.get_context_data(form=form, **kwargs))

    def post(self, request, *args, **kwargs):
        if not self.request.user.has_perm('monitoringlist.change_monitoringlist'):
            return HttpResponseForbidden()
        self.dataset_id = kwargs['dataset']
        self.database = self.get_database(self.request.session.get('dblogin', None))
        if request.POST['action'] == 'Delete selected':
            sources = [int(source) for source in
                       request.POST.getlist('sources', [])]
            self.database.delete_monitoringlist(sources)
            return HttpResponseRedirect(self.get_succes_url())
        form_class = self.get_form_class()
        form = self.get_form(form_class)
        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(form)

    def get_context_data(self, **kwargs):
        context = super(MonitoringListView, self).get_context_data(**kwargs)
        self.set_template('monitoringlist')
        context['sources'] = self.database.monitoringlist(dataset=kwargs['dataset'])
        context['dataset'] = self.database.dataset(id=kwargs['dataset'])[0]
        context['form'] = kwargs['form']
        return context


class TransientLightcurveView(BaseView):

    def get_context_data(self, **kwargs):
        context = super(TransientLightcurveView, self).get_context_data(**kwargs)
        transient = self.database.transient(id=kwargs['id'], dataset=kwargs['dataset'])
        if not transient:
            raise Http404
        else:
            transient = transient[0]
        context['transient'] = transient
        context['id'] = transient['trigger_xtrsrc']
        return context

    def render_to_response(self, context, **kwargs):
        response = HttpResponse(mimetype="image/png")
        plot.LightcurvePlot(response=response).render(
            self.database.lightcurve(context['id']))
        return response


class SourceLightcurveView(BaseView):

    def get_context_data(self, **kwargs):
        context = super(SourceLightcurveView, self).get_context_data(**kwargs)
        context['id'] = kwargs['id']
        return context

    def render_to_response(self, context, **kwargs):
        response = HttpResponse(mimetype="image/png")
        plot.LightcurvePlot(response=response).render(
            self.database.lightcurve(context['id']))
        return response


class ImagePlotView(BaseView):

    def get_context_data(self, **kwargs):
        context = super(ImagePlotView, self).get_context_data(**kwargs)
        context['id'] = kwargs['id']

    def render_to_response(self, context, **kwargs):
        response = HttpResponse(mimetype="image/png")
        image = self.database.image(
            id=self.kwargs['id'], dataset=self.kwargs['dataset'],
            extra_info=['ntotalsources'])
        if not image:
            raise Http404
        else:
            image = image[0]
        sources = self.database.extractedsource(image=image['id'])
        plot.ImagePlot(response=response, size=(12, 12)).render(
            image, plotsources=sources)
        return response

class TransientLightsurfaceView(BaseView):

    def get_context_data(self, **kwargs):
        context = super(TransientLightsurfaceView, self).get_context_data(**kwargs)
        transient = self.database.transient(id=kwargs['id'], dataset=kwargs['dataset'])
        if not transient:
            raise Http404
        else:
            transient = transient[0]
        context['transient'] = transient
        context['id'] = transient['trigger_xtrsrc']
        return context

    def render_to_response(self, context, **kwargs):
        response = HttpResponse(mimetype="image/png")
        plot.LightcurvePlot(response=response).render(
            self.database.lightcurve(context['id']))
        return response