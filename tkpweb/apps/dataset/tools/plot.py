import StringIO
import base64
import datetime
import time
import numpy
import aplpy
from scipy.stats import scoreatpercentile
import matplotlib
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.patches import Rectangle
from matplotlib.collections import PatchCollection
from .image import open_image
import dbase


def image(dbimage, scale=0.9, plotsources=None, database=None):
    figure = Figure(figsize=(5, 5))
    canvas = FigureCanvasAgg(figure)
    image = aplpy.FITSFigure(dbimage['url'], figure=figure, auto_refresh=False)
    image.show_grayscale()
    image.tick_labels.set_font(size=5)
    if plotsources:
        ra = [source['ra'] for source in plotsources]
        dec = [source['decl'] for source in plotsources]
        major = [source['major']/900 for source in plotsources]
        minor = [source['minor']/900 for source in plotsources]
        angle = [source['theta']+90 for source in plotsources]
        image.show_ellipses(ra, dec, major, minor, angle, facecolor='none', edgecolor='green')
        #show_ellipses(self, xw, yw, width, height, angle=0, layer=False, zorder=None, **kwargs):
        #image.show_markers(ra, dec, s=40, facecolor='none', edgecolor='green')
    memfig = StringIO.StringIO()
    canvas.print_figure(memfig, format='png', transparent=True)
    encoded_png = StringIO.StringIO()
    encoded_png.write('data:image/png;base64,\n')
    encoded_png.write(base64.b64encode(memfig.getvalue()))
    return encoded_png.getvalue()


def lightcurve(lc, T0=None, response=None, images=None, trigger_index=None, size=(8, 8)):
    #dates = matplotlib.dates.date2num([point[0] for point in lc])
    times = numpy.array([time.mktime(point[0].timetuple()) for point in lc])
    inttimes = [point[1]/2. for point in lc]
    fluxes = [point[2] for point in lc]
    errors = [point[3] for point in lc]
    if T0 is None:
        tmin = sorted(times)[0]
        if images:
            tmin2 = time.mktime(sorted(zip(*images)[0])[0].timetuple())
            if tmin2  < tmin:
                tmin = tmin2
        tmin = datetime.datetime.fromtimestamp(tmin)
        T0 = datetime.datetime(tmin.year, tmin.month, tmin.day, 0, 0, 0)
    tdiff = T0 - datetime.datetime(1970, 1, 1)
    tdiff = (tdiff.microseconds + (tdiff.seconds + tdiff.days * 86400) * 1e6) / 1e6
    times -= tdiff
    figure = Figure(figsize=size)
    axes = figure.add_subplot(1, 1, 1)
    axes.errorbar(x=times, y=fluxes, yerr=errors, xerr=numpy.array(inttimes)/2., fmt='bo')
    if trigger_index is not None:
        axes.errorbar(x=times[trigger_index], y=fluxes[trigger_index], fmt='o', mec='r', ms=15., mfc='None')
    ylimits = axes.get_ylim()
    if images:
        images = zip(*images)
        x = numpy.array([time.mktime(x.timetuple()) for x in images[0]]) - tdiff
        xerr = numpy.array(images[1])/2.
        patches = [Rectangle((xx - xxerr, ylimits[0]), xxerr, ylimits[1]-ylimits[0])
                for xx, xxerr in zip(x, xerr)]
        patches = PatchCollection(patches, alpha=0.3, linewidth=0, visible=True, color='r')
        axes.add_collection(patches)
    axes.set_xlabel('Seconds since %s' % T0.strftime('%Y-%m-%dT%H:%M:%S'))
    axes.set_ylabel('Flux (Jy)')
    canvas = FigureCanvasAgg(figure)
    if response:
       canvas.print_figure(response, format='png')
       return response
    memfig = StringIO.StringIO()
    canvas.print_figure(memfig, format='png')
    encoded_png = StringIO.StringIO()
    encoded_png.write('data:image/png;base64,\n')
    encoded_png.write(base64.b64encode(memfig.getvalue()))
    return encoded_png.getvalue()
