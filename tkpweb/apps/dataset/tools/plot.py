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
from tkp.utility import accessors
from .image import open_image
import dbase

from tkpweb.settings import MONGODB
if MONGODB["enabled"]:
    from .mongo import fetch_hdu_from_mongo

class Plot(object):

    def __init__(self, response=None, size=(5, 5)):
        self.size = size
        self.response = response
        self.image = None

    def pre(self):
        """Hook for any preprocessing"""
        pass

    def post(self):
        """Hook for any postprocessing"""
        pass

    def setup(self):
        self.figure = Figure(figsize=self.size)
        self.canvas = FigureCanvasAgg(self.figure)

    def output(self, format='png'):
        if self.response:
             self.canvas.print_figure(self.response, format=format)
             self.image = self.response
        memfig = StringIO.StringIO()
        self.canvas.print_figure(memfig, format=format, transparent=True)
        encoded_png = StringIO.StringIO()
        encoded_png.write('data:image/%s;base64,\n' % format)
        encoded_png.write(base64.b64encode(memfig.getvalue()))
        self.image = encoded_png.getvalue()

    def render(self, *args, **kwargs):
        format = kwargs.pop('format', 'png')
        self.pre()
        self.setup()
        self.plot(*args, **kwargs)
        self.output(format=format)
        self.post()
        return self.image

    def plot(self, *args, **kwargs):
        """Do the actual plotting work"""
        raise NotImplementedError


class ImagePlot(Plot):

    def plot(self, dbimage, scale=0.9, plotsources=None, database=None):
        try:
            if os.path.exists(dbimage['url']):
                hdu = pyfits.open(dbimage['url'], readonly=True)
            elif MONGODB["enabled"]:
                hdu = fetch_hdu_from_mongo(dbimage['url'])
            else:
                raise Exception("FITS file not available")
        except:
            # Unable to access file
            return

        image = aplpy.FITSFigure(hdu, figure=self.figure, auto_refresh=False)

        image.show_grayscale()
        image.tick_labels.set_font(size=5)
        if plotsources:
            ra = [source['ra'] for source in plotsources]
            dec = [source['decl'] for source in plotsources]
            semimajor = [source['semimajor']/900 for source in plotsources]
            semiminor = [source['semiminor']/900 for source in plotsources]
            pa = [source['pa']+90 for source in plotsources]
            image.show_ellipses(ra, dec, semimajor, semiminor, pa, facecolor='none', edgecolor='green')


class ThumbnailPlot(Plot):

    def plot(self, filename, position, boxsize=(40, 40)):
        # Guess the file format from the extension
        try:
            if filename.lower().endswith(".fits"):
                image = accessors.FITSImage(filename)
            else:  # CASA does not really have default extensions
                image = accessors.CASAImage(filename)
        except IOError:
            # File doesn't exist; return nothing
            return
        # Convert the input coordinates to the pixel coordinates
        x, y = image.wcs.s2p(position)
        box = ((x-boxsize[0], x+boxsize[0]), (y-boxsize[0], y+boxsize[1]))
        thumbnail = image.data[box[0][0]:box[0][1],box[1][0]:box[1][1]]
        axes = self.figure.add_subplot(1, 1, 1)
        axes.get_xaxis().set_visible(False)
        axes.get_yaxis().set_visible(False)
        axes.imshow(thumbnail)
        self.figure.subplots_adjust(bottom=0, left=0, top=1, right=1)


class LightcurvePlot(Plot):

    def plot(self, lc, T0=None, images=None, trigger_index=None):
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
        axes = self.figure.add_subplot(1, 1, 1)
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


