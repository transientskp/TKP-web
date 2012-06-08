"""
Quality control checks
Based on tkp/database/qcplots.py
"""

import StringIO
import base64
import numpy
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
from .plot import Plot


def plot_rms_distance_from_fieldcentre(
    database, dsid, dist_arcsec_cutoff=36000, response=None,
    size=(8, 8)):
    """Plot the rms of extracted sources in given dataset vs their
    distance from the field centre."""

    query = """\
SELECT *
FROM (SELECT ex.image_id
            ,ex.xtrsrcid as xtrsrcid1
            ,3600* DEGREES(2 * ASIN(SQRT( (ex.x - rc.x) * (ex.x - rc.x)
                                        + (ex.y - rc.y) * (ex.y - rc.y)
                                        + (ex.z - rc.z) * (ex.z - rc.z)
                                        ) / 2)) AS centr_img_dist_deg
            ,20000 * ex.i_peak / ex.det_sigma as rms_mJy
        FROM extractedsources ex
            ,images im
            ,runningcatalog rc
       WHERE ex.image_id = im.imageid
         AND im.ds_id = rc.ds_id
         AND rc.ds_id = %s
         AND rc.xtrsrc_id = ex.xtrsrcid
      ) t
WHERE centr_img_dist_deg < %s
ORDER BY centr_img_dist_deg
"""
    database.db.cursor.execute(query, (dsid, dist_arcsec_cutoff))
    results = zip(*database.db.get(query, dsid, dist_arcsec_cutoff))

    if not results:
        return None
    dist_deg, rms = results[2], results[3]
    
    figure = Figure(figsize=size)
    canvas = FigureCanvasAgg(figure)
    axes = figure.add_subplot(1, 1, 1)
    axes.scatter(dist_deg, rms, c='r', s=20, edgecolor='r')
    axes.set_xlabel(r'Distance from Pointing Centre (deg)', size='x-large')
    axes.set_ylabel(r'rms (mJy/beam)', size='x-large')
    axes.set_xlim(xmin=0)
    axes.set_ylim(ymin=0)
    axes.grid(True)

    if response:
        canvas.print_figure(response, format='png')
        return response
    memfig = StringIO.StringIO()
    canvas.print_figure(memfig, format='png')
    encoded_png = StringIO.StringIO()
    encoded_png.write('data:image/png;base64,\n')
    encoded_png.write(base64.b64encode(memfig.getvalue()))
    return encoded_png.getvalue()


class HistSourcesPerImagePlot(Plot):

    def plot(self, database, dsid):
        def autolabel(axes, rects, taustart):
            i = 0
            for rect in rects:
                height = rect.get_height()
                print height
                axes.text(rect.get_x()+rect.get_width()/2., 1.05*height, int(height),
                         rotation='horizontal', ha='center', va='bottom')
                axes.text(rect.get_x()+rect.get_width()/2., 0.05*height, taustart[i].isoformat(),
                         rotation='vertical', ha='center', va='bottom')
                i += 1

        query = """\
SELECT imageid
      ,taustart_ts
      ,nsources
FROM images
    ,(SELECT x1.image_id 
            ,COUNT(*) as nsources
      FROM extractedsources x1
          ,images im1
      WHERE x1.image_id = im1.imageid
        AND ds_id = %s
      GROUP BY x1.image_id
     ) t1
WHERE t1.image_id = imageid
"""
        results = zip(*database.db.get(query, dsid))
        if not results:
            return None
        imageid = results[0]
        taustart_ts = results[1]
        nsources = results[2]

        axes = self.figure.add_subplot(1, 1, 1)    
        width = 0.8
        ind = numpy.arange(len(imageid))
        rects = axes.bar(ind, nsources, width, color='r')
        axes.set_xlabel(r'Image')
        axes.set_ylabel(r'Number of Sources')
        axes.set_xticks(ind + width/2.)
        axes.set_xticklabels(imageid)
        autolabel(axes, rects, taustart_ts)
        axes.grid(True)
    

class ScatterPosAllCounterpartsPlot(Plot):

    def plot(self, database, dsid):
        """Plot positions of all counterparts for all (unique) sources for
        the given dataset.
    
        The positions of all (unique) sources in the running catalog are
        at the centre, whereas the positions of all their associated
        sources are scattered around the central point.  Axes are in
        arcsec relative to the running catalog position.
        """
    
        query = """\
SELECT x.xtrsrcid
      ,x.ra
      ,x.decl
      ,3600 * (x.ra - r.wm_ra) as ra_dist_arcsec
      ,3600 * (x.decl - r.wm_decl) as decl_dist_arcsec
      ,x.ra_err/2
      ,x.decl_err/2 
      ,r.wm_ra_err/2
      ,r.wm_decl_err/2
  FROM assocxtrsources a
      ,extractedsources x 
      ,runningcatalog r
      ,images im1
 WHERE a.xtrsrc_id <> a.assoc_xtrsrc_id
   AND a.xtrsrc_id = r.xtrsrc_id
   AND a.assoc_xtrsrc_id = x.xtrsrcid
   AND x.image_id = im1.imageid
   AND im1.ds_id = %s

"""
        results = zip(*database.db.get(query, dsid))
    
        if not results:
            return None
        xtrsrc_id = results[0]
        ra = results[1]
        decl = results[2]
        ra_dist_arcsec = results[3]
        decl_dist_arcsec = results[4]
        ra_err = results[5]
        decl_err = results[6]
        wm_ra_err = results[7]
        wm_decl_err = results[8]
    
        axes = self.figure.add_subplot(1, 1, 1)
        axes.errorbar(ra_dist_arcsec, decl_dist_arcsec, xerr=ra_err, yerr=decl_err,
                      fmt='+',  color='b', label="xtr")
        axes.set_xlabel(r'RA (arcsec)')
        axes.set_ylabel(r'DEC (arcsec)')
        lim = 1 + max(int(numpy.trunc(max(abs(min(ra_dist_arcsec)),
                                          abs(max(ra_dist_arcsec))))),
                      int(numpy.trunc(max(abs(min(decl_dist_arcsec)),
                                          abs(max(decl_dist_arcsec))))))
        axes.set_xlim(xmin=-lim, xmax=lim)
        axes.set_ylim(ymin=-lim, ymax=lim)
        axes.grid(False)
