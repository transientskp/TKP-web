from tkp.database import database
from tkp.database.dataset import ExtractedSource
from scipy.stats import chisqprob
from .image import open_image
from tkpweb import settings
import datetime


class DataBase(object):

    def __init__(self, dblogin=None):
        self.dblogin = dblogin
        self.db = database.DataBase(**dblogin) if dblogin else database.DataBase()

    def dataset(self, id=None, extra_info=()):
        """Get information on one or more datasets form the database

        Kwargs:

            id (int or None): if None, obtain a listing of all
                datasets. Otherwise, obtain the information for a specific
                dataset.

            extra_info (set of strings): if given, some extra
                information (through other tables) is obtained from the
                database. Each string can be one of:

                - ntransients: number of transients for a dataset.

                - nimages: number of images for a dataset.

                - nsources: number of unique sources for a dataset.

                - ntotalsources: total number of sources (found by
                  sourcefinder) for this dataset.

                Note that a tuple or list of strings is valid input, but
                will be transformed into a set, to filter out double
                strings.

         Returns:

            (list): A list of dicts; each list item corresponds to a
                single database row, while each dict contains the values
                for the columns (with the column names the keys; some
                column values are available twice, with a different
                key). For a single dataset, the returned value is a
                single-element list.
        """

        extra_info = set(extra_info)
        if id is not None:  # id = 0 could be valid for some databases
            self.db.execute("""SELECT * FROM dataset WHERE id = %s""", id)
        else:
            self.db.execute("""SELECT * FROM dataset""")
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        datasets = []
        for row in self.db.cursor.fetchall():
            datasets.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Format into slightly nicer keys
            for key1, key2 in zip(
                ['id', 'process_ts'],
                ['id', 'processdate']):
                datasets[-1][key2] = datasets[-1][key1]
            if 'ntransients' in extra_info:
                query = """\
SELECT COUNT(*) FROM transient tr, runningcatalog rc
WHERE tr.runcat = rc.id AND rc.dataset = %s"""
                datasets[-1]['ntransients'] = self.db.getone(
                    query, datasets[-1]['id'])[0]
            if 'nimages' in extra_info:
                query = """\
SELECT COUNT(*) FROM image WHERE id = %s"""
                datasets[-1]['nimages'] = self.db.getone(
                    query, datasets[-1]['id'])[0]
            if 'nsources' in extra_info:
                query = """\
SELECT COUNT(*) FROM runningcatalog WHERE dataset = %s"""
                datasets[-1]['nsources'] = self.db.getone(
                    query, datasets[-1]['id'])[0]
            if 'ntotalsources' in extra_info:
                query = """\
SELECT COUNT(*) FROM extractedsource ex, image im
WHERE ex.image = im.id and im.dataset = %s"""
                datasets[-1]['ntotalsources'] = self.db.getone(
                    query, datasets[-1]['id'])[0]
        return datasets


    def image(self, id=None, dataset=None, extra_info=()):
        """Get information on one or more datasets form the database

        Kwargs:

            id (int or None): if None, obtain a listing of all applicable
                images. Otherwise, obtain the information for a specific
                image.

            dataset (int or None): limit image(s) to given dataset, if
                any.

            extra_info (set of strings): if given, some extra
                information (through other tables) is obtained from the
                database. Each string can be one of:

                - ntotalsources: total number of sources (found by
                  sourcefinder) for this image.

                Note that a tuple or list of strings is valid input, but
                will be transformed into a set, to filter out double
                strings.

         Returns:

            (list): A list of dicts; each list item corresponds to a
                single database row, while each dict contains the values
                for the columns (with the column names the keys; some
                column values are available twice, with a different
                key). For a single image, the returned value is a
                single-element list.
        """

        extra_info = set(extra_info)
        if id is not None:  # id = 0 could be valid for some databases
            if dataset is not None:
                self.db.execute("""\
SELECT * FROM image WHERE id = %s AND dataset = %s""", id, dataset)
            else:
                self.db.execute("""\
SELECT * FROM image WHERE id = %s""", id)
        else:
            if dataset is not None:
                self.db.execute("""SELECT * FROM image WHERE dataset = %s""", dataset)
            else:
                self.db.execute("""SELECT * FROM image""")
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        images = []
        for row in self.db.cursor.fetchall():
            images.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Format into slightly nicer keys
            for key1, key2 in zip(
                ['id', 'taustart_ts', 'tau_time', 'freq_eff', 'freq_bw',
                 'dataset'],
                ['id', 'obsstart', 'inttime', 'frequency', 'bandwidth',
                 'dataset']):
                images[-1][key2] = images[-1][key1]
            # Open image to obtain phase center
            img = open_image(images[-1]['url'], database=self.db)
            try:
                header = img.get_header()
                images[-1]['ra'] = header['phasera']
                images[-1]['dec'] = header['phasedec']
            except (KeyError, AttributeError):
                images[-1]['ra'] = None
                images[-1]['dec'] = None
            if 'ntotalsources' in extra_info:
                query = """\
SELECT COUNT(*) FROM extractedsource WHERE image = %s"""
                images[-1]['ntotalsources'] = self.db.getone(
                    query, images[-1]['id'])[0]
        return images


    def transient(self, id=None, dataset=None):
        """Get information on one or more datasets form the database

        Kwargs:

            id (int or None): if None, obtain a listing of all applicable
                images. Otherwise, obtain the information for a specific
                dataset.

            dataset (int or None): limit image(s) to given dataset, if
                any.

         Returns:

            (list): A list of dicts; each list item corresponds to a
                single database row, while each dict contains the values
                for the columns (with the column names the keys; some
                column values are available twice, with a different
                key). For a single image, the returned value is a
                single-element list.
        """

        if id is not None:  # id = 0 could be valid for some databases
            if dataset is not None:
                self.db.execute("""\
    SELECT * FROM transient tr, runningcatalog rc
    WHERE tr.id = %s AND tr.runcat = rc.id AND rc.dataset = %s""",
                           id, dataset)
            else:
                self.db.execute("""\
    SELECT * FROM transient WHERE id = %s""", id)
        else:
            if dataset is not None:
                self.db.execute("""\
    SELECT * FROM transient tr, runningcatalog rc
    WHERE tr.runcat = rc.id AND dataset = %s""", dataset)
            else:
                self.db.execute("""SELECT * FROM transient""")
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        transients = []
        for row in self.db.cursor.fetchall():
            transients.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Format into somewhat nicer keys
            for key1, key2 in zip(
                ['transientid', 't_start'],
                ['id', 'startdate']):
                transients[-1][key2] = transients[-1][key1]
            # Obtain the actual number of datapoints, including those from
            # sub-detection level monitoring observations
            transients[-1]['npoints'] = self.db.getone(
                "SELECT COUNT(*) FROM assocxtrsource WHERE xtrsrc = %s",
                transients[-1]['xtrsrc'])[0]
            # Calculate the significance level (note: here we do need rc.datapoints,
            # instead of the above npoints)
            n = transients[-1]['datapoints']
            transients[-1]['siglevel'] = chisqprob(
                transients[-1]['siglevel'] * n, n)
        return transients


    def source(self, id=None, dataset=None):
        """Get information on one or sources from the database

        The sources obtained are those in the runningcatalog; these are the
        unique (= with associations) sources.

        Kwargs:

            id (int or None): if None, obtain a listing of all applicable
                sources. Otherwise, obtain the information for a specific
                dataset.

            dataset (int or None): limit image(s) to given dataset, if
                any.

         Returns:

            (list): A list of dicts; each list item corresponds to a
                single database row, while each dict contains the values
                for the columns (with the column names the keys; some
                column values are available twice, with a different
                key). For a single image, the returned value is a
                single-element list.
        """

        if id is not None:  # id = 0 could be valid for some databases
            if dataset is not None:
                self.db.execute("""
    SELECT * FROM runningcatalog
    WHERE xtrsrc = %s AND dataset = %s""", id, dataset)
            else:
                self.db.execute("""\
    SELECT * FROM runningcatalog WHERE xtrsrc = %s""", id)
        else:
            if dataset is not None:
                self.db.execute("""\
    SELECT * FROM runningcatalog WHERE dataset = %s""", dataset)
            else:
                self.db.execute("""SELECT * FROM runningcatalog""")
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        sources = []
        for row in self.db.cursor.fetchall():
            sources.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Format into somewhat nicer keys
            for key1, key2 in zip(
                ['xtrsrc', 'dataset'],
                ['id', 'dataset']):
                sources[-1][key2] = sources[-1][key1]
        return sources


    def extractedsource(self, id=None, dataset=None, image=None):
        """Get information on one or more extractedsources from the
        database

        Kwargs:

            id (int or None): if None, obtain a listing of all applicable
                sources. Otherwise, obtain the information for a specific
                dataset.

            dataset (int or None): limit image(s) to given dataset, if
                any.

            image (int or None): limit sources to given image. If the
                image is not in the dataset, an empty list will be
                returned.

         Returns:

            (list): A list of dicts; each list item corresponds to a
                single database row, while each dict contains the values
                for the columns (with the column names the keys; some
                column values are available twice, with a different
                key). For a single image, the returned value is a
                single-element list.

            Note important keys:
            'id' : extracted source id
            'assoc_id' : Unique id for associated source.
            'image': if of image from which source extracted
        """

        if id is not None:  # id = 0 could be valid for some databases
            if dataset is not None:
                if image is not None:
                    self.db.execute("""
    SELECT ex.*, im.*, ax.xtrsrc
    FROM 
        extractedsource ex LEFT JOIN assocxtrsource ax
                    on ex.id = ax.xtrsrc,
        image im
    WHERE ex.id = %s AND ex.image = im.id AND
    im.dataset = %s and ex.image = %s
    """, id, dataset, image)
                else:
                    self.db.execute("""
    SELECT ex.*, im.*, ax.xtrsrc
    FROM 
        extractedsource ex LEFT JOIN assocxtrsource ax
                    on ex.id = ax.xtrsrc,
        image im, assocxtrsource ax
    WHERE ex.id = %s AND ex.image = im.id AND
    im.dataset = %s
    """, id, dataset)
            else: #id is not none, dataset is none
                if image is not None:
                    self.db.execute("""\
    SELECT ex.*, ax.xtrsrc
    FROM extractedsource ex LEFT JOIN assocxtrsource ax
                    on ex.id = ax.xtrsrc ,
    WHERE ex.id = %s
    AND ex.image = %s
    """, id, image)
                else:
                    self.db.execute("""\
    SELECT ex.*, ax.xtrsrc
    FROM extractedsource ex LEFT JOIN assocxtrsource ax
                    on ex.id = ax.xtrsrc ,
    WHERE ex.id = %s
    """, id)
        else: #id is None
            if dataset is not None:
                if image is not None:
                    self.db.execute("""\
    SELECT ex.*, im.*, ax.xtrsrc
    FROM 
        extractedsource ex LEFT JOIN assocxtrsource ax
                    on ex.id = ax.xtrsrc ,
        image im
    WHERE ex.image = im.id AND im.dataset = %s
    AND ex.image = %s
    """, dataset, image)
                else:
                    self.db.execute("""\
    SELECT ex.*, im.*, ax.xtrsrc
    FROM 
        extractedsource ex LEFT JOIN assocxtrsource ax
                    on ex.id = ax.xtrsrc ,
        image im
    WHERE ex.image = im.id AND im.dataset = %s
    """, dataset)
            else:#id is none, dataset is none
                if image is not None:
                    self.db.execute("""\
    SELECT ex.*, ax.xtrsrc
    FROM extractedsource ex LEFT JOIN assocxtrsource ax
                    on ex.id = ax.xtrsrc
    WHERE ex.image = %s
    """, image)
                else: ##All none. Simply return all extracted sources.
                    self.db.execute("""\
    SELECT ex.*, ax.xtrsrc
    FROM extractedsource ex LEFT JOIN assocxtrsource ax
                    on ex.id = ax.xtrsrc
    """)
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        sources = []
        for row in self.db.cursor.fetchall():
            sources.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Format into somewhat nicer keys
            for key1, key2 in zip(
                ['id', 'xtrsrc', 'image'],
                ['id', 'assoc_id', 'image']):
                sources[-1][key2] = sources[-1][key1]
            #sources[-1]['flux'] = {'peak': {}, 'int': {}}
            #    for fluxtype in ('peak', 'int'):
            #        sources[-1]['flux'][fluxtype].append(
            #            {'stokes': stokes,
            #             'value': sources[-1][stokes+"_"+fluxtype],
            #             'error': sources[-1][stokes+"_"+fluxtype+"_err"]}
            #            )
        return sources


    def monitoringlist(self, dataset):
        # Get all user defined entries
        query = """\
SELECT * FROM monitoringlist WHERE userentry = true AND dataset = %s"""
        self.db.execute(query, dataset)
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        sources = []
        for row in self.db.cursor.fetchall():
            sources.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Format into somewhat nicer keys;
            for key1, key2 in zip(
                ['monitorid', 'dataset'],
                ['id', 'dataset']):
                sources[-1][key2] = sources[-1][key1]
        # Get all non-user entries belonging to this dataset
        query = """\
SELECT * FROM monitoringlist ml, runningcatalog rc
WHERE ml.userentry = false AND ml.xtrsrc = rc.xtrsrc AND rc.dataset = %s"""
        self.db.execute(query, dataset)
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        for row in self.db.cursor.fetchall():
            sources.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Format into somewhat nicer keys;
            # replace ra, dec by values from runningcatalog
            for key1, key2 in zip(
                ['monitorid', 'dataset', 'wm_ra', 'wm_decl'],
                ['id', 'dataset', 'ra', 'decl']):
                sources[-1][key2] = sources[-1][key1]
        return sources

    def update_monitoringlist(self, ra, dec, dataset):
        query = """\
INSERT INTO monitoringlist
(xtrsrc, ra, decl, dataset, userentry)
VALUES (-1, %s, %s, %s, TRUE)"""
        self.db.execute(query, ra, dec, dataset)
        self.db.commit()

    def delete_monitoringlist(self, sources):
        for source in sources:
            self.db.execute(
                "DELETE FROM monitoringlist WHERE monitorid = %s", source)
            self.db.commit()

    def lightcurve(self, srcid):
        lc = ExtractedSource(id=srcid, database=self.db).lightcurve()
        return lc


    def image_times(self, dataset):
        image_times = self.db.get(
            "SELECT taustart_ts, tau_time FROM image WHERE dataset = %s",
            dataset)
        # Make the dates mid-point
        image_times = [(taustart_ts + datetime.timedelta(seconds=tau_time/2),
                        tau_time) for taustart_ts, tau_time in image_times]
        return image_times

    def thumbnail(self, srcid):
        """Get thumbnail information for a source.

        Returns ra, dec and image filename
        """

        ra, dec, filename = self.db.getone("""\
SELECT ex.ra, ex.decl, im.url
FROM extractedsource ex, image im
WHERE ex.id = %s
  AND ex.image = im.id
""", srcid)
        return ra, dec, filename
