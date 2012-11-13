import tkp.database as tkpdb
from scipy.stats import chisqprob
from .image import open_image
from tkpweb import settings
import datetime


class DataBase(object):

    def __init__(self, dblogin=None):
        self.dblogin = dblogin
        self.db = tkpdb.DataBase(**dblogin) if dblogin else tkpdb.DataBase()

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
SELECT COUNT(*) FROM image WHERE dataset = %s"""
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
                    SELECT t.id
                          ,t.runcat
                          ,t.trigger_xtrsrc
                          ,i.freq_eff
                          ,t.band
                          ,t.siglevel
                          ,t.V_int
                          ,t.eta_int
                          ,t.t_start
                          ,rc.datapoints
                          ,rc.wm_ra
                          ,rc.wm_ra_err
                          ,rc.wm_decl
                          ,rc.wm_decl_err 
                      FROM runningcatalog as rc
                          ,transient t
                          ,extractedsource x
                          ,image i
                     WHERE t.id = %s 
                       AND rc.dataset = %s 
                       AND t.runcat = rc.id
                       AND t.trigger_xtrsrc = x.id
                       AND x.image = i.id
                    """, id, dataset)
            else:
                self.db.execute("""\
                    SELECT transient.*
                          ,rc.datapoints
                          ,rc.wm_ra
                          ,rc.wm_ra_err
                          ,rc.wm_decl
                          ,rc.wm_decl_err 
                      FROM runningcatalog as rc
                          ,transient 
                     WHERE transient.id = %s 
                       AND transient.runcat = rc.id
                    """, id)
        else:
            if dataset is not None:
                self.db.execute("""\
                    SELECT t.id
                          ,t.runcat
                          ,t.trigger_xtrsrc
                          ,i.freq_eff
                          ,t.band
                          ,t.siglevel
                          ,t.V_int
                          ,t.eta_int
                          ,t.t_start
                          ,rc.datapoints
                          ,rc.wm_ra
                          ,rc.wm_ra_err
                          ,rc.wm_decl
                          ,rc.wm_decl_err 
                      FROM runningcatalog as rc
                          ,transient t
                          ,extractedsource x
                          ,image i
                     WHERE rc.dataset = %s 
                       AND t.runcat = rc.id
                       AND t.trigger_xtrsrc = x.id
                       AND x.image = i.id
                    """, dataset)
            else:
                self.db.execute("""\
                    SELECT transient.*
                          ,rc.datapoints
                          ,rc.wm_ra
                          ,rc.wm_ra_err
                          ,rc.wm_decl
                          ,rc.wm_decl_err 
                      FROM runningcatalog as rc
                          ,transient 
                     WHERE transient.runcat = rc.id
                    """)


        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        transients = []
        for row in self.db.cursor.fetchall():
            transients.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Format into somewhat nicer keys
            #for key1, key2 in zip(
            #    ['id', 't_start'],
            #    ['id', 'startdate']):
            #    transients[-1][key2] = transients[-1][key1]
            # Obtain the actual number of datapoints, including those from
            # sub-detection level monitoring observations
            transients[-1]['npoints'] = self.db.getone(
                "SELECT COUNT(*) FROM assocxtrsource WHERE runcat = %s",
                transients[-1]['runcat'])[0]
            
            # TODO: FEEDBACK: Why is the siglevel recalculated. This was
            # already done and stored in the transient table
            # Calculate the significance level (note: here we do need rc.datapoints,
            # instead of the above npoints)
            #n = transients[-1]['datapoints']
            #transients[-1]['siglevel'] = chisqprob(
            #    transients[-1]['siglevel'] * n, n)
        return transients


    def source(self, runcat=None, dataset=None):
        """Get information on one or sources from the database

        The sources obtained are those in the runningcatalog; these are the
        unique (= with associations) sources.

        Kwargs:

            runcat (int or None): if None, obtain a listing of all applicable
                sources. Otherwise, obtain the information for a specific
                entry in running catalog

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
        #NB In new schema Runcat --> Dataset. 
        #Need only condition on one of these.
        
        if runcat is not None:  # id = 0 could be valid for some databases
            self.db.execute("""
            SELECT * 
            FROM runningcatalog
            WHERE id = %s""", runcat)
        elif dataset is not None:
            self.db.execute("""\
            SELECT * 
            FROM runningcatalog 
            WHERE dataset = %s""", dataset)
        else: #Gotta catch em all
            self.db.execute("""\
            SELECT * FROM runningcatalog""")
            
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        sources = []
        for row in self.db.cursor.fetchall():
            sources.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
            # Duplicate dictionary key 'id' as more explicit 'runcat'
            sources[-1]['runcat'] = sources[-1]['id']
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

        partial_query = """\
        SELECT ex.*, im.dataset as dataset, ax.runcat as runcat
        FROM 
            extractedsource ex LEFT JOIN assocxtrsource ax
                        on ex.id = ax.xtrsrc,
            image im
        WHERE ex.image = im.id
        """
        
        #NB: image.id implies -> Dataset
        #    xtrsrc.id implies -> Image.
        #Therefore, we need only condition 
        #on the *best* information. (xtrsrc > image > dataset)

        if id is not None:  # id = 0 could be valid for some databases
            q_args = (id,)
            extra_condition ="""
            AND ex.id = %s  
            """
        elif image is not None:
            q_args = (image,)
            extra_condition ="""  
            AND ex.image = %s 
            """
        elif dataset is not None: #image is None
            q_args = (dataset,)
            extra_condition ="""  
            AND im.dataset = %s 
            """
        else: ##All none. Simply return all extracted sources.
            extra_condition = ''
            q_args=()
                    
        self.db.cursor.execute(partial_query+extra_condition, q_args)
        description = dict(
            [(d[0], i) for i, d in enumerate(self.db.cursor.description)])
        sources = []
        for row in self.db.cursor.fetchall():
            sources.append(
                dict([(key, row[column])
                      for key, column in description.iteritems()]))
        return sources


    def monitoringlist(self, dataset):
        monlist_entries =  tkpdb.utils.columns_from_table(self.db.connection,
                                              'monitoringlist', 
                                              where={"dataset":dataset})
        
        #Replace empty ra, dec with runcat weighted means for blind entries
        for m in monlist_entries:
            if m['userentry']==False:
                runcat_entry = tkpdb.utils.columns_from_table(
                                              self.db.connection,
                                              'runningcatalog',
                                              keywords=['wm_ra','wm_decl'],
                                              where={'id':m['runcat']}
                                              )[0]
#                print runcat_entry
                m['ra']=runcat_entry['wm_ra']
                m['decl']=runcat_entry['wm_decl'] 
        return monlist_entries
    
    
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
        lc = tkpdb.ExtractedSource(id=srcid, database=self.db).lightcurve()
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
