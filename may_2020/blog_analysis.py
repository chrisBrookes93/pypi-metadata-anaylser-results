from __future__ import division
import sqlite3
import tempfile
import os
import shutil


class PyPiQuery:

    def __init__(self, db_path):
        # Take a copy of the database to modify it as per our querying needs. This allows the tool that builds the
        # database to remain generic but we're still able to service our needs
        temp_dir = tempfile.mkdtemp()
        temp_db_path = os.path.join(temp_dir, 'pypi.sqlite')
        shutil.copy(db_path, temp_db_path)
        print('Database copied to {}'.format(temp_db_path))
        self.db_path = temp_db_path
        self._db_con = sqlite3.connect(self.db_path)
        self.with_py2_class_cache = None
        self.with_py3_class_cache = None
        self.all_package_ids_cache = None

    def delete_stale_packages(self, period='4 years'):
        sql = """
        DELETE FROM packages 
        WHERE packages.id IN 
            (SELECT package_id FROM package_releases 
               INNER JOIN packages ON packages.id = package_releases.package_id
               WHERE upload_time_iso_8601 < date('now','-{}'))""".format(period)
        cur = self._db_con.cursor()
        cur.execute(sql)
        self._db_con.commit()

    def _get_first_column_list_for_query(self, sql, args=()):
        cur = self._db_con.cursor()
        cur.execute(sql, args)
        rows = cur.fetchall()
        cols_ls = [r[0] for r in rows]
        return cols_ls

    def select_package_ids_with_classifier(self, classifier):
        sql = """
        SELECT DISTINCT packages.id FROM packages
        INNER JOIN package_classifiers
            ON package_classifiers.package_id = packages.id
        INNER JOIN classifier_strings
            ON classifier_strings.id = package_classifiers.classifier_id
        WHERE classifier_strings.name LIKE ?"""
        results = self._get_first_column_list_for_query(sql, (classifier,))
        return results

    def get_all_package_ids(self, flush_cache=False):
        if not self.all_package_ids_cache or flush_cache:
            self.all_package_ids_cache = self._get_first_column_list_for_query("SELECT id FROM packages")
        return sorted(set(self.all_package_ids_cache))

    def with_py3_classifier(self):
        if not self.with_py3_class_cache:
            self.with_py3_class_cache = self.select_package_ids_with_classifier('Programming Language :: Python :: 3%')
        return sorted(set(self.with_py3_class_cache))

    def with_py2_classifier(self):
        if not self.with_py2_class_cache:
            self.with_py2_class_cache = self.select_package_ids_with_classifier('Programming Language :: Python :: 2%')
        return sorted(set(self.with_py2_class_cache))

    def with_py2_and_py3_classifier(self):
        with_py2 = set(self.with_py2_classifier())
        with_py3 = set(self.with_py3_classifier())
        return sorted(list(with_py2.intersection(with_py3)))

    def with_py2_but_not_py3_classifier(self):
        with_py2 = set(self.with_py2_classifier())
        with_py3 = set(self.with_py3_classifier())
        return sorted(list(with_py2 - with_py3))

    def with_py3_but_not_py2_classifier(self):
        with_py2 = set(self.with_py2_classifier())
        with_py3 = set(self.with_py3_classifier())
        return sorted(list(with_py3 - with_py2))

    def package_not_using_pyversion_classifer(self):
        package_list = set(self.get_all_package_names())
        package_list -= set(self.with_py2_classifier())
        package_list -= set(self.with_py3_classifier())
        return sorted(list(package_list))

    def packages_with_no_classifiers(self):
        sql = """
        SELECT packages.id FROM packages 
        INNER JOIN package_classifiers 
            ON packages.id = package_classifiers.package_id 
        GROUP BY packages.id"""
        with_classifiers = self._get_first_column_list_for_query(sql)
        return sorted(list(set(self.get_all_package_ids()) - set(with_classifiers)))

    def package_with_no_version_classifiers(self):
        with_py2 = set(self.with_py2_classifier())
        with_py3 = set(self.with_py3_classifier())
        total_with_v_class = with_py2.union(with_py3)
        return sorted(list(set(self.get_all_package_ids()) - total_with_v_class))

    def with_release_in_last_x(self, x='1 month'):
        sql = """
        SELECT packages.id FROM package_releases 
        INNER JOIN packages ON packages.id = package_releases.package_id
        WHERE upload_time_iso_8601 > date('now','-{}') 
        GROUP BY package_id""".format(x)
        return self._get_first_column_list_for_query(sql)

    def get_release_package_type_prevalence(self):
        sql = """
        SELECT packagetype, COUNT(*) 
        FROM package_releases 
        GROUP BY packagetype"""
        cur = self._db_con.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def with_release_containing_license_keyword(self, keyword):
        sql = """
        SELECT id
        FROM packages 
        WHERE LOWER(license) LIKE ?"""
        return self._get_first_column_list_for_query(sql, ('%' + keyword + '%',))

    def with_release_python_version_x(self, x):
        sql = """
        SELECT packages.id FROM packages
        INNER JOIN package_releases ON packages.id = package_releases.package_id
        WHERE python_version LIKE ? or python_version == 'any'
        GROUP BY packages.id
        """
        return self._get_first_column_list_for_query(sql, (x,))

    def biggest_package_author(self, limit=1):
        sql = """
        SELECT author, COUNT(*) AS count 
        FROM packages 
        WHERE author NOT IN ("", "UNKNOWN", "Example Author") 
        GROUP BY author 
        ORDER BY count DESC 
        LIMIT ?"""
        cur = self._db_con.cursor()
        cur.execute(sql, (limit,))
        return cur.fetchall()

    def delete_packages_by_ids(self, names_to_del):
        sql = """
        DELETE FROM packages 
        WHERE id IN ({})""".format(','.join([str(x) for x in names_to_del]))
        cur = self._db_con.cursor()
        cur.execute(sql)
        self._db_con.commit()

    def with_py3_release_not_py2(self):
        sql = """
        SELECT packages.id FROM packages
        INNER JOIN package_releases ON packages.id = package_releases.package_id
        WHERE python_version LIKE '%3%' AND python_version NOT LIKE '%2%'
        GROUP BY packages.name
        """
        return self._get_first_column_list_for_query(sql)

    def with_py2_release_not_py3(self):
        sql = """
        SELECT packages.id FROM packages
        INNER JOIN package_releases ON packages.id = package_releases.package_id
        WHERE python_version LIKE '%2%' AND python_version NOT LIKE '%3%'
        GROUP BY packages.name
        """
        return self._get_first_column_list_for_query(sql)


ppq = PyPiQuery('pypi_metadata.sqlite')

# General
all_pkg_ids = ppq.get_all_package_ids()
print('Total packages in DB: {}'.format(len(all_pkg_ids)))

stale_period = '4 years'
print('Removing packages from the DB that have not had a release in {}'.format(stale_period))
ppq.delete_stale_packages()
all_pkg_ids = ppq.get_all_package_ids(True)
print('DB now has {} packages'.format(len(all_pkg_ids)))

# Licenses
print('Checking package licenses:')
to_get = ['APACHE', 'BSD', 'MIT', 'GNU', 'GPL', 'SAME AS', 'COPYRIGHT']
for to in to_get:
    packages = ppq.with_release_containing_license_keyword(to)
    active_packages = set(packages)
    print('\tPackages with \'{0}\' in the field: {1}'.format(to, len(active_packages)))

# Release history
# THIS IS RELIANT ON ONLY THE LATEST RELEASE BEING PRESENT IN THE DB FOR EACH PACKAGE
print('Checking package release history:')
to_get = ['1 month', '3 months', '6 months', '1 year', '2 years', '5 years']
for to in to_get:
    packages = ppq.with_release_in_last_x(to)
    active_packages = set(packages)
    print('\tPackages with a release in the last {0}: {1}'.format(to, len(active_packages)))

print('Release File Type:')
for file_type, count in ppq.get_release_package_type_prevalence():
    print('\t{0}: {1}'.format(file_type, count))

print('People that author the most packages:')
limit = 15
biggest_authors = ppq.biggest_package_author(limit)
for author_name, num_packages in biggest_authors:
    print('\t{0}: {1}'.format(num_packages, author_name))


# Python 2/3 support indicated by classifiers
print('Python 2/3 support based on classifiers:')
with_py2_c = set(ppq.with_py2_classifier())
print('\tPackages with any Python 2 Classifier: {}'.format(len(with_py2_c)))

with_py3_c = set(ppq.with_py3_classifier())
print('\tPackages with any Python 3 Classifier: {}'.format(len(with_py3_c)))

with_py2_not_py3_c = set(ppq.with_py2_but_not_py3_classifier())
print('\tPackages with Py2 Classifier but not Py3 classifier: {}'.format(len(with_py2_not_py3_c)))

with_py3_not_py2_c = set(ppq.with_py3_but_not_py2_classifier())
print('\tPackages with Py3 Classifier but not Py2 classifier: {}'.format(len(with_py3_not_py2_c)))

with_py2_py3_c = set(ppq.with_py2_and_py3_classifier())
print('\tPackages with both Py2 & Py3 classifiers: {}'.format(len(with_py2_py3_c)))

zero_classifiers = set(ppq.packages_with_no_classifiers())
print('\tPackages that have 0 classifiers: {}'.format(len(zero_classifiers)))

zero_ver_classifiers = set(ppq.package_with_no_version_classifiers())
print('\tPackages that have 0 classifiers related to Python version: {}'.format(len(zero_ver_classifiers)))

print('Removing from the DB packages that have already had their version determined via classifiers')
classified = with_py3_c.union(with_py2_c)
ppq.delete_packages_by_ids(classified)
all_packages = ppq.get_all_package_ids(True)
print('DB now has {} packages'.format(len(all_packages)))

with_py2_release = ppq.with_release_python_version_x('%2%')
print('\tPackages with release that supports Python 2: {}'.format(len(with_py2_release)))

with_py3_release = ppq.with_release_python_version_x('%3%')
print('\tPackages with release that supports Python 3: {}'.format(len(with_py3_release)))

with_py2_py3_release = set(with_py2_release).intersection(with_py3_release)
print('\tPackages with release that supports Python 2 & 3: {}'.format(len(with_py2_py3_release)))

with_py3_not_py2_release = ppq.with_py3_release_not_py2()
print('\tPackages with release that supports Python 3 but not 2: {}'.format(len(with_py3_not_py2_release)))

with_py2_not_py3_release = ppq.with_py2_release_not_py3()
print('\tPackages with release that supports Python 2 but not 3: {}'.format(len(with_py2_not_py3_release)))

print('Removing from the DB packages that have already had their version determined via release python_version')
classified = set(with_py2_release).union(set(with_py3_release))
ppq.delete_packages_by_ids(classified)

all_packages = ppq.get_all_package_ids(True)
print('DB now has {} packages'.format(len(all_packages)))
