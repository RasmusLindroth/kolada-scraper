# encoding: utf-8
import math, re
from copy import deepcopy

from statscraper import BaseScraper, Dataset, Dimension, Result, DimensionValue
import numpy as np
import requests

class KoladaScraper(BaseScraper):
    """Sample scraper for statscraper boilerplate."""

    @BaseScraper.on('init')
    def _init(self):
        """Inits variables for class"""
        self.base_url = 'http://api.kolada.se/v2/'
        self.areas = None
        self.areaGroups = None

    def _fetch_itemslist(self, item):
        """Yield a collection or dataset at
        the current cursor position."""
        r = requests.get(self.base_url + '/kpi')
        data = r.json()
        for d in data['values']:
            yield Dataset(d['id'], label=d['title'], blob=d) # blob (data)

    def _fetch_dimensions(self, dataset):
        """Yield the available dimensions in <dataset>."""
        yield Dimension('area_group', label='area group')
        yield Dimension('municipality', label='municipality')
        yield Dimension('kpi', label='indicator')
        yield Dimension('kpi_label', label='indicator name')
        yield Dimension('gender', label='gender')
        yield Dimension('period', label='period')
        yield Dimension('status', label='status')
    
    def _getAllowedAreas(self, type):
        """Caches areas and returns them by type (`K`|`L`)"""
        if self.areas == None:
            self.areas = {
                'K': [],
                'L': []
            }
            data = requests.get(self.base_url + '/municipality').json()
            for row in data['values']:
                r = (row['id'], row['title'])
                if row['type'] in ('K','L'):
                    self.areas[row['type']].append(r)
        if type in self.areas:
            return self.areas[type]
        return []
    
    def _getAllowedAreaGroups(self, type):
        """Caches area groups and returns them by type (`K`|`L`)"""
        if self.areaGroups == None:
            self.areaGroups = {
                'K': [],
                'L': []
            }

            municipalityReg = re.compile(r'[Kk]ommuner')
            regionalReg = re.compile(r'[Ll]andsting')

            data = requests.get(self.base_url + '/municipality_groups').json()
            for row in data['values']:
                r = (row['id'], row['title'])
                if municipalityReg.search(row['title']):
                    self.areaGroups['K'].append(r)
                elif regionalReg.search(row['title']):
                    self.areaGroups['L'].append(r)
        if type in self.areaGroups:
            return self.areaGroups[type]
        return []


    def _fetch_allowed_values(self, dimension):
        """Yield the allowed values for <dimension>."""
        if dimension.id in 'municipality':
            areas = self._getAllowedAreas(dimension.dataset.blob['municipality_type'])

            for a_id, a_name in areas:
                yield DimensionValue(a_id,
                                    dimension,
                                    label=a_name)
        elif dimension.id == 'area_group':
            areas = self._getAllowedAreaGroups(dimension.dataset.blob['municipality_type'])

            for a_id, a_name in areas:
                yield DimensionValue(a_id,
                                    dimension,
                                    label=a_name)




    def _fetch_data(self, dataset, query):
        """Make query for actual data.
        Get all regions and years by default.
        `period` (year) and `municipality` are the only implemented queryable
        dimensions.

        :param query: a dict with dimensions and values to query by.
            Examples:
            {"municipality": ["0180"]}
            {"period": 2016 }
        """
        
        # Make query a dict if isn't
        if isinstance(query, dict) == False:
            query = {}

        # If nothing is set, default to all allowed municipalities
        queryable_dims = ['municipality', 'period', 'area_group']
        if all([x not in query for x in queryable_dims]):
            query['municipality'] = []
            for x in dataset.dimensions['municipality'].allowed_values:
                query['municipality'].append(x.value)

        # Listify queried values (to allow single values in query, like {"year": 2016})
        for key, values in query.items():
            if not isinstance(values, list):
                query[key] = [values]
            # Format all values as strings for url creation
            query[key] = [str(x) for x in query[key]]

        # Validate query
        for dim in query.keys():
            if dim not in queryable_dims:
                raise Exception("You cannot query on dimension '{}'".format(dim))
            # Check if the values are allowed
            if dim in ('municipality', 'area_group'):
                allowed = [x.value for x in dataset.dimensions[dim].allowed_values]
                for dimVal in query[dim]:
                    if dimVal not in allowed:
                        raise Exception("You cannot query on dimension '{}' with '{}'".format(dim, dimVal))

        # base url for query
        next_url = '{}data/kpi/{}'.format(self.base_url, dataset.id)

        # Merge `municipality` and `area_group`
        municipalities = []
        if 'municipality' in query:
            municipalities = municipalities + query['municipality']
        if 'area_group' in query:
            municipalities = municipalities + query['area_group']

        if len(municipalities) > 0:
            next_url += '/municipality/{}'.format(','.join(municipalities))
        if 'period' in query:
            next_url += '/year/{}'.format(','.join(query['period']))

        while next_url:
            print('/GET {}'.format(next_url))
            r = requests.get(next_url)
            r.raise_for_status()
            json_data = r.json()
            for row in json_data['values']:
                for d in row['values']:
                    yield Result(d['value'], {
                            'kpi': dataset.id,
                            'kpi_label': dataset.label,
                            'municipality': row['municipality'],
                            'period': row['period'],
                            'gender': d['gender'],
                            'status': d['status'],
                        }
                    )

            #
            if 'next_page' in json_data:
                next_url = json_data['next_page']
            else:
                next_url = False
