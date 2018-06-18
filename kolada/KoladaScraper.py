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
        self._municipalities = None
        self._municipality_groups = None

    def _fetch_itemslist(self, item):
        """Yield a collection or dataset at
        the current cursor position."""
        r = requests.get(self.base_url + '/kpi')
        data = r.json()
        for d in data['values']:
            yield Dataset(d['id'], label=d['title'], blob=d) # blob (data)

    def _fetch_dimensions(self, dataset):
        """Yield the available dimensions in <dataset>."""
        yield Dimension('municipality_groups', label='municipality groups')
        yield Dimension('municipality', label='municipality')
        yield Dimension('kpi', label='indicator')
        yield Dimension('kpi_label', label='indicator name')
        yield Dimension('gender', label='gender')
        yield Dimension('period', label='period')
        yield Dimension('status', label='status')
    
    def _get_allowed_municipalities(self, type):
        """Caches municipalities and returns them by type (`K`|`L`)"""
        if self._municipalities == None:
            self._municipalities = {
                'K': [],
                'L': []
            }
            data = requests.get(self.base_url + '/municipality').json()
            for row in data['values']:
                r = (row['id'], row['title'])
                if row['type'] in ('K','L'):
                    self._municipalities[row['type']].append(r)
        if type in self._municipalities:
            return self._municipalities[type]
        return []
    
    def _get_allowed_municipality_groups(self, type):
        """Caches municipality groups and returns them by type (`K`|`L`)"""
        if self._municipality_groups == None:
            self._municipality_groups = {
                'K': [],
                'L': []
            }

            municipalityReg = re.compile(r'[Kk]ommuner')
            regionalReg = re.compile(r'[Ll]andsting')

            data = requests.get(self.base_url + '/municipality_groups').json()
            for row in data['values']:
                r = (row['id'], row['title'])
                if municipalityReg.search(row['title']):
                    self._municipality_groups['K'].append(r)
                elif regionalReg.search(row['title']):
                    self._municipality_groups['L'].append(r)
        if type in self._municipality_groups:
            return self._municipality_groups[type]
        return []


    def _fetch_allowed_values(self, dimension):
        """Yield the allowed values for <dimension>."""
        if dimension.id == 'municipality':
            municipalities = self._get_allowed_municipalities(
                dimension.dataset.blob['municipality_type']
            )

            for m_id, m_name in municipalities:
                yield DimensionValue(m_id,
                                    dimension,
                                    label=m_name)
        elif dimension.id == 'municipality_groups':
            municipality_groups = self._get_allowed_municipality_groups(
                dimension.dataset.blob['municipality_type']
            )

            for m_id, m_name in municipality_groups:
                yield DimensionValue(m_id,
                                    dimension,
                                    label=m_name)




    def _fetch_data(self, dataset, query):
        """Make query for actual data.
        Get all regions and years by default.
        `period` (year), `municipality` and `municipality_groups` are the only 
        implemented queryable dimensions.

        :param query: a dict with dimensions and values to query by.
            Examples:
            {"municipality": ["0180"]}
            {"period": 2016 }
        """
        
        # Make query a dict if it already isn't
        if isinstance(query, dict) == False:
            query = {}

        # If nothing is set, default to all allowed municipalities
        queryable_dims = ['municipality', 'period', 'municipality_groups']
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
            if dim in ('municipality', 'municipality_groups'):
                allowed = [x.value for x in dataset.dimensions[dim].allowed_values]
                for dimVal in query[dim]:
                    if dimVal not in allowed:
                        raise Exception("You cannot query on dimension '{}' with '{}'".format(dim, dimVal))

        # base url for query
        next_url = '{}data/kpi/{}'.format(self.base_url, dataset.id)

        # Merge `municipality` and `municipality_groups`
        municipalities = []
        if 'municipality' in query:
            municipalities = municipalities + query['municipality']
        if 'municipality_groups' in query:
            municipalities = municipalities + query['municipality_groups']

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
