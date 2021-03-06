This is a scraper for statistical data from http://api.kolada.se/v2/ built on top of the `Statscraper package <https://github.com/jplusplus/statscraper>`.

Install
-------

  pip install -r requirements.txt

Example usage
-------------

.. code:: python

  from kolada import KoladaScraper

  scraper = KoladaScraper()

  dataset = scraper.items["N00002"] # pass a KPI id

  # Query by year
  data = dataset.fetch({
    'period': [2016, 2015],
  })

  # ...or by municipality
  towns = [x.value for x in dataset.dimensions['municipality'].allowed_values]
  data = dataset.fetch({
    'municipality': towns[:5],
  })

  # ...or by municipality_groups
  groups = [x.value for x in dataset.dimensions['municipality_groups'].allowed_values]
  data = dataset.fetch({
    'municipality_groups': groups[:5],
  })


  # ... or by all three
  data = dataset.fetch({
    'period': [2016, 2015],
    'municipality': towns[:5],
    'municipality_groups': groups[:5],
  })

  # And then do something with the results.
  print(data.pandas)

TODO
----

- Add more allowed values
- Implement errors when unallowed values are passed
- Implement regions
- Update `chunkify()` function, to make url building better
