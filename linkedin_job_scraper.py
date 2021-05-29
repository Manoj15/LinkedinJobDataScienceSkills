import logging
import json
import pandas as pd
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, RemoteFilters

# Change root logger level (default is WARN)
logging.basicConfig(level = logging.INFO)

job_data = {'title' : [], 'company' : [], 'date_posted' : [], 'job_desc' : [], 'link' : []}

def on_data(data: EventData):
    # jobs_df = jobs_df.append(pd.DataFrame({'title' : data.title, 'company' : data.company, 'date_posted' : data.date, 'link' : data.link, 'jd' : data.description}))
    print('[ON_DATA]', data.title, data.company, data.date, data.link, len(data.description))
    job_data['title'].append(data.title)
    job_data['company'].append(data.company)
    job_data['date_posted'].append(data.date)
    job_data['job_desc'].append(data.description)
    job_data['link'].append(data.link)

def on_error(error):
    print('[ON_ERROR]', error)


def on_end():
    print('[ON_END]')


scraper = LinkedinScraper(
    chrome_executable_path='C:\\Users\\manho\\Downloads\\chromedriver_win32\\chromedriver', # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver) 
    chrome_options=None,  # Custom Chrome options here
    headless=True,  # Overrides headless mode only if chrome_options is None
    max_workers=4,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
    slow_mo=1.3,  # Slow down the scraper to avoid 'Too many requests (429)' errors
)

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

queries = [Query(
        query='Data Scientist',
        options=QueryOptions(
            locations=['United States'],
            optimize=False,
            limit=100,
            filters=QueryFilters(relevance=RelevanceFilters.RELEVANT,
                time=TimeFilters.MONTH,
                type=[TypeFilters.FULL_TIME, TypeFilters.INTERNSHIP],
                experience=None,                
            )
        )
    ),
]

scraper.run(queries)
with open('job_data.json', 'w') as fp:
    json.dump(job_data, fp)