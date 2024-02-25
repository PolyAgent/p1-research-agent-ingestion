import os
from datetime import datetime
import pymongo
import requests

client = pymongo.MongoClient(os.environ['MONGODB_URI'])

from typing import List


MAX_NUMBER_PAPERS = 200 #number of papers to pull

def ingest_arxiv_papers():
    import paperscraper
    papers_df = paperscraper.arxiv.get_arxiv_papers("cat:cs.AI", max_results=MAX_NUMBER_PAPERS, search_options={'sort_by': paperscraper.arxiv.arxiv.SortCriterion.SubmittedDate})    
    
    db = client.arxiv
    collection = db['test_papers']
    records = papers_df.to_dict(orient='records')
    collection.insert_many(records)
    return papers_df 


try:
    SOME_SECRET = os.environ["SOME_SECRET"]
except KeyError:
    SOME_SECRET = "Token not available!"
    #logger.info("Token not available!")
    #raise


if __name__ == "__main__":
    papers = ingest_arxiv_papers()
    webhook_url = 'https://hooks.zapier.com/hooks/catch/6996241/3ej8cwi/'
    r = requests.post(webhook_url, json={'time': str(datetime.now()), 'num_papers': len(papers)})
