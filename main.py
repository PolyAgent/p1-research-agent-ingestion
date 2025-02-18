import os, sys, traceback
from datetime import datetime
import pymongo
import requests
import paperscraper

client = pymongo.MongoClient(os.environ['MONGODB_URI'])

from typing import List


MAX_NUMBER_PAPERS = 200 #number of papers to pull

def ingest_arxiv_papers():
    categories_to_pull = ['cs.AI', 'cs.LG', 'cs.CL']
    count = 0
    for cat in categories_to_pull:
        papers_df = paperscraper.arxiv.get_arxiv_papers(f"cat:{cat}", max_results=MAX_NUMBER_PAPERS, search_options={'sort_by': paperscraper.arxiv.arxiv.SortCriterion.SubmittedDate})    
        # add url
        papers_df['url'] = papers_df["doi"].apply(lambda row: "https://arxiv.org/abs/"+row.split("/arXiv.")[1])
        papers_df['pdf_url'] = papers_df["doi"].apply(lambda row: "https://arxiv.org/pdf/"+row.split("/arXiv.")[1]+".pdf") 
        
        db = client.arxiv
        collection = db['papers_for_review']
        records = papers_df.to_dict(orient='records')
        for i,record in enumerate(records):
            try:
                print(record['doi'])
                collection.insert_one(record)
                count +=1
            except:
                print("duplicate found", record['doi'])
    return count

if __name__ == "__main__":
    print(f"test_secret: {os.environ['TEST_SECRET']}")
    succesful_injection_count = ingest_arxiv_papers()
    webhook_url = os.environ["ZAPIER_WEBHOOK"]
    r = requests.post(webhook_url, json={'time': str(datetime.now()), 'succesful_count': succesful_injection_count})
