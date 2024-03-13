import pymongo
from datetime import datetime, timedelta
import os
import pandas as pd
import fireworks.client

client = pymongo.MongoClient(os.environ['MONGODB_URI'])
fireworks.client.api_key = os.environ['API_KEY']

def pull_daily_papers():
    db = client.arxiv
    collection = db['papers_for_review']
    # pull documents with the date since yesterday.
    # Usually mongo collection contains one day+ old documents
    results = collection.find({"date": {"$gte": (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")}})
    return results

def generate_summary(title, abstract):
    content = f"""
        Here's an abstract of a research paper. Your goal is optimize this abstract to be the most condensed version without verbose redundancy of language so that it's designed for a machine to read and understand in the most compact way (3-4 sentences, under 60 words total). Output only resulting abstract.

Title: {title}

Abstract: 
        {abs}

        
    """
    completion = fireworks.client.ChatCompletion.create(
        model="accounts/fireworks/models/mixtral-8x7b-instruct",
        messages=[
            {
            "role": "user",
            "content": content}
        ],
        stop=["<|im_start|>","<|im_end|>","<|endoftext|>"],
        stream=True,
        n=1,
        top_p=1,
        top_k=40,
        presence_penalty=0,
        frequency_penalty=0,
        prompt_truncate_len=1024,
        context_length_exceeded_behavior="truncate",
        temperature=0.9,
        max_tokens=4000
    )
    output = list(completion)
    msg = ""
    for chunk in output:
        curr = chunk.choices[0].delta.content
        if curr:
            msg += curr
    return msg

    
def generate_daily_digest(summaries):
    content = f"""
    You are a research agent that is analyzing hundreds of research papers that are published for the day and your task is to produce the most useful and compact explanation of what kind of research is being published. Don't copy information, digest it and analyze. Keep it under 200 words. Your target audience is AI researchers trying to stay on top of what's happening in the field.

    List of summaries: {summaries}

        
    """
    completion = fireworks.client.ChatCompletion.create(
        model="accounts/fireworks/models/mixtral-8x7b-instruct",
        messages=[
            {
            "role": "user",
            "content": content}
        ],
        stop=["<|im_start|>","<|im_end|>","<|endoftext|>"],
        stream=True,
        n=1,
        top_p=1,
        top_k=40,
        presence_penalty=0,
        frequency_penalty=0,
        prompt_truncate_len=32768,
        context_length_exceeded_behavior="truncate",
        temperature=0.9,
        max_tokens=32768
    )
    output = list(completion)
    msg = ""
    for chunk in output:
        curr = chunk.choices[0].delta.content
        if curr:
            msg += curr
    return msg
    
if __name__ == "__main__":
    df = pd.DataFrame(pull_daily_papers())
    list_summaries = []
    for _, row in df.iterrows():
        summary = generate_summary(row['title'], row['abstract'])
        row['summary'] = summary
        list_summaries.append(summary)
    
    digest = generate_daily_digest(list_summaries)
    print(digest)
