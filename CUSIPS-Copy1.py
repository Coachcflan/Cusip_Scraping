#!/usr/bin/env python
# coding: utf-8

# In[1]:


import scrape
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re
import requests
import json
import unicodedata
import warnings
from bs4.builder import XMLParsedAsHTMLWarning
from collections import Counter


# In[2]:


file_path = "/Users/thesavage/Global_Key/Data/Missing_Cusips.csv"


# In[3]:


# clean the dataframe
dirty_df = pd.read_csv(file_path)[["Comp_ID"]].iloc[:3016]
cleaned_df = dirty_df.drop_duplicates().dropna().astype(str)
cleaned_df["cik"] = cleaned_df["Comp_ID"].apply(lambda x: x.zfill(10))#
cleaned_df = cleaned_df.drop("Comp_ID", axis = 1)


# In[4]:


cleaned_df


# In[5]:


header = {"User-Agent" : "cflan1278@gmail.com"}


# In[6]:


cik = cleaned_df["cik"].values[1]


# In[7]:


def get_company_info(cik):
    company_ep = scrape.cik_endpoint(cik) # endpoint (url)
    request = requests.get(company_ep, headers = header)
    json = request.json() # json format
    return json


# In[8]:


def access_13_txt(cik):
    endpoint = scrape.cik_endpoint(cik)
    filings = scrape.filings_df(endpoint)
    filing_13_text = filings[(filings["form"].str.contains("13")) & (filings["primaryDocument"].str.contains(".txt"))].copy()
    if filing_13_text.shape[0] == 0:
        return "No 13 File"
#     filing_13_text = filings_13[filings_13["primaryDocument"].str.contains(".txt")].copy().sort_values("filingDate", ascending = False)
    filing_13_text["accessionNumber"] = filing_13_text.apply(lambda row: row["accessionNumber"].replace("-", ""), axis = 1)
    filing_13_text["file_endpoint"] = filing_13_text.apply(lambda row: scrape.create_request_link(cik, row['accessionNumber'], row['primaryDocument']), axis=1)
    request_url = filing_13_text["file_endpoint"].values[0]
    return requests.get(request_url, headers = header).content


# In[9]:


def access_13_any(cik):
    endpoint = scrape.cik_endpoint(cik)
    filings = scrape.filings_df(endpoint)
    filing_13 = filings[(filings["form"].str.contains("13")) & (~filings["form"].str.contains("F"))].copy().sort_values("filingDate", ascending = False)
    if filing_13.shape[0] == 0:
        return "No 13 File"
    filing_13["accessionNumber"] = filing_13.apply(lambda row: row["accessionNumber"].replace("-", ""), axis = 1)
    filing_13["file_endpoint"] = filing_13.apply(lambda row: scrape.create_request_link(cik, row['accessionNumber'], row['primaryDocument']), axis=1)
    
    return filing_13


# In[10]:


def clean_text_file(html_string):
    soup = BeautifulSoup(html_string, "html.parser")
    document_text = soup.get_text()
    normalized_text = scrape.restore_windows_1252_characters(unicodedata.normalize('NFKD', document_text)).replace("\n", " ").replace("\t", " ").replace("#", "")
    return normalized_text.upper()#[normalized_text.find("CUSIP") - 200:normalized_text.find("CUSIP") + 100]



# In[11]:


# clean_text = clean_text_file(access_13(cik))


# cusip_pattern = "\s[0-9]{3}[a-zA-Z0-9]{6}\s"

# re.search(cusip_pattern, clean_text).group(0).strip()


# In[12]:


def get_cusip_from_df(df):
    cusip_pattern = ["[0-9A-Z]{1,2}[0-9]{1,2}[[A-Z0-9\s-]{1,7}[0-9]{1}[A-Z0-9\s-][A-Z0-9]\s"]
    for end_point in df["file_endpoint"].values:
        file_content = requests.get(end_point, headers = header).content
        clean_file = clean_text_file(file_content)
        contains_cusip = clean_file.find("CUSIP")
        if contains_cusip == (-1):
            continue
        else:
            for pattern in cusip_pattern:
                matched = re.findall(pattern, clean_file)
                if len(matched) == 0:
                    continue
                else:
                    matched = [item.strip().replace(" ", "").replace("-", "") for item in matched]
                    matched_filtered = [re.search("[0-9A-Z]{9}", item).group(0) for item in matched if re.search("[0-9A-Z]{9}", item) is not None]
                    try:
                        counter = Counter(matched_filtered)
                        # Sort the list by frequency of occurrences in descending order
                        matched = sorted(counter, key=lambda x: counter[x], reverse=True)

                        return matched[0]
                    except:
                        matched_filtered = [re.search("[0-9]{3}[a-zA-Z0-9\s]{6,8}", item).group(0) for item in matched if re.search("[0-9]{3}[a-zA-Z0-9\s]{6,8}", item) is not None]
                        counter = Counter(matched_filtered)
                        # Sort the list by frequency of occurrences in descending order
                        matched = sorted(counter, key=lambda x: counter[x], reverse=True)
            
    return "Cusip Not Found"
            


access_13_any("0000318380")

def get_cusip_txt(string):
    cusip_pattern = ["[0-9A-Z]{1,2}[0-9]{1,2}[[A-Z0-9\s]{1,7}[0-9]{1}[A-Z0-9\s][A-Z0-9]\s"]
    cusip_present = string.find("CUSIP")
    if cusip_present == (-1):
        return False
    else:
        for pattern in cusip_pattern:
                matched = re.findall(pattern, string)
                print(matched)
                if len(matched) == 0:
                    continue
                else:
                    matched = [item.strip().replace(" ", "").replace("-", "") for item in matched]
                    matched_filtered = [re.search("[0-9A-Z]{9}", item).group(0) for item in matched if re.search("[0-9A-Z]{9}", item) is not None]
                    try:
                        counter = Counter(matched_filtered)
                        # Sort the list by frequency of occurrences in descending order
                        matched = sorted(counter, key=lambda x: counter[x], reverse=True)

                        return matched[0]
                    except:
                        matched_filtered = [re.search("[0-9]{3}[a-zA-Z0-9\s]{6,8}", item).group(0) for item in matched if re.search("[0-9]{3}[a-zA-Z0-9\s]{6,8}", item) is not None]
                        counter = Counter(matched_filtered)
                        # Sort the list by frequency of occurrences in descending order
                        matched = sorted(counter, key=lambda x: counter[x], reverse=True)
                        
        return False
        

'''
This is the function that we are working up to
'''

def scrape_cusips(list_of_ciks):
    df_dict = {"cik" : [], "company_name" : [], "cusip" : []}
    for cik in list_of_ciks:
        try:
            #print(cik)
            company_name = get_company_info(cik)["name"]
            
            text_file = access_13_txt(cik)
            if text_file == "No 13 File":
                cusip = text_file
            else:
                df_13 = access_13_any(cik)
                cusip = get_cusip_from_df(df_13)

            print(cusip)
            df_dict["cik"].append(cik)
            df_dict["company_name"].append(company_name)
            df_dict["cusip"].append(cusip[:10])



        except Exception as e:
            print(cik)
            print(e)
            company_name = get_company_info(cik)["name"]
            cusip = "No 13 File"
            df_dict["cik"].append(cik)
            df_dict["company_name"].append(company_name)
            df_dict["cusip"].append(cusip)
            
            
    return pd.DataFrame(df_dict)





        
        

