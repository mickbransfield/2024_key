# Import modules
import json
import requests
import pandas as pd
import datetime
import os
import zipfile #FEC
import urllib.request #FEC
from io import BytesIO #FEC


#### PREDICTIT ####

# Pull in market data from PredictIt's API
URL = "https://www.predictit.org/api/marketdata/all/"
response = requests.get(URL)
jsondata = response.json()

# Replace null values with zero
def dict_clean(items):
    result = {}
    for key, value in items:
        if value is None:
            value = 0
        result[key] = value
    return result
dict_str = json.dumps(jsondata)
jsondata = json.loads(dict_str, object_pairs_hook=dict_clean)

# Market data by contract/price in dataframe
data = []
for p in jsondata['markets']:
	for k in p['contracts']:
		data.append([p['id'],p['name'],p['url'],k['id'],k['name'],k['bestBuyYesCost'],k['bestBuyNoCost'],k['bestSellYesCost'],k['bestSellNoCost'],p['timeStamp'],p['status']])

# Pandas dataframe named 'PredictIt_df'
PredictIt_df = pd.DataFrame(data)

# Update dataframe column names
PredictIt_df.columns=['Market_ID','Market_Name','Market_URL','Contract_ID','Contract_Name','bestBuyYesCost','bestBuyNoCost','BestSellYesCost','BestSellNoCost','Time_Stamp','Status']

# Filter PredictIt_df to Market #7053 (Who will win the 2024 Republican presidential nomination?)
PredictIt_df = PredictIt_df[PredictIt_df['Market_ID'] == 7053]

# Workaround to fix bestBuyNoCost issue
PredictIt_df['bestBuyNoCost'] = PredictIt_df['bestBuyNoCost'].replace(0, 1)

# PredictIt price estimate from the four prices listed
PredictIt_df['PI_est'] = ((PredictIt_df['bestBuyYesCost'] + PredictIt_df['BestSellYesCost'] + 1 - PredictIt_df['BestSellNoCost'] + 1 - PredictIt_df['bestBuyNoCost']) / 4).round(2)

# Rename PredictIt contract column
PredictIt_df = PredictIt_df.rename({'Contract_ID': 'PI_contract'}, axis=1)


#### 538 AVERAGE ####

# Read in polling averages
polling_avg = pd.read_json('https://projects.fivethirtyeight.com/polls/president-primary-r/2024/national/polling-average.json')

#convert 'date' to datetime
polling_avg['date'] = pd.to_datetime(polling_avg['date']) 

# Sort by date and drop duplicates
polling_avg = polling_avg.sort_values(by=['date'], ascending=False).drop_duplicates(['candidate'], keep='first')

# Rename candidate name field to match
polling_avg = polling_avg.rename({'candidate': 'answer'}, axis=1)
polling_avg = polling_avg.rename({'pct_estimate': 'polling_avg'}, axis=1)


#### 538 LATEST ####

# Pull in polling data from 538
pres_polling = pd.read_csv('https://projects.fivethirtyeight.com/polls-page/data/president_primary_polls.csv')

# Filter to national GOP primary polls
pres_polling = pres_polling[pres_polling['race_id'] == 8916]

# Drop extraneous columns
pres_polling = pres_polling.drop(['state', 'poll_id', 'subpopulation', 'source', 'cycle', 'ranked_choice_round', 'election_date' ,'stage', 'party', 'office_type', 'pollster_id', 'sponsor_ids', 'sponsor_candidate_party', 'sponsor_candidate_id','sponsors','display_name', 'pollster_rating_id', 'pollster_rating_name', 'fte_grade', 'sample_size', 'population', 'population_full', 'methodology', 'seat_number', 'seat_name', 'start_date', 'sponsor_candidate', 'internal', 'partisan', 'tracking', 'nationwide_batch', 'ranked_choice_reallocated', 'notes', 'url'], axis=1)

# Rename 538 'pct' column to '538_latest_poll'
pres_polling = pres_polling.rename({'pct': '538_latest_poll'}, axis=1)

# Rename 538 'end_date' column to '538_poll_date'
pres_polling = pres_polling.rename({'end_date': 'poll_end_date'}, axis=1)

# Filter to most recent poll for Biden & Trump
# create a count column for 'question_id' to work around "Delaware problem": multiple matchups in same survey
pres_polling['poll_end_date'] = pd.to_datetime(pres_polling['poll_end_date']) #convert 'created_at' to datetime
pres_polling['Count'] = pres_polling.groupby('question_id')['question_id'].transform('count')
pres_polling = pres_polling[(pres_polling.Count > 1)]
pres_polling = pres_polling.sort_values(by=['poll_end_date'], ascending=False).drop_duplicates(['answer'], keep='first')


#### FEC CANDIDATE COMMITTEES ####

zip_url = 'https://www.fec.gov/files/bulk-downloads/2024/weball24.zip'
text_file_to_extract = 'weball24.txt'
response = requests.get(zip_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Read the content of the ZIP file
    zip_data = BytesIO(response.content)
	# Open the ZIP file using the zipfile library
    with zipfile.ZipFile(zip_data, 'r') as zip_file:
        # Check if the text file exists in the ZIP folder
        if text_file_to_extract in zip_file.namelist():
            # Extract the text file
            with zip_file.open(text_file_to_extract) as extracted_file:
                # Read the extracted text file into a DataFrame using pandas
                df = pd.read_csv(extracted_file, sep='|')
else:
    print("Failed to retrieve the ZIP file. Status code:", response.status_code)

# Add column names and rename dataframe
column_names= ['CAND_ID', 'CAND_NAME', 'CAND_ICI', 'PTY_CD', 'CAND_PTY_AFFILIATION', 'TTL_RECEIPTS', 'TRANS_FROM_AUTH', 'TTL_DISB', 'TRANS_TO_AUTH', 'COH_BOP', 'COH_COP', 'CAND_CONTRIB', 'CAND_LOANS', 'OTHER_LOANS', 'CAND_LOAN_REPAY', 'OTHER_LOAN_REPAY', 'DEBTS_OWED_BY', 'TTL_INDIV_CONTRIB', 'CAND_OFFICE_ST', 'CAND_OFFICE_DISTRICT', 'SPEC_ELECTION', 'PRIM_ELECTION', 'RUN_ELECTION', 'GEN_ELECTION', 'GEN_ELECTION_PRECENT', 'OTHER_POL_CMTE_CONTRIB', 'POL_PTY_CONTRIB', 'CVG_END_DT', 'INDIV_REFUNDS', 'CMTE_REFUNDS']
FEC_cand_df = pd.DataFrame(df.values, columns = column_names )

# Rename FEC candidate columns used
FEC_cand_df = FEC_cand_df.rename({'TTL_RECEIPTS': 'cand_TTL_RECEIPTS'}, axis=1)
FEC_cand_df = FEC_cand_df.rename({'TTL_DISB': 'cand_TTL_DISB'}, axis=1)
FEC_cand_df = FEC_cand_df.rename({'CAND_ID': 'fec_cand_id'}, axis=1)


#### FEC SUPER PACS ####

zip_url = 'https://www.fec.gov/files/bulk-downloads/2024/webk24.zip'
text_file_to_extract = 'webk24.txt'
response = requests.get(zip_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Read the content of the ZIP file
    zip_data = BytesIO(response.content)
	# Open the ZIP file using the zipfile library
    with zipfile.ZipFile(zip_data, 'r') as zip_file:
        # Check if the text file exists in the ZIP folder
        if text_file_to_extract in zip_file.namelist():
            # Extract the text file
            with zip_file.open(text_file_to_extract) as extracted_file:
                # Read the extracted text file into a DataFrame using pandas
                df_pac = pd.read_csv(extracted_file, sep='|')
else:
    print("Failed to retrieve the ZIP file. Status code:", response.status_code)

# Add column names and rename dataframe
column_names= ['CMTE_ID', 'CMTE_NM', 'CMTE_TP', 'CMTE_DSGN', 'CMTE_FILING_FREQ', 'TTL_RECEIPTS', 'TRANS_FROM_AFF', 'INDV_CONTRIB', 'OTHER_POL_CMTE_CONTRIB', 'CAND_CONTRIB', 'CAND_LOANS', 'TTL_LOANS_RECEIVED', 'TTL_DISB', 'TRANF_TO_AFF', 'INDV_REFUNDS', 'OTHER_POL_CMTE_REFUNDS', 'CAND_LOAN_REPAY', 'LOAN_REPAY', 'COH_BOP', 'COH_COP', 'DEBTS_OWED_BY', 'NONFED_TRANS_RECEIVED', 'CONTRIB_TO_OTHER_CMTE', 'IND_EXP', 'PTY_COORD_EXP', 'NONFED_SHARE_EXP', 'CVG_END_DT']
FEC_pac_df = pd.DataFrame(df_pac.values, columns = column_names )

# Rename FEC candidate columns used
FEC_pac_df = FEC_pac_df.rename({'TTL_DISB': 'pac_TTL_DISB'}, axis=1)
FEC_pac_df = FEC_pac_df.rename({'TTL_RECEIPTS': 'pac_TTL_RECEIPTS'}, axis=1)
FEC_pac_df = FEC_pac_df.rename({'CMTE_ID': 'fec_pac_id'}, axis=1)


### 2024 KEY ###

# Read in GOP primary candidate keys
key = pd.read_csv('https://raw.githubusercontent.com/mickbransfield/2024_key/main/2024_GOP_Primary_Key.txt', sep='|', index_col=False)

# Merge data to key
df = pd.merge(key,PredictIt_df[['PI_contract','PI_est']],on='PI_contract', how='left')
df = pd.merge(df,polling_avg[['answer','polling_avg']],on='answer', how='left')
df = pd.merge(df,pres_polling[['candidate_id','538_latest_poll', 'poll_end_date', 'pollster']],on='candidate_id', how='left')
df = pd.merge(df,FEC_cand_df[['fec_cand_id','cand_TTL_RECEIPTS', 'cand_TTL_DISB']],on='fec_cand_id', how='left')
df = pd.merge(df,FEC_pac_df[['fec_pac_id','pac_TTL_RECEIPTS', 'pac_TTL_DISB']],on='fec_pac_id', how='left')

# Combine receipts and disbursements
df['COMB_RECEIPTS'] = df['cand_TTL_RECEIPTS'] + df['pac_TTL_RECEIPTS']
df['COMB_DISB'] = df['cand_TTL_DISB'] + df['pac_TTL_DISB']

# Write dataframe with timestamp
snapshotdate = datetime.datetime.today().strftime("%Y-%m-%d %H_%M_%S")
df.to_csv('GOP_2024_Primary_candidates_'+snapshotdate+'.csv', index=False)