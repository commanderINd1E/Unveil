import pandas as pd
import ast
from Connection import * 
import datetime
from datetime import timedelta


#only for development purposes import already existing log
#log = pd.read_csv('Data/testlog.csv')
#deduplicate
#log=log.drop_duplicates(subset=log.columns.difference(['Name similarity_first']), ignore_index=True)
#reassign index
#log=log.iloc[:,1:]


def RedFlag(log):
    print('Tagging Red Flags')
#Flag 
    #iterate log for all entries imported from the companies house API
    count = int()
    chlog=log[log['source']=='CH API']
    for i in range(len(chlog)):

     # flag if dates of birth are less than 3 year appart
        if isinstance((log.loc[i,'DOB']), datetime.date) and isinstance((log.loc[i,'birthDate']), datetime.date) :
            if timedelta(days=-1075) < log.loc[i,'DOB'] - log.loc[i,'birthDate'] < timedelta(days=1075):
                log.loc[i, 'DOB Flag']=True   

     # Add name redflag if both name similarities are above 80
        if (log.loc[i,'Name similarity'] > 80) & (log.loc[i,'Name similarity_first'] > 80):
            log.loc[i, 'Name Flag']=True

         
    #Flag suspicious geographical regions
        #test officers address
        if log.loc[i,'city']=='London': 
            log.loc[i, 'Area Flag Officer']=True
        # company address tested with multiple companies check
        
    #Flag addresses used multiple times
                #iterate connected companies
        companieslist= ast.literal_eval(log.loc[i,'Company Number'])
        for k in companieslist:
            #get companies adresses
            count=count+1
            url='https://api.company-information.service.gov.uk/company/'+k+'/registered-office-address'
            response = connection(url, count)
            # test company geographical region
            if response.json().get('locality') != None:
                city=response.json()['locality']
                if city=='London': 
                    log.loc[i, 'Area Flag Company']=True
            #get company location and search for other companies
            if response.json().get('address_line_1') != None:
                location = response.json()['address_line_1']          
                count=count+1
                url = "https://api.company-information.service.gov.uk/advanced-search/companies?location=" + location + "&size=" + "5000"
                response2 = connection(url, count)
                # get number of companies found at the address
                if len(response2.json()['items']) > 5:
                    log.loc[i, 'Address Flag']=True
                    # add amount of companies (should be a list)
                    log.loc[i, 'Companies at address']=len(response2.json()['items'])

    #Flag suspicious countries
        with open ('Data/Suspicious Countries.csv') as f:
            blacklist = [line.strip('\n') for line in f]
        if log.loc[i,'country'] in blacklist:
            log.loc[i, 'Country Flag']=True

    #Flag changes in first quarter 2022
        start_date=datetime.datetime.strptime("01-01-2022", "%d-%m-%Y")
        end_date=datetime.datetime.strptime("31-03-2022", "%d-%m-%Y")
        if (not pd.isna(log.loc[i,'appointment date'])) & (log.loc[i,'appointment date'] != 'NaN'):
            if start_date <= datetime.datetime.strptime((log.loc[i,'appointment date']), "%Y-%m-%d") <= end_date:
                log.loc[i, 'date Flag']=True
            elif (not pd.isna(log.loc[i,'resignation date'])) & (log.loc[i,'resignation date'] != 'NaN'):
                if start_date <= datetime.datetime.strptime((log.loc[i,'resignation date']), "%Y-%m-%d") <= end_date:
                    log.loc[i, 'date Flag']=True              

    
        

    return (log)

#RedFlag(log)
