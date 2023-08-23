import pandas as pd
import datetime
import ast
import json
import functools
from thefuzz import fuzz
import os.path
from Connection import * 
from multiprocessing import Pool


#fuzzy string analysis of first or last name based on the index
def analyseFuzzy(fullname, name, index):
    o_last_name=(fullname.split())[index]
    n_last_name=(name.split())[index]  
    #use standard ratio since its word by word comparison
    return fuzz.ratio(o_last_name.upper(), n_last_name.upper())

#read Sanctions List (SL) from ods file
def readSL(log):
    sanctionslist = pd.read_excel('Data/UK_Sanctions_List.ods')
    for i in range(0, len(sanctionslist)):
        # get full name from different name segments
        fullname=''
        n=1
        while n<=4:
            name=sanctionslist.loc[i,'Name '+str(n)]
            if type(name)==str and sanctionslist.loc[i, 'Individual, Entity, Ship']=='Individual':
                fullname=fullname+name+' '
            n=n+1
        fullname=fullname[:-1]
        ID = sanctionslist.loc[i, 'Unique ID']
        birthDate = sanctionslist.loc[i,  'D.O.B']
        #add to dataframe if name has been found
        if fullname != '': 
            # also document which names where searched for database
            toadd = pd.DataFrame({'Identifier': ID,  'name': fullname,  'source':'Sanctionslist', 'birthDate': birthDate, 'Name similarity':100},  index=[0])
            log=pd.concat([toadd, log], ignore_index=True)
    print('Sanctionslist read')
    return log

#search entries for each name 
def search_API(fullname,  CH_results, ID, birthdate, count):
  # search CH for the name
  url = "https://api.company-information.service.gov.uk/search/officers?q="+fullname
  count=count+1
  response = connection(url, count)
  df = pd.DataFrame(response.json()['items'])
  #process every hit of the search iteratively
  for i in range (0,  len(df)): 
    name=str(df['title'].loc[df.index[i]])

    #excluding irrelevant hits using fuzzy string     
    Fuzzy_last=analyseFuzzy(fullname, name, -1)
    if Fuzzy_last > 66:
        Fuzzy_first=analyseFuzzy(fullname, name, 0)

        #get date of birth
        if 'date_of_birth' in df.columns:
            if str(df['date_of_birth'].loc[df.index[i]]) != 'nan':
                datedict=ast.literal_eval(str(df['date_of_birth'].loc[df.index[i]]))
                DOB=datetime.datetime(datedict['year'], datedict['month'], 1)
            else:
                DOB='NaN'
        else:
            DOB='NaN'
    
    #get companies an officer is appointed to
        #get officer ID
        officer=str(df['links'].loc[df.index[i]])[20:-15:]
        url = 'https://api.company-information.service.gov.uk/officers/'+officer+'/appointments'
        count=count+1
        #query appointments of officer
        response2 = connection(url, count)
        appointments = pd.json_normalize(response2.json()['items'])
        if 'appointed_on' in appointments:
            date_appointment=appointments['appointed_on']
        else:
            date_appointment='NaN'
        if 'resigned_on' in appointments:
            date_resignation=appointments['resigned_on']
        else:
            date_resignation='NaN'
        company_name=str(list(appointments['appointed_to.company_name']))
        company_number=str(list(appointments['appointed_to.company_number']))
        #associates=
    
    #get address as full text from address snippet
        address=str(df['address_snippet'].loc[df.index[i]])
        #get city from address dictionary (not all entries have country included)
        if df['address'].loc[df.index[i]].get('locality') != None:
            city=df['address'].loc[df.index[i]]['locality']
        else:
            city='NaN'
        #get country from address dictionary (not all entries have country included)
        if df['address'].loc[df.index[i]].get('country') != None:
            country=df['address'].loc[df.index[i]]['country']
        else:
            country='NaN'
        #add all values
        toadd = pd.DataFrame({'Identifier': ID,  'name': name, 'DOB': DOB, 'birthDate': birthdate, 'addresses':address, 'city': city, 'country':country, 'source':'CH API', 'Officer  ID': officer, 'Name similarity':Fuzzy_last, 'Name similarity_first':Fuzzy_first, 'Company Names': company_name, 'Company Number': company_number, 'appointment date':date_appointment, 'resignation date':date_resignation},  index=[0])
        #add results to dataframe
        CH_results=pd.concat([toadd, CH_results], ignore_index=True)
  return [CH_results, count]

#iteratively process and import to dataframe
def import_from_CH(log):
# use counting mechnanism to avoid doubling work
    count = int()
    CH_results=log
    for i in range(0, log.shape[0]):
        fullname= log.loc[i, 'name']
        ID=log.loc[i, 'Identifier']
        birthdate=log.loc[i, 'birthDate']
        CH_results=search_API(fullname,  CH_results,  ID, birthdate, count)
        count=CH_results[1]
        CH_results=CH_results[0]
    return CH_results

#search PSC data for matches

#search batches
def searchPSC_batch(args):
    batch_file, shared_sanctionslist = args

    with open(batch_file, 'r') as file:
        jbatch = [json.loads(line) for line in file if 'data' in line]

    # Convert jbatch to DataFrame
    batch = pd.json_normalize(jbatch)

    # Apply the fuzzy matching and filtering logic using apply() instead of the for loop
    def filter_data(row):
        fullname = row['name']
        ID = row['Identifier']

        def fuzzy_check(name):
            Fuzzy_last = analyseFuzzy(fullname, name, -1)
            Fuzzy_first = analyseFuzzy(fullname, name, 0)
            if Fuzzy_last > 66 and Fuzzy_first > 66:
                PSC_ID = row['data.links.self']
                officer = PSC_ID[47:]
                company_number = PSC_ID[9:-63]
                return pd.Series({'Identifier': ID, 'name': name, 'source': 'PSC', 'Officer ID': officer,
                                  'Name similarity': Fuzzy_last, 'Name similarity_first': Fuzzy_first,
                                  'Company Number': company_number})
            else:
                return pd.Series()

        return batch['data.name'].apply(fuzzy_check)

    # Filter the data and drop empty rows
    filtered_data = filter_data(shared_sanctionslist)
    log = filtered_data.dropna()

    return log

def searchPSC(sanctionslist, log):
    num_files = len(os.listdir(path='./Data/PSC'))
    # search each file separately
    for i in range(num_files):
        print('searching batch '+str(i+1)+' of '+str(num_files))
        # load batch
        jbatch = [json.loads(line) for line in open('Data/PSC/psc-snapshot-2023-07-19_'+str(1)+'of'+str(num_files)+'.txt', 'r')]
        batch = pd.json_normalize(jbatch)
        # iterate sanctionslist
        for j in range(len(sanctionslist)):
            fullname= sanctionslist.loc[j, 'name']
            ID=sanctionslist.loc[j, 'Identifier']
            print(ID)
            for k in range(len(batch)):
                name= batch.loc[k,'data.name']
                #filter not matching last name
                Fuzzy_last=analyseFuzzy(fullname, name, -1)
                if Fuzzy_last > 66:
                    #filter not machting first name
                    Fuzzy_first = analyseFuzzy(fullname, name, 0)
                    if  Fuzzy_first > 66:
                        #add relevant fields
                        PSC_ID = batch.loc[k, ['data.links.self']]
                        officer = PSC_ID[47:]
                        company_number = PSC_ID[9:-63:]
                        toadd = pd.DataFrame({'Identifier': ID,  'name': name, 'source':'PSC', 'Officer  ID': officer, 'Name similarity':Fuzzy_last, 'Name similarity_first':Fuzzy_first, 'Company Number': company_number},  index=[0])
                        log=pd.concat([toadd, log], ignore_index=True)
    return log

