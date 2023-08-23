from requests.auth import HTTPBasicAuth
import requests
import timeit
from time import sleep
import os

#API connection properties
#query if API keyfile already exists else prompt for user input
if os.path.isfile('APIkey.txt'):
    with open('APIkey.txt','r') as file:
        access_token=file.read()
else:
    access_token = input ('Paste your Companies House API-Key: ')
    with open('APIkey.txt','w') as file:
        file.write(access_token)
username = access_token
password = ""
size = "5000"
basic = HTTPBasicAuth(username, password)

#connecction to the companieshouse API
def connection (url, count):
    print(count)
    try:
        response=requests.get(url, auth=basic)
        response.raise_for_status()
        if response.status_code == 200:
            return response 
    #connection failes
    except requests.exceptions.ConnectionError:
        print('No connection possible, retrying in 30 sec')
        sleep (30)
        return connection(url, count)        
    #http status codes
    except:   
        #request limit reached
        if response.status_code == 429:
            #calculate how much seconds to go til slightly more than 5 minutes are completed
            distance = 302 - (timeit.default_timer() % 302)
            print('request limit reached,  waiting '+str(distance)+' seconds')          
            sleep(distance) 
            #print('request limit reached,  waiting 5 minutes')
            #sleep(150)
            #print('2,5 minutes to go')
            #sleep(150)
        #no matches
        elif response.status_code == 404:
            print('No hits')
            # use a url with exactly one hit since error is produced when looking for address count
            url='https://api.company-information.service.gov.uk/advanced-search/companies?location=51 Magdelene Gardens&size=5000'
        #other connection error
        else:
            print('ConnectiomError - retrying')
            sleep (10)
        return connection(url, count)