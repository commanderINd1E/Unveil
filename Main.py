import pandas as pd
import timeit
from RedFlag import * 
from SearchCH import * 


#timer for runtime measurement
start = timeit.default_timer()


#create logfile
log = pd.DataFrame(columns =['Identifier'])        
        

#read sanctionsdata
sanctionslist = readSL(log)
#import CH data
log=import_from_CH(sanctionslist)


#clean data
#drop duplicates (based on different queries the first name similarity is rated different blocking duplicates to be removed efficiently)
log=log.drop_duplicates(subset=log.columns.difference(['Name similarity_first']), ignore_index=True)
#reassign index
log=log.reset_index() 


# Tag red flags
log=RedFlag(log)


#create human readable output

log_csv_data = log.to_csv('Data/results.csv', index = True)
print('\nCSV String:\n', log_csv_data)

stop = timeit.default_timer()
print('Time: ', stop - start)

# not in use since the runtime is to high


#log=searchPSC(sanctionslist, log)

#log_csv_data = log.to_csv('Data/PSC_log.csv', index = True)
#print('\nCSV String:\n', log_csv_data)

#stop = timeit.default_timer()
#print('Time: ', stop - start)
