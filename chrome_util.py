import os
import sqlite3
import operator
import shutil
from datetime import datetime
from pytz import timezone 
import pandas as pd
import boto3
import traceback 


def get_dt():
    # EST, Aisa/Kolkata -- are accepted values
    return datetime.now(timezone('EST')).strftime('%Y-%m-%d-%H%M%S')


if __name__ == '__main__':
    print(f'{get_dt()} ::: execution started')

    ######################################
    # Setting up the paths               #
    ######################################
    chrome_history_path = os.path.expanduser('~')+r"\AppData\Local\Google\Chrome\User Data\Default"
    temp_path = os.path.expanduser('~')+"\AppData\Local\Temp"
    print(f'{get_dt()} ::: chrome_history_path set to--> {chrome_history_path}')
    print(f'{get_dt()} ::: temp_path set to--> {temp_path}')

    try:
        ###########################################################
        # Take a copy of history file and create DB               #
        ###########################################################
        shutil.copyfile(os.path.join(chrome_history_path, 'History'),os.path.join(temp_path,'History.db'))
        history_db = os.path.join(temp_path,'History.db')
        print(f'{get_dt()} ::: copied the history file to {temp_path}')

        ############################################################################
        # Identify what was the last processed record by this script               #
        ############################################################################
        if os.path.exists(temp_path+r'\last_processed_time.txt'):
            print(f'{get_dt()} ::: found last_processed_time file at {temp_path}')
            with open(temp_path+'\last_processed_time.txt') as f:
                lpt_fetch = f.read()
                print(f'{get_dt()} ::: lpt_fetched is {lpt_fetch}')
                lpt = lpt_fetch
        else:
            print(f'{get_dt()} ::: did NOT find last_processed_time file at {temp_path}')
            lpt = 0
            print(f'{get_dt()} ::: lpt_fetched set to ZERO')
            

        #######################################
        # Connect to the DB                   #
        #######################################
        conn = sqlite3.connect(history_db)


        ##############################################
        # Fetch the recent history                   #
        ##############################################
        get_recent_history_query = pd.read_sql_query(f"SELECT urls.url, urls.title as page_title, visits.visit_time  FROM urls, visits WHERE urls.id = visits.url and visits.visit_time > {lpt};", conn)

        df = pd.DataFrame(get_recent_history_query, columns = ['url', 'page_title', 'visit_time'])
        df["domain"] = df.url.str.replace("www.","").str.replace(".com","").str.split("/").str[2]
        df["visit_time"] = (df.visit_time//1000000)-11644473600
        df["visit_time"] = pd.to_datetime(df["visit_time"], unit='s', utc = True)
        df["visit_time"] = df["visit_time"].dt.tz_convert('US/Eastern')
        df["machine_name"] = os.environ['COMPUTERNAME'] # 'Ravi'   #
        #df.drop('url', axis=1, inplace=True)


        #######################################################
        # Create csv local - Upload to S3  - Clean local      #
        #######################################################
        computer_name = os.environ['COMPUTERNAME']
        suffix = datetime.now().strftime('%Y-%m-%d-%H%M%S')
        
        df.drop_duplicates(inplace = True)
        df.to_csv(f"{temp_path}/{computer_name}-{suffix}.csv", header = True, index = False)
        print(f'{get_dt()} ::: Temp csv created')
        
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(Filename = f'{temp_path}/{computer_name}-{suffix}.csv',
                                   Bucket = 'aws-ec2-logs-307592787224',
                                   Key = f'ec2-search-logs/{computer_name}-{suffix}.csv')
        print(f'{get_dt()} ::: temp csv "{computer_name}-{suffix}.csv" uploaded to s3')
        
        os.remove(f'{temp_path}/{computer_name}-{suffix}.csv')
        print(f'{get_dt()} ::: Temp csv deleted')


        #######################################################################
        # Identify the max record processed now and store in temp file        #
        #######################################################################
        get_max_visit_time_query = pd.read_sql_query(f"SELECT max(visits.visit_time) as lpt FROM urls, visits WHERE urls.id = visits.url;", conn)
        df2 = pd.DataFrame(get_max_visit_time_query, columns =['lpt'])
        lpt_value = df2['lpt'][0]
        print(f'{get_dt()} ::: In this run lpt is {lpt_value}')

        with open(temp_path+'\last_processed_time.txt', 'w') as f:
            f.write(f'{lpt_value}')

        print(f'{get_dt()} ::: Updated last_processed_time file with {lpt_value}')
        print(f'{get_dt()} ::: EOP')
    except Exception as e :
        print('\n#####################\n')
        print(f'Error occured {e}')
        print('\n#####################\n')
        print(traceback.format_exc())
