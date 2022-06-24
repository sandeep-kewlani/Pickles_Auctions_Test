"""
  date:14-June-2022
  name:SandeepKumarKewlani
  Object:This script is used for reading the data from
  csv/json files and put into mysql etl automation.
  Text -> SQL .
"""

import os,sys,subprocess
import mysql.connector as sql
import csv
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import yaml,zipfile

start_time = datetime.now()
print("Script started time {} ".format(start_time))

def createLogDir():
    """
    :description: This method is used to
    create the log dir if doesn't exist
    :return:
    """
    check_dir = os.path.isdir('logs')
    if not check_dir:
        os.makedirs('logs')


def createLogFile():
    """
    :description: This method is used to
    check the log file size if more than 5 MB
    then will create a new file and logs will
    be written into that file
    :return: Filename : String
    """
    createLogDir()
    cmd = 'ls logs -t1 | head -n 1'
    filename = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT, universal_newlines=True).stdout
    cmd2 = 'stat --format=%s logs/'+filename
    prev_file_size = subprocess.run(cmd2, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True).stdout
    if int(prev_file_size) > 5000000 or not filename:
        log_file = datetime.now().strftime('icore_deposit_db_pull_%d_%m_%Y.log')
        return "logs/"+str(log_file)
    else:
        return "logs/"+str(filename)


logging.basicConfig(filename=createLogFile(), format='%(asctime)s %(message)s', filemode='w')
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

def readFromProperty():
    try:
        logger.info("Reading data from config file")
        with open("config/FileUploadConfig.yaml") as file:
            propObj = yaml.safe_load(file)
            return propObj
    except Exception as e:
        logger.error("Exception found while reading from property file : ",str(e))


def readCSVFile(csvFile):
    """
    :description: This method is used to
    read csv file from dm shared drive and return object
    :param : property object : prop
    :return: CSVData : Object
    """
    try:
        logger.info("Reading data from file")
        with open(csvFile) as csv_file:
            csv_data = csv.reader(csv_file,delimiter='^')
            logger.info("Total number of rows in the file : ", len(list(csv_data)))
            return csv_data
    except Exception as e:
        logger.error("Exception found to read data from file : "+str(e))

def writeDataIntoMySql(prop, csvDF):
    """
    :description: This method is used to
    write data into sql using pandas inbuilt function
    :return: Connection SqlConnection
    """
    try:
        user = prop['db_properties']['user']
        password = prop['db_properties']['password']
        database = prop['db_properties']['database']
        host = prop['db_properties']['host']

        logger.info("Writing data into the table")
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}", echo=False)
        logger.info("Sql connection established and connected to database")
        csvDF.to_sql("temp", con=engine, if_exists='append', chunksize=1000, index=False)
    except Exception as e:
        logger.error("Exception found to connect mysql : "+str(e))

if __name__ == '__main__':
    logger.info("Program starts from main method")
    try:
        logger.info("Script start time {} ".format(start_time))

        prop = readFromProperty()

        for value in prop['files']:
            for fileObj in prop['files'][value]:
                #Reading data from CSV File
                if str(value) == 'csv':
                    fileLocation = fileObj['filelocation']
                    pwd = fileObj['password'] if fileObj.__contains__('password') else ""
                    sep = fileObj['delimiter'] if fileObj.__contains__('delimiter') else ""
                    readCSVFile(fileLocation, pwd, sep)
                    writeDataIntoMySql(prop, csvDF)

                # Reading data from JSON File
                elif str(value) == 'json':
                    jsonLoc = fileObj['file_directory']
                    if jsonLoc is None:
                        logger.info("Zip file is not available")
                        continue
                    pwd = fileObj['password'] if fileObj.__contains__('password') else ""
                    sep = fileObj['delimiter'] if fileObj.__contains__('delimiter') else ""
                    csvDF = readCSVFile(jsonLoc, pwd, sep)
                    writeDataIntoMySql(prop, csvDF)
        end_time = datetime.now()
        logger.error("Script end time {} ".format(end_time))
        print("Script end time {} ".format(end_time))
        logger.error("Total time taken {} ".format(end_time - start_time))


    except Exception as e:
        logger.error("Exception found in main method : "+str(e))
