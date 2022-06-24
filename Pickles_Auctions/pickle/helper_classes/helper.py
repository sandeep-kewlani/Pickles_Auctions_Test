from csv import reader
import json
import os


class Pickle_Helper:
    def __init__(self):
        pass

    def chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def read_csv_file(self, directory, dd=',', skip_header=False):
        with open(directory, 'r', encoding='latin1') as read_obj:
            csv_reader = reader(read_obj, delimiter=dd)

            list_of_tuples = list(map(tuple, csv_reader))
            if(skip_header):
                list_of_tuples.pop(0)
            return list_of_tuples

    def read_config(self, config_dir):
        with open(config_dir, 'r') as config_file:
            settings = json.load(config_file)
            return settings