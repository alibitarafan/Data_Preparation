import csv
import logging
import os.path

logger = logging.getLogger('tripData Logger')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('tripData.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

required = {"trip duration":0, "start time":1, "start station id":3,
            "end station id":7,  "bikeid":11, "user type":12,
            "birth year":13, "gender":14}

dest_path = 'DESIREDPATH'

def genDataFiles():
    import glob
    for data_file in glob.glob("*citibike*.csv"):
        yield data_file

# read csv
def genTripData(f_data):
    try:
        with open(f_data,buffering=20000000) as f:
            next(f,None)
            for line in f:
                data_row = list(line.split(',')[i] for i in required.values())
                yield (';'.join(data_row))

    except:
        logger.exception('reading trip data faced an issue')

for f in genDataFiles():
    with open((os.path.join(dest_path,f)), 'a') as output:
        logger.info('processing ', output)
        for data in genTripData(f):
            output.write(data)
