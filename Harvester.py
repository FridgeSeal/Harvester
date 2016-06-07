import gzip
import pandas
import os
import re
import multiprocessing
#import logging
import config

# #logging.basicConfig(level = logging.info)
# # TODO replace print statements with proper logging
# print('Library import complete')
# print('Connecting to S3...')
# session = boto3.Session(profile_name='am')
# print('Connection to S3 complete')
# print('Acquiring bucket...')
# resource = boto3.resource('s3')
# PixelBucket = resource.Bucket(config.Bucket)
# print('Bucket acquired')
# columns = ['OpID', 'Pixel', 'Country', 'OS']


def namer(pixel, extension):
    name = pixel + extension
    return name

def join_dir(pixel):
    filepath = 'C\\Users\\tom.watson\\PycharmProjects\\AWS Sync\\' + pixel
    return filepath


def walk_directory(pixel):
    rootdir = join_dir(pixel)
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if file.endswith('.gz'):
                processGZ(file)


def downloadTar(key, counter, pixel):  # Download specified object from S3
    downloadName = namer(pixel, counter, '.gz')
    PixelBucket.download_file(key, downloadName)
    print('Downloaded object: ' + repr(key))


def removeFile(fileName):  # remove file once we've finished with it
    os.remove(fileName)
    print('Removed file ' + repr(fileName))


def processGZ(filename):  # Expand tar.gz file
    archiveName = namer(pixel, '.gz')
    csvName = namer(pixel, '.csv')
    inFile = gzip.open(filename, 'rb')
    outFile = open(csvName, 'wb')
    outFile.write(inFile.read())
    inFile.close()
    outFile.close()
    removeFile(archiveName)

# "C:\Users\tom.watson\PycharmProjects\AWS Sync\test_dir"
def parseFile(maximum, pixel):
    nameList = []
    frameList = []
    for i in range(maximum + 1):
        nameList.append(namer(pixel, i, '.csv'))
        tempData = pandas.read_csv(nameList[i], sep=',', header=None)
        frameList.append(tempData)
    joinedDataFrame = pandas.concat(frameList)
    joinedDataFrame = joinedDataFrame.iloc[:, [0, 3, 8, 12]]
    joinedDataFrame.columns = columns
    return joinedDataFrame


def parseOSName(parsingFrame):
    for row in range(len(parsingFrame.index)):
        parsingFrame.iloc[row, 3] = re.sub('-[\d.]*', '', parsingFrame.iloc[row, 3])
        parsingFrame.iloc[row, 3] = re.sub('iPhone OS', 'iOS', parsingFrame.iloc[row, 3])
    print('Dataframe parsed')
    return parsingFrame

def partitionDataFrame(dataframe, pixel):  # take the dataframe and split it up by OS and Country
    name_one = pixel + 'AU' + 'Android' + '.csv'
    name_two = pixel + 'AU' + 'iOS' + '.csv'
    name_three = pixel + 'NZ' + 'Android' + '.csv'
    name_four = pixel + 'NZ' + 'iOS' + '.csv'
    frameOne = dataframe.loc[(dataframe['OS'] == 'Android') & (dataframe['Country'] == 'AU')]
    frameTwo = dataframe.loc[(dataframe['OS'] == 'iOS') & (dataframe['Country'] == 'AU')]
    frameThree = dataframe.loc[(dataframe['OS'] == 'Android') & (dataframe['Country'] == 'NZ')]
    frameFour = dataframe.loc[(dataframe['OS'] == 'iOS') & (dataframe['Country'] == 'NZ')]
    # TODO Put an if statement here to not write out empty data frames
    frameOne.to_csv(name_one, header=False, index=False)
    print('frame ' + repr(name_one) + 'written to file')
    frameTwo.to_csv(name_two, header=False, index=False)
    print('frame ' + repr(name_two) + 'written to file')
    frameThree.to_csv(name_three, header=False, index=False)
    print('frame ' + repr(name_three) + 'written to file')
    frameFour.to_csv(name_four, header=False, index=False)
    print('frame ' + repr(name_four) + 'written to file')


def pixelExtraction(pixel):
    maximum = getObjects(pixel)
    print('Got objects for pixel: ' + repr(pixel))
    processGZ(maximum, pixel)
    print('Processed objects for pixel: ' + repr(pixel))
    df = parseFile(maximum, pixel)
    for i in range(maximum + 1):
        csvToRemove = namer(pixel, i, '.csv')
        removeFile(csvToRemove)
    df = parseOSName(df)
    partitionDataFrame(df, pixel)
    # TODO Use method of a getName() setName() function to reference pixel name instead of passing it everywhere


if __name__ == '__main__':
    print('Starting parallelisation')
    pool = multiprocessing.Pool(4)
    pool.map(pixelExtraction, config.pixel_list_A)
