import gzip
import pandas
import os
import re
import multiprocessing
import collections
#import logging
import config


def namer(pixel, extension):
    name = pixel + extension
    return name

def join_dir(pixel):
    filepath = 'C\\Users\\tom.watson\\PycharmProjects\\AWS Sync\\' + pixel
    return filepath


def walk_directory(pixel):
    rootdir = join_dir(pixel)
    filelist = []
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if file.endswith('.gz'):
                processGZ(file)
            elif file.endswith('.csv'):
                filelist.append(os.path.join(rootdir, file))
    return filelist

def removeFile(fileName):  # remove file once we've finished with it
    os.remove(fileName)
    print('Removed file ' + repr(fileName))


def processGZ(filename):  # Expand tar.gz file
    archiveName = filename + '.gz'
    csvName = filename + '.csv'
    inFile = gzip.open(archiveName, 'rb')
    outFile = open(csvName, 'wb')
    outFile.write(inFile.read())
    inFile.close()
    outFile.close()
    removeFile(archiveName)

# "C:\Users\tom.watson\PycharmProjects\AWS Sync\test_dir"

def parseCSV(filelist):
    joinedDataFrame = pandas.concat((pandas.read_csv(filename) for filename in filelist))
    # for i in filelist:
    #     tempData = pandas.read_csv(filelist[i], sep=',', header=None)
    #     frameList.append(tempData)
    # joinedDataFrame = pandas.concat(frameList)
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
    # frameCollector = collections.namedtuple('frameCollector', ['Alpha', 'Beta', 'Gamma', 'Delta'])
    # outputFrames = frameCollector(frameOne, frameTwo, frameThree, frameFour)
    exportDataFrame(name_one, frameOne)
    exportDataFrame(name_two, frameTwo)
    exportDataFrame(name_three, frameThree)
    exportDataFrame(name_four, frameFour)


def exportDataFrame(frame_name, dataframe, pixel):
    frame_path = os.path.join(rootdir, pixel)
    if dataframe.empty == False:
        dataframe.to_csv(frame_path, header = False, index = False)
        print('Dataframe ' + repr(frame_name) + ' written to file')
    else:
        print('Dataframe ' + repr(frame_name) + ' was empty. Not written to file')



def pixelExtraction(pixel):
    filelist = walk_directory(pixel)
    df = parseCSV(filelist)
    for i in filelist:
        removeFile(i)
    df = parseOSName(df)
    partitionDataFrame(df, pixel)
    # TODO Use method of a getName() setName() function to reference pixel name instead of passing it everywhere


if __name__ == '__main__':
    print('Starting parallelisation')
    pool = multiprocessing.Pool(4)
    pool.map(pixelExtraction, config.pixel_list_A)