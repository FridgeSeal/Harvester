import gzip
import pandas
import os
import re
# import multiprocessing
import config


def namer(pixel, extension):
    name = pixel + extension
    return name


def join_dir(pixel):
    filepath = os.path.join(config.root_dir, pixel)
    return filepath


def walk_directory(pixel):
    rootdir = join_dir(pixel)
    filelist = []
    for dir, subdirs, files in os.walk(rootdir):
        for file in files:
            if file.endswith('.gz'):
                processGZ(file, dir)
            elif file.endswith('.csv'):
                filelist.append(os.path.join(dir, file))
    return filelist


def removeFile(fileName):  # remove file once we've finished with it
    os.remove(fileName)
    print('Removed file ' + repr(fileName))


def processGZ(filename, dir):  # Expand tar.gz file
    csvName = filename[:-3] + '.csv'
    inFile = gzip.open(os.path.join(dir, filename), 'rb')
    outFile = open(os.path.join(dir, csvName), 'wb')
    outFile.write(inFile.read())
    inFile.close()
    outFile.close()
    # removeFile(filename)


def parseCSV(filelist):
    columns = ['OpID', 'Pixel', 'Country', 'OS']
    joinedDataFrame = pandas.concat((pandas.read_csv(filename, sep = ',', header=None) for filename in filelist))
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
    exportDataFrame(name_one, frameOne, pixel)
    exportDataFrame(name_two, frameTwo, pixel)
    exportDataFrame(name_three, frameThree, pixel)
    exportDataFrame(name_four, frameFour, pixel)


def exportDataFrame(frame_name, dataframe, pixel):
    frame_path = join_dir(pixel)
    frame_path = os.path.join(frame_path, frame_name)
    if not dataframe.empty:
        dataframe[['OS']].to_csv(frame_path, header=False, index=False)
        print('Dataframe ' + repr(frame_name) + ' written to file')
    else:
        print('Dataframe ' + repr(frame_name) + ' was empty. Not written to file')


def pixelExtraction(pixel):
    # noinspection PyUnusedLocal
    dummylist = walk_directory(pixel)
    filelist = walk_directory(pixel)  # This is really poor programming pls don't judge me, it'll be changed v soon
    # TODO re-write that part to be less terrible
    df = parseCSV(filelist)
    for i in filelist:
        removeFile(i)
    df = parseOSName(df)
    partitionDataFrame(df, pixel)
    # TODO Use method of a getName() setName() function to reference pixel name instead of passing it everywhere


# if __name__ == '__main__':
#     print('Starting parallelisation')
#     pool = multiprocessing.Pool(4)
#     pool.map(pixelExtraction, config.test_list)

pixelExtraction(config.test_list)
