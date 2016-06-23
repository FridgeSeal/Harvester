import gzip
import pandas
import os
import re
import logging
# import multiprocessing
import config

logging.basicConfig(filename = 'local.log', filemode = 'w', level = logging.INFO, format = '%(asctime)s %(message)s')
logger = logging.getLogger(__name__)


def namer(pixel, extension):
    name = pixel + extension
    logger.info('File named: %s', name)
    return name


def join_dir(pixel):
    filepath = os.path.join(config.root_dir, pixel)
    logger.info('Filepath constructed: %s', filepath)
    return filepath


def walk_directory(pixel):
    rootdir = join_dir(pixel)
    filelist = []
    for dir, subdirs, files in os.walk(rootdir):
        for file in files:
            if file.endswith('.gz'):
                logger.debug('About to unpack: %s', file)
                processGZ(file, dir)
            elif file.endswith('.csv'):
                logger.debug('About to append .csv: %s', file)
                filelist.append(os.path.join(dir, file))
    return filelist


def removeFile(fileName):  # remove file once we've finished with it
    os.remove(fileName)
    logger.debug('Removed file: %s', fileName)


def processGZ(filename, dir):  # Expand tar.gz file
    csvName = filename[:-3] + '.csv'
    inFile = gzip.open(os.path.join(dir, filename), 'rb')
    outFile = open(os.path.join(dir, csvName), 'wb')
    outFile.write(inFile.read())
    inFile.close()
    outFile.close()
    logger.info('.gz unpacked: %s', filename)
    # removeFile(filename)


def parseCSV(filelist):
    columns = ['OpID', 'Pixel', 'Country', 'OS']
    joinedDataFrame = pandas.concat((pandas.read_csv(filename, sep = ',', header=None, encoding = 'ISO-8859-1') for filename in filelist))
    logger.info('Dataframe joined')
    joinedDataFrame = joinedDataFrame.iloc[:, [0, 3, 8, 12]]
    logger.info('Columns removed')
    joinedDataFrame.columns = columns
    return joinedDataFrame


def parseOSName(parsingFrame):
    for row in range(len(parsingFrame.index)):
        parsingFrame.iloc[row, 3] = re.sub('-[\d.]*', '', parsingFrame.iloc[row, 3])
        parsingFrame.iloc[row, 3] = re.sub('iPhone OS', 'iOS', parsingFrame.iloc[row, 3])
    print('Dataframe parsed')
    return parsingFrame


def partitionDataFrame(dataframe, pixel, flag):  # take the dataframe and split it up by OS and Country
    if flag == 'granular':
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
    elif flag == 'aggregated':
        name_one = pixel + 'AU' + '.csv'
        name_two = pixel + 'NZ' + '.csv'
        frameOne = dataframe.loc[((dataframe['OS'] == 'Android') | (dataframe['OS'] == 'iOS')) & (dataframe['Country'] == 'AU')]
        frameTwo = dataframe.loc[((dataframe['OS'] == 'Android') | (dataframe['OS'] == 'iOS')) & (dataframe['Country'] == 'NZ')]
        exportDataFrame(name_one, frameOne, pixel)
        exportDataFrame(name_two, frameTwo, pixel)
    else:
        print('Error: Flag not recognised')


def exportDataFrame(frame_name, dataframe, pixel):
    # frame_path = join_dir(pixel)
    # frame_path = os.path.join(frame_path, frame_name)
    export_path = ''
    if not os.path.exists(os.path.join(os.getcwd(), 'data output')):
        export_path = os.path.join(os.getcwd(), 'data output')
        os.makedirs(export_path)
        logger.info('data output directory created')
    frame_path = os.path.join(export_path, frame_name)
    if not dataframe.empty:
        dataframe[['OpID']].to_csv(frame_path, header=False, index=False)
        print('Dataframe ' + repr(frame_name) + ' written to file')
        logger.info('Dataframe %s written to file', frame_name)
    else:
        print('Dataframe ' + repr(frame_name) + ' was empty. Not written to file')
        logger.info('Dataframe %s was empty, not written to file', frame_name)


def pixelExtraction(pixel):
    # noinspection PyUnusedLocal
    dummylist = walk_directory(pixel)
    print('.gz files unpacked')
    filelist = walk_directory(pixel)  # This is really poor programming pls don't judge me, it'll be changed v soon
    print('.csv files captured')
    # TODO re-write that part to be less terrible
    df = parseCSV(filelist)
    print('dfs merged')
    for i in filelist:
        removeFile(i)
    print('.csv files removed')
    df = parseOSName(df)
    partitionDataFrame(df, pixel, 'aggregated')
    # TODO Use method of a getName() setName() function to reference pixel name instead of passing it everywhere


# if __name__ == '__main__':
#     print('Starting parallelisation')
#     pool = multiprocessing.Pool(4)
#     pool.map(pixelExtraction, config.test_list)

if __name__ == '__main__':
    for pixel in config.pixel_list:
        pixelExtraction(pixel)
    print('Process Complete')
