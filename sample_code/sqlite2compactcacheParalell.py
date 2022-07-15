# -------------------------------------------------------------------------------
# Name:        mbtiles2compactcache
# Purpose:     Build compact cache V2 bundles from MBTiles in SQLLite databases
#
# Author:      luci6974
#
# Created:     20/09/2016
# Modified:    04/05/2018,esristeinicke
#              23/10/2019,mimo
#
#  Copyright 2016 Esri
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.?
#
# -------------------------------------------------------------------------------
#
# Converts .mbtile files to the esri Compact Cache V2 format
#
# Takes two arguments, the first one is the input .mbfile folder
# the second one being the output cache folder (_alllayers)
#
#
# Assumes that the input .mbtile files are named after the level.. (17.mbtile)
#
# Loops over columns and then row, in the order given by os.walk
# Keeps one bundle open in case the next tile fits in the same bundle
# In most cases this combination results in good performance
#
# It does not check the input tile format, and assumes that all
# the files are valid sqlite tile databases.  In other
# words, make sure there are no spurious files and folders under the input
# path, otherwise the output bundles might have strange content.
#
# -------------------------------------------------------------------------------
#
# v1.2 added grayscale Option (requires pillow)
#    * to install pillow:
#       * make sure python & scripts/pip is in path
#       * in cmd type pip install pillow
#
# v1.3 fixed grayscale (Grayscale + Alpha = fixed grayscale Image) & (96 DPI)
#
# v1.4 better logging / fixing for python 3 / fixed ETA
#
# v1.5 better parameter support & parameter help
import argparse
import sqlite3
import os
import struct
import shutil
import datetime
import re
import io
from joblib import Parallel, delayed
from threading import Lock

try:
    from PIL import Image
    is_pillow = True
except ImportError as import_error:
    is_pillow = False

# Bundle linear size in tiles
BSZ = 128
# Tiles per bundle
BSZ2 = BSZ ** 2
# Index size in bytes
IDXSZ = BSZ2 * 8

# Output path
output_path = None
lock = Lock()

# The curr_* variable are used for caching of open output bundles
# current bundle is kept open to reduce overhead
# TODO: Eliminate global variables
curr_bundle = None
# A bundle index list
# Array would be better, but it lacks 8 byte int support
curr_index = None
# Bundle file name without path or extension
curr_bname = None
# Current size of bundle file
# curr_offset = long(0)
curr_offset = int(0)
# max size of a tile in the current bundle
curr_max = 0
# how much records to read per request
rec_per_request = 100


def get_arguments():
    """
    Parses commandline arguments.

    :return: commandline arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('-s', '--source',
                        help='Input folder containing the mbtile files.', required=True)
    parser.add_argument('-d', '--destination',
                        help='Output for level folders.', required=True)
    parser.add_argument('-ml', '--max_level',
                        help='Do until this Level.', default=-1, type=int,
                        required=True)
    parser.add_argument('-g', '--grayscale',
                        help='Convert tiles to grayscale while processing.', default=False, action="store_true",
                        required=False)

    # Return the command line arguments.
    arguments = parser.parse_args()

    # validate folder parameters
    if not is_pillow and arguments.grayscale:
        parser.error("Grayscale option requires Pillow (PIL) module to be installed.")
    if not os.path.exists(arguments.source):
        parser.error("Input folder does not exist or is inaccessible.")

    return arguments


#class Bundle:
#    def __init__(self):
#        pass


def init_bundle(file_name):
    """Create an empty V2 bundle file
    :param file_name: bundle file name
    """
    fd = open(file_name, "wb")
    # Empty bundle file header, lots of magic numbers
    header = struct.pack("<4I3Q6I",
                         3,  # Version
                         BSZ2,  # numRecords
                         0,  # maxRecord Size
                         5,  # Offset Size
                         0,  # Slack Space
                         64 + IDXSZ,  # File Size
                         40,  # User Header Offset
                         20 + IDXSZ,  # User Header Size
                         3,  # Legacy 1
                         16,  # Legacy 2
                         BSZ2,  # Legacy 3
                         5,  # Legacy 4
                         IDXSZ  # Index Size
                         )
    fd.write(header)
    # Write empty index.
    fd.write(struct.pack("<{}Q".format(BSZ2), *((0,) * BSZ2)))
    fd.close()


def cleanup():
    """
    Updates header and closes the current bundle
    """
    global curr_bundle, curr_bname, curr_index, curr_max, curr_offset
    curr_bname = None

    # Update the max rec size and file size, then close the file
    if curr_bundle is not None:
        curr_bundle.seek(8)
        curr_bundle.write(struct.pack("<I", curr_max))
        curr_bundle.seek(24)
        curr_bundle.write(struct.pack("<Q", curr_offset))
        curr_bundle.seek(64)
        curr_bundle.write(struct.pack("<{}Q".format(BSZ2), *curr_index))
        curr_bundle.close()

        curr_bundle = None


def open_bundle(row, col):
    """
    Make the bundle corresponding to the row and col current
    """
    global curr_bname, curr_bundle, curr_index, curr_offset, output_path, curr_max
    # row and column of top-left tile in the output bundle
    # start_row = (row / BSZ) * BSZ
    start_row = int((row / BSZ)) * BSZ
    # start_col = (col / BSZ) * BSZ
    start_col = int((col / BSZ)) * BSZ
    bname = "R{:04x}C{:04x}".format(start_row, start_col)
    # bname = "R%(r)04xC%(c)04x" % {"r": start_row, "c": start_col}

    # If the name matches the current bundle, nothing to do
    if curr_bname is not None:
        b = os.path.join(output_path, bname + ".bundle")
        if b == curr_bundle.name:
            return
        #else:
        #    print("Opening {0}".format(os.path.join(output_path, bname + ".bundle")))

    # Close the current bundle, if it exists
    cleanup()

    # Make the new bundle current
    curr_bname = bname
    # Open or create it, seek to end of bundle file
    fname = os.path.join(output_path, bname + ".bundle")

    # Create the bundle file if it didn't exist already
    if not os.path.exists(fname):
        init_bundle(fname)

    # Open the bundle
    curr_bundle = open(fname, "r+b")
    # Read the current max record size
    curr_bundle.seek(8)
    curr_max = int(struct.unpack("<I", curr_bundle.read(4))[0])
    # Read the index as longs in a list
    curr_bundle.seek(64)
    curr_index = list(struct.unpack("<{}Q".format(BSZ2),
                                    curr_bundle.read(IDXSZ)))
    # Go to end
    curr_bundle.seek(0, os.SEEK_END)
    curr_offset = curr_bundle.tell()


def add_tile(byte_buffer, row, col=None):
    """
    Add this tile to the output cache

    :param byte_buffer: input tile as byte buffer
    :param row: row number
    :param col: column number
    """
    global BSZ, curr_bundle, curr_max, curr_offset

    # Read the tile data
    tile = io.BytesIO(byte_buffer).getvalue()
    tile_size = len(tile)

    # Write the tile at the end of the bundle, prefixed by size
    open_bundle(row, col)
    curr_bundle.write(struct.pack("<I", tile_size))
    curr_bundle.write(tile)
    # Skip the size
    curr_offset += 4

    # Update the index, row major
    curr_index[(row % BSZ) * BSZ + col % BSZ] = curr_offset + (tile_size << 40)
    curr_offset += tile_size

    # Update the current bundle max tile size
    curr_max = max(curr_max, tile_size)


def add_tile_gray(byte_buffer, row, col=None):
    """
    Convert tile to grayscale before adding it to toe bundle.

    :param byte_buffer: input tile as byte buffer
    :param row: row number
    :param col: column number
    """
    global BSZ, curr_bundle, curr_max, curr_offset

    # read & convert to grayscale
    image = Image.open(io.BytesIO(byte_buffer))
    image_gray = image.convert('LA')
    byte_buffer_gray = io.BytesIO()
    # image_gray.save(byte_buffer_gray, format="PNG", dpi=(96, 96))
    image_gray.save(byte_buffer_gray, 'PNG', dpi=(96, 96))

    # Read the tile data
    tile = byte_buffer_gray.getvalue()
    tile_size = len(tile)

    # Write the tile at the end of the bundle, prefixed by size
    open_bundle(row, col)
    curr_bundle.write(struct.pack("<I", tile_size))
    curr_bundle.write(tile)
    # Skip the size
    curr_offset += 4

    # Update the index, row major
    curr_index[(row % BSZ) * BSZ + col % BSZ] = curr_offset + (tile_size << 40)
    curr_offset += tile_size

    # Update the current bundle max tile size
    curr_max = max(curr_max, tile_size)


def process(start, arguments):
    global output_path
    global lock

    mb_tile_file = arguments.source
    cache_output_folder = arguments.destination
    cache_output_folder = os.path.join(cache_output_folder, "A3_MyCachedService", "Layers", "_alllayers")
    max_level_param = arguments.max_level
    do_grayscale = arguments.grayscale

    sql = 'SELECT * FROM tiles where rowid > {0} limit {1}'.format(start, rec_per_request)
    if max_level_param != -1:
        sql = 'SELECT * FROM (SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles where rowid > {0} limit ' \
              '{1}) WHERE zoom_level <= {2}'.format(start, rec_per_request, max_level_param)

    print(sql)
    database = sqlite3.connect(mb_tile_file)
    row_cursor = database.cursor()
    row_cursor.execute(sql)

    current_tile = 0
    has_data = False
    for row in row_cursor:
        has_data = True
        current_tile += 1
        level = 'L' + '{:02d}'.format(row[0])
        # print('Current level: {0}'.format(level))

        with lock:
            output_path = os.path.join(cache_output_folder, level)
            max_rows = 2 ** int(row[0]) - 1
            if do_grayscale:
                add_tile_gray(row[3], max_rows - int(row[2]), int(row[1]))
            else:
                add_tile(row[3], max_rows - int(row[2]), int(row[1]))

    row_cursor.close()
    database.close()
    return rec_per_request


def main(arguments):
    global output_path

    # parse parameter
    mb_tile_file = arguments.source
    cache_output_folder = arguments.destination
    max_level_param = arguments.max_level
    do_grayscale = arguments.grayscale

    print('Working on file: {0}'.format(os.path.basename(mb_tile_file)))
    database = sqlite3.connect(mb_tile_file)

    #prepare output template
    shutil.copytree(os.path.join(os.path.dirname(__file__), "..", "sample_template"), cache_output_folder, symlinks=False, ignore=None,  ignore_dangling_symlinks=False)
    cache_output_folder = os.path.join(cache_output_folder, "A3_MyCachedService", "Layers", "_alllayers")

    #creating lvl directories
    for lvl in range(max_level_param + 1):
        level = 'L' + '{:02d}'.format(lvl)
        dir = os.path.join(cache_output_folder, level)
        if not os.path.exists(dir):
            os.makedirs(dir)

    # sequetially loop the data
    print('Exporting {0} rows at a time \t'.format(rec_per_request))
    print(' \t')

    row_cursor = database.cursor()
    number_of_tiles = row_cursor.execute('SELECT max(rowid) FROM tiles').fetchone()[0]

    start = 0
    treated_tiles = 0
    start_time = datetime.datetime.now()

    p_jobs = 10

    from multiprocessing import Pool
    pool = Pool(p_jobs)
    while treated_tiles < number_of_tiles:
        results = pool.starmap(process, [(start+(rec_per_request*i), arguments) for i in range(p_jobs)])
        #pool.join()
        for res in results:
            treated_tiles += res
            start += rec_per_request

    pool.close()
    # cleanup open bundles
    cleanup()

    # close the database when finished
    database.close()

    exit()


    #
    # Old code section
    #

    while treated_tiles < number_of_tiles:
        sql = 'SELECT * FROM tiles where rowid > {0} limit {1}'.format(start, rec_per_request)
        if level_param != -1:
            sql = 'SELECT * FROM (SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles where rowid > {0} limit {1}) WHERE zoom_level = {2}'.format(start, rec_per_request, level_param)
        if max_level_param != -1:
            sql = 'SELECT * FROM (SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles where rowid > {0} limit {1}) WHERE zoom_level <= {2}'.format(start, rec_per_request, max_level_param)
        row_cursor.execute(sql)
        #print('Treating rows: {0} to {1}'.format(start + 1, start + rec_per_request))
        start += rec_per_request
        current_tile = 0
        has_data = False
        for row in row_cursor:
            has_data = True
            current_tile += 1
            level = 'L' + '{:02d}'.format(row[0])
            #print('Current level: {0}'.format(level))

            output_path = os.path.join(cache_output_folder, level)
            # create level folder if not exists
            if not row[0] in lvl_dict:
                lvl_dict[row[0]] = output_path
                if not os.path.exists(output_path):
                    os.makedirs(output_path)

            max_rows = 2 ** int(row[0]) - 1
            if do_grayscale:
                add_tile_gray(row[3], max_rows - int(row[2]), int(row[1]))
            else:
                add_tile(row[3], max_rows - int(row[2]), int(row[1]))

        treated_tiles += rec_per_request
        current_tile_time = (datetime.datetime.now() - start_time).total_seconds() / treated_tiles * (
                        number_of_tiles - treated_tiles) / 3600  # hours to reach 100% Tiles
        print('Treated tiles {:3.2f}% - {:3.2f} hours left.'.format(treated_tiles/number_of_tiles*100, current_tile_time))

        #if not has_data:
        #    break

    # cleanup open bundles
    cleanup()

    # close the database when finished
    database.close()

if __name__ == '__main__':
    main(get_arguments())
