# -------------------------------------------------------------------------------
# Name:        mbtilesRaster2compactcache
# Purpose:     Build compact cache V2 bundles from single MBTiles Raster dataset file.
#
# Author:      ltbam, luci6974
#
# Created:     18/07/2022
# Modified:    -
#
#  Copyright 2022 swisstopo. 2016 ESRI
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
# Converts .mbtile file to the esri Compact Cache V2 format
#
# Takes two arguments, the first one is the input .mbfile folder
# the second one being the output cache folder (_alllayers)
#
#
# This script is intended to transform big Files, so it will loop each record
# and resolve is it has to be exported based on the maxlvl parameter.
#
# It does not check the input tile format, and assumes that the file
# is a valid sqlite tile databases.
#
# -------------------------------------------------------------------------------
#
# Changeset
# Version 1.0.0 ltbam
import argparse
import sqlite3
import os
import struct
import shutil
import datetime
import re
import io
import time

from joblib import Parallel, delayed
from threading import Lock, get_ident, Thread
import multiprocessing


class Bundle:
    # Bundle linear size in tiles
    BSZ = 128
    # Tiles per bundle
    BSZ2 = BSZ ** 2
    # Index size in bytes
    IDXSZ = BSZ2 * 8

    def __init__(self, file_name):
        self.file_name = file_name
        self.curr_max = 0
        self.curr_offset = 0
        self.curr_index = []
        self.lock = Lock()
        self.fd = None
        self.init_content()

    def init_content(self):
        # print("t {0}: initializing: {1}".format(get_ident(), self.file_name))
        self.fd = open(self.file_name, "wb")
        # Empty bundle file header, lots of magic numbers
        header = struct.pack("<4I3Q6I",
                             3,  # Version
                             Bundle.BSZ2,  # numRecords
                             0,  # maxRecord Size
                             5,  # Offset Size
                             0,  # Slack Space
                             64 + Bundle.IDXSZ,  # File Size
                             40,  # User Header Offset
                             20 + Bundle.IDXSZ,  # User Header Size
                             3,  # Legacy 1
                             16,  # Legacy 2
                             Bundle.BSZ2,  # Legacy 3
                             5,  # Legacy 4
                             Bundle.IDXSZ  # Index Size
                             )
        self.fd.write(header)
        # Write empty index.
        self.fd.write(struct.pack("<{}Q".format(Bundle.BSZ2), *((0,) * Bundle.BSZ2)))
        self.fd.close()
        self.fd = None
        # time.sleep(0.2)

    def open(self):
        # Open the bundle
        # wait a bit if a thread closed a File before opening it again
        self.fd = open(self.file_name, "r+b")
        # Read the current max record size
        self.fd.seek(8)
        self.curr_max = int(struct.unpack("<I", self.fd.read(4))[0])
        # Read the index as longs in a list
        self.fd.seek(64)
        self.curr_index = list(struct.unpack("<{}Q".format(Bundle.BSZ2),
                                             self.fd.read(Bundle.IDXSZ)))
        # Go to end
        self.fd.seek(0, os.SEEK_END)
        self.curr_offset = self.fd.tell()

    def write_tile(self, tile, tile_size, row, col):
        self.fd.write(struct.pack("<I", tile_size))
        self.fd.write(tile)
        self.curr_offset += 4
        # Update the index, row major
        self.curr_index[(row % Bundle.BSZ) * Bundle.BSZ + col % Bundle.BSZ] = self.curr_offset + (tile_size << 40)
        self.curr_offset += tile_size
        # Update the current bundle max tile size
        self.curr_max = max(self.curr_max, tile_size)

    def cleanup(self):
        """
        Updates header and closes the current bundle
        """
        # Update the max rec size and file size, then close the file
        self.fd.seek(8)
        self.fd.write(struct.pack("<I", self.curr_max))
        self.fd.seek(24)
        self.fd.write(struct.pack("<Q", self.curr_offset))
        self.fd.seek(64)
        self.fd.write(struct.pack("<{}Q".format(Bundle.BSZ2), *self.curr_index))
        self.fd.close()
        self.fd = None
        # print("t {0}: cleaned up: {1}".format(get_ident(), self.file_name))
        # time.sleep(0.2)


class BundleManager:
    b_list = {}

    def __init__(self):
        pass

    @staticmethod
    def process(start, results, index, arguments):
        mb_tile_file = arguments.source
        cache_output_folder = arguments.destination
        cache_output_folder = os.path.join(cache_output_folder, "A3_MyCachedService", "Layers", "_alllayers")
        max_level_param = arguments.max_level
        sql = 'SELECT * FROM tiles where rowid > {0} limit {1}'.format(start, Application.rec_per_request)
        if max_level_param != -1:
            sql = 'SELECT * FROM (SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles where rowid > {0} limit ' \
                  '{1}) WHERE zoom_level <= {2}'.format(start, Application.rec_per_request, max_level_param)

        # print(sql)

        database = sqlite3.connect(mb_tile_file)
        row_cursor = database.cursor()
        row_cursor.execute(sql)
        current_tile = 0
        data = {}
        for rec in row_cursor:
            current_tile += 1
            level = 'L' + '{:02d}'.format(rec[0])
            output_path = os.path.join(cache_output_folder, level)
            max_rows = 2 ** int(rec[0]) - 1
            tile = io.BytesIO(rec[3]).getvalue()
            row = max_rows - int(rec[2])
            col = int(rec[1])

            # resolve the bundle
            start_row = int(row / Bundle.BSZ) * Bundle.BSZ
            start_col = int(col / Bundle.BSZ) * Bundle.BSZ
            bname = "R{:04x}C{:04x}".format(start_row, start_col)
            fname = os.path.join(output_path, bname + ".bundle")

            if fname not in data:
                data[fname] = []
            data[fname].append([fname, tile, row, col])

        row_cursor.close()
        database.close()

        BundleManager.add_tiles(data, arguments.lock)

        results[index] = current_tile

    @staticmethod
    def add_tiles(data, lock):
        for bundle in data.keys():

            # init list one bundle/object at a time
            with lock:
                if bundle not in BundleManager.b_list:
                    BundleManager.b_list[bundle] = Bundle(bundle)

            # lock write operation on the bundle per thread
            # print("t {0}: enter lock: {1}".format(get_ident(), bundle))
            with BundleManager.b_list[bundle].lock:
                if not BundleManager.b_list[bundle].fd:
                    BundleManager.b_list[bundle].open()
                for t in data[bundle]:
                    tile = t[1]
                    tile_size = len(t[1])
                    row = t[2]
                    col = t[3]
                    # print("add tile row:{0} col:{1} buff:{2} path:{3}".format(row, col, tile_size, bundle))
                    # Open or create it, seek to end of bundle file
                    BundleManager.b_list[bundle].write_tile(tile, tile_size, row, col)

                BundleManager.b_list[bundle].cleanup()

            # print("t {0}: left lock: {1}".format(get_ident(), bundle))

    @staticmethod
    def add_tile(output_path, byte_buffer, row, col=None):
        """
        Add this tile to the output cache

        :param output_path: path where the bundle is.
        :param byte_buffer: input tile as byte buffer
        :param row: row number
        :param col: column number
        """

        # Read the tile data
        tile = io.BytesIO(byte_buffer).getvalue()
        tile_size = len(tile)

        # resolve the bundle
        start_row = int((row / Bundle.BSZ)) * Bundle.BSZ
        start_col = int((col / Bundle.BSZ)) * Bundle.BSZ
        bname = "R{:04x}C{:04x}".format(start_row, start_col)
        fname = os.path.join(output_path, bname + ".bundle")

        print("add tile row:{0} col:{1} buff:{2} path:{3}".format(row, col, len(byte_buffer), output_path))

        # should be thread safe
        if not fname in BundleManager.b_list:
            BundleManager.b_list[fname] = Bundle(fname)

        # lock write operation on the bundle per thread
        with BundleManager.b_list[fname].lock:
            print("t {0}: enter lock: {1}".format(get_ident(), fname))
            # Open or create it, seek to end of bundle file
            if not BundleManager.b_list[fname].fd:
                BundleManager.b_list[fname].open()

            BundleManager.b_list[fname].write_tile(tile, tile_size, row, col)
            BundleManager.b_list[fname].cleanup()
            print("t {0}: left lock: {1}".format(get_ident(), fname))

        return fname


class Application:
    # Number of concurrent jobs for the export
    p_jobs = multiprocessing.cpu_count()
    # Records per request to be treated by a single thread
    rec_per_request = 10000

    def __init__(self):
        self.bm = BundleManager()

    @staticmethod
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

        # Return the command line arguments.
        arguments = parser.parse_args()

        # validate folder parameters
        if not os.path.exists(arguments.source):
            parser.error("Input folder does not exist or is inaccessible.")

        return arguments


#
# Entry point, start the Application
#
def main():
    app = Application()
    arguments = app.get_arguments()
    arguments.lock = Lock()

    # parse parameters
    mb_tile_file = arguments.source
    cache_output_folder = arguments.destination
    max_level_param = arguments.max_level
    print('Input file: {0}'.format(os.path.basename(mb_tile_file)))

    # prepare output template
    shutil.copytree(os.path.join(os.path.dirname(__file__), "..", "template"), cache_output_folder,
                    symlinks=False, ignore=None, ignore_dangling_symlinks=False)
    cache_output_folder = os.path.join(cache_output_folder, "A3_MyCachedService", "Layers", "_alllayers")

    # creating lvl directories
    for lvl in range(max_level_param + 1):
        level = 'L' + '{:02d}'.format(lvl)
        dir = os.path.join(cache_output_folder, level)
        if not os.path.exists(dir):
            os.makedirs(dir)

    # get max records based on rowid
    database = sqlite3.connect(mb_tile_file)
    row_cursor = database.cursor()
    number_of_tiles = row_cursor.execute('SELECT max(rowid) FROM tiles').fetchone()[0]
    database.close()
    start = 0
    treated_tiles = 0
    start_time = datetime.datetime.now()
    if number_of_tiles > app.rec_per_request:
        app.rec_per_request = int(number_of_tiles ** 0.65)

    print('Exporting {0} rows at a time within {1} threads.\t'.format(app.rec_per_request, app.p_jobs))

    while treated_tiles < number_of_tiles:
        t_arr = {}
        r_arr = {}
        
        # starting threads
        for i in range(app.p_jobs):
            t_arr[i] = Thread(target=BundleManager.process,
                              args=(start + (app.rec_per_request * i), r_arr, i, arguments))
            t_arr[i].start()
            
        # wait for threads before next round
        for i in range(app.p_jobs):
            t_arr[i].join()

        for res in r_arr:
            treated_tiles += r_arr[res]
            start += app.rec_per_request

        if treated_tiles > 0:
            current_tile_time = (datetime.datetime.now() - start_time).total_seconds() / treated_tiles * (
                        number_of_tiles - treated_tiles) / 3600
            print('Treated tiles {:3.2f}% - {:3.2f} hours left.'.format(treated_tiles / number_of_tiles * 100,
                                                                        current_tile_time))
        else:
            print('Treated tiles {:3.2f}'.format(treated_tiles))


if __name__ == '__main__':
    main()
