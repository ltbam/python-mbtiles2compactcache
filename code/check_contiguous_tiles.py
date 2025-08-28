# -------------------------------------------------------------------------------
# Name:        check_contiguous_tiles
# Purpose:     Check if there is no hole (missing tiles) in a bundle.
#
# Author:      ltbam
#
# Created:     18/07/2022
# Modified:    -
#
#  Copyright 2023 swisstopo.
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
# Changeset
# Version 1.0.0 ltbam
from mbtilesRaster2compactcache import Bundle
import os
import math


cache_output_folder = r"\\v0t0081a.adr.admin.ch\p-dat\d4p\d_src\MGDI\A3\ChSwisstopoSwissimageProduct\20230524094759\cache"

def main():
    print("Checking contiguous tiles in Bundle")
    for path, subdirs, files in os.walk(cache_output_folder):
        for name in files:
            if name.endswith(".bundle"):
                bdl = Bundle(os.path.join(path, name))
                bdl.open()
                results = listMissingTiles(bdl)
                if len(results) == 0:
                    print("bundle {} is ok".format(name))
                else:
                    for res in results:
                        print("Missing contiguous tile: level {}, row {}, col {}".format(res["lvl"], res["row"],
                                                                                         res["col"]))
                # close bundle without writing anything
                bdl.fd.close()
                bdl.fd = None

def listMissingTiles(bundle):
    files = []
    # Loop each Tile index and resolve if it has data
    # range(0, 128) means 0-127
    for row in range(0, 128):
        startTile = 0
        numTiles = 0
        # count tiles with data, determine drawing center
        for col in range(0, 128):
            t_idx = bundle.curr_index[128 * row + col]
            t_size = int(math.floor(t_idx / Bundle.M))
            if t_size > 0:
                numTiles += 1
                if startTile == 0:
                    startTile = col

        if numTiles > 3:
            data_started = False
            mid_range = startTile + numTiles // 2
            # print("lvl {} row {} mid_range: {}".format(bundle.level, row, mid_range))
            # inspect from left
            # range(0, 128) means 0-127
            for col in range(0, mid_range):
                t_idx = bundle.curr_index[128 * row + col]
                t_size = int(math.floor(t_idx / Bundle.M))
                if data_started:
                    if t_size == 0:
                        absrow = bundle.row_offset + row
                        abscol = bundle.col_offset + col
                        files.append(dict(col=abscol, row=absrow, lvl=int(bundle.level)))
                else:
                    if t_size != 0:
                        data_started = True
            data_started = False
            # inspect from right
            for col in range(127, mid_range - 1, -1):
                t_idx = bundle.curr_index[128 * row + col]
                t_size = int(math.floor(t_idx / Bundle.M))
                if data_started:
                    if t_size == 0:
                        absrow = bundle.row_offset + row
                        abscol = bundle.col_offset + col
                        files.append(dict(col=abscol, row=absrow, lvl=int(bundle.level)))
                else:
                    if t_size != 0:
                        data_started = True

    return files

if __name__ == '__main__':
    main()
