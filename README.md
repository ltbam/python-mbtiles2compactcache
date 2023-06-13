# python-mbtiles2compactcache
(credits: https://github.com/Esri/raster-tiles-compactcache)

## Compact Cache V2

### mbtilesRaster2compactcache.py

Convert a single .mbtile raster dataset file to the [Esri Compact Cache V2](./CompactCacheV2.md) format bundles. It only builds a completely functional cache. This script is designed to export large to huge raster dataset mbtiles. the export occurs using multiple threads reading all records sequentially.

Requirement:
- data must be in Web Mercator (EPSG:3857) 
- the tiles table must be a rowid table.

The script does not check the input tile format, and assumes that all the files under the source contain valid SQLLite databases with tiles in MBTiles format. 
The algorithm loops over the records, inserting each tile in the appropriate bundle. Each thread writes its records in a bundle and then close it.

The [file](./file) folder contains example [MBTiles]
The [cache] (./cache) folder contains a Compact Cache V2 cache produced as result of the mbtilesRaster2compactcache.py script. The commands used to generate the cache is:

```console
python .\code\mbtilesRaster2compactcache.py -ml 15 -s .\file\countries-raster.mbtiles -d .\cache
```

## Documentation and sample code for Esri Compact Cache V2 format

The Compact Cache V2 is the current file format used by ArcGIS to store raster tiles.  The Compact Cache V2 stores multiple tiles in a single file called a bundle.  The bundle file structure is very simple and optimized for quick access, the result being improved performance over other alternative formats.

| | Col 0 | Col 1 |
|---|---|---|
| Row 0 | Row 0 Col 0 | Row 0 Col 1  |
| Row 1 | Row 1 Col 0 | Row 1 Col 1 |

## Content
This repository contains [documentation](CompactCacheV2.md), a [cache](cache) and a Python 3.x [code example](code) of how to build Compact Cache V2 bundles from MBTiles.

## Licensing

Copyright 2018 Esri Germany GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.

