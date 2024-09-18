# ABS GeoPackages

ABS GeoPackages are suitable for data users who have their own Geospatial Information System (GIS). GeoPackages do not include software. This geopackage has GDA2020 digital boundary files.

GeoPackages contain Estimated Resident Population (ERP) data linked together with the ASGS boundaries in an SQLite container that can be easily used. They are formatted in a standards based format gpkg which is in an open format, that is supported by commonly used software systems. 

ERP GeoPackages contain the following data:
* Geography levels - Australian Statistical Geography Standard (ASGS) Statistical Areas Level 2 (SA2) or Local Government Areas (LGA) 
* ERP for each year from 2001 to current (number)
* ERP change from previous year to current year (number)
* ERP change from previous year to current year (per cent)
* Area in square kilometres
* Population density for current year (persons per square kilometre) 
* Components of ERP change - births, deaths, natural increase, internal arrivals, internal departures, net internal migration, overseas arrivals, overseas departures, net overseas migration, for 2021-22 and 2022-23 (number)

ABS ERP GeoPackages include metadata and reference documents to enable you to use and read the data. The compressed file contains:
* GeoPackage file - Estimated Resident Population data merged with boundary information, for SA2s or LGAs (.gpkg)
* AboutGeopackage_readme.txt - "Read Me" documentation containing helpful information for users about the GeoPackages and contents (.txt). These files can be viewed in any text editor or web browser
* Xml file containing geographic specific metadata about the boundaries, for SA2s or LGAs (.xml)
* Creative Commons Licensing information (.txt)

The population estimates in this product are final for 2001 to 2021, revised for 2022, and preliminary for 2023.


# More information about GeoPackages

The GeoPackage format is a container format specifically designed for holding, viewing and transporting multiple spatial layers and data as a single "file", and may be more suitable for those using a modern GIS system for analysis rather than non-spatial analysis tool such as SAS.

[Open Geospatial Consortium (OGC)](http://www.opengeospatial.org/) GeoPackage is an open, non-proprietary, platform-independent and standards-based data format for geographic information systems implemented as a SQLite database container. Originally designed to meet mobile geospatial needs, it is a way of exchanging geospatial datasets and the associated base maps and layers as a single ready-to-use file.

GeoPackage is based on a technology called [SQLite](https://sqlite.org/), which is a lightweight, public domain database system. If you know SQLite, then you are most of the way to understanding GeoPackage. Most modern geographical information systems and tools (for example MapInfo, ArcMap, QGIS, FME and others) are starting to support GeoPackage natively, making it a widely adopted geospatial data interchange format.

Further information on the specification for GeoPackage can be found on the following website http://www.GeoPackage.org/ , including implementations and examples.

Some advantages of GeoPackage are:
* It is backed by the OGC (http://www.opengeospatial.org/) - the international standards organisation for geospatial information and systems
* It can hold and transport multiple spatial layers as a self-contained, single file container
* It is based on an open and widely supported database format, allowing SQL querying of the data
* Data can be added and removed from a GeoPackage
* The file format is vendor agnostic. QGIS, FME and others are starting to support GeoPackage natively, making it a powerful and widely adopted geospatial data interchange format
