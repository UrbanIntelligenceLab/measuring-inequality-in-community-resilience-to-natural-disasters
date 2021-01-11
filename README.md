# Summary
This repository documents the data processing and the analysis performed on the data to support the findings presented in the article **"Measuring Inequality in Community Resilience to Natural Disasters Using Large-scale Mobility Data"** published in *Nature Communications*.

## Title: Measuring Inequality in Community Resilience to Natural Disasters Using Large-scale Mobility Data

**Authors**: [Boyeong Hong](https://marroninstitute.nyu.edu/people/boyeong-hong)<sup>1</sup>, [Bartosz J. Bonczak](https://marroninstitute.nyu.edu/people/bartosz-bonczak)<sup>1</sup>, [Arpit Gupta](https://www.stern.nyu.edu/faculty/bio/arpit-gupta)<sup>2</sup>, and [Constantine E. Kontokosta](https://marroninstitute.nyu.edu/people/constantine-kontokosta)<sup>1,3,*</sup>

<sup>1</sup>[New York University, Marron Institute of Urban Management, 60 5th Avenue, New York, NY 10011](https://marroninstitute.nyu.edu/)

<sup>2</sup>[New York University, Stern School of Business, 44 West 4th Street, New York, NY 10012](https://www.stern.nyu.edu/)

<sup>3</sup>[New York University, Center for Urban Science and Progress, 370 Jay Street, Brooklyn, NY 11201](https://cusp.nyu.edu/)

<sup>*</sup>Correspondence and requests for materials should be addressed to Constantine E. Kontokosta (email: <ckontokosta@nyu.edu>).

![Figure 1](https://github.com/UrbanIntelligenceLab/measuring-inequality-in-community-resilience-to-natural-disasters/blob/main/figures/Fig1.png?raw=true)

## Abstract
While conceptual definitions provide a foundation for the study of disasters and their impacts,  thechallenge for researchers and practitioners alike has been to develop objective and rigorous measuresof resilience that are generalizable and scalable, taking into account spatio-temporal dynamics in theresponse and recovery of localized communities. In this paper, we analyze mobility patterns of morethan 800,000 anonymized mobile devices in Houston, Texas, representing approximately 35% of thelocal population, in response to Hurricane Harvey in 2017. Using changes in mobility behavior before,during, and after the disaster, we empirically define community resilience capacity as a function of themagnitude of impact and time-to-recovery. Overall, we find clear socioeconomic and racial disparities inresilience capacity and evacuation patterns. Our work provides new insight into the behavioral responseto disasters and provides the basis for data-driven public sector decisions that prioritize the equitableallocation of resources to vulnerable neighborhoods.

## Cite
Hong, B., Bonczak, B., Gupta, A., & Kontokosta, C. E. (in press). Measuring Inequality in Community Resilience to Natural Disasters Using Large-scale Mobility Data. *Nature Communications*

# Repository Structure
The repository documents the data processing and analysis perform on the data to support the findings of the study reported in the published article. It is structured as follows:

```
.\root
    |--- data                   # placeholder directory intended to contain raw data
    |--- figures                # figures generated for the publication purposes
    |--- scripts                # contains all scripts used in the study 
         |--- data_processing   # scripts processing raw data with PySpark on HDFS cluster
         |--- analysis          # scripts documenting analytical tasks
```

# Data Description

## Mobility Data
The primary data used for the main analysis are the **annonymized geolocated mobile application activity** provided by [VenPath, Inc.](https://www.venpath.net/) – a data marketplace company providing mobile application data and business analytics services extracted from more than 200 various mobile applications and covering more than 120 million devices every month across the U.S. The initial dataset of 23TB of compressed comma-separated-value files (CSV) covering the period from June 2016 through October 2017 and contains more than 320 billion data points.

### Data Availability Statement
The annonymized geolocated mobile application activity data that support the findings of this study are available from VenPath, Inc. but restrictions apply to the availability of these data, which were used under license for the current study, and so are not publicly available. Data are however available from the authors upon
reasonable request and with permission of VenPath, Inc.

### Data structure
The mobility data was organized by the date of registering it in the database, not by the date of ping creation and is partitioned by year, month and day. It is of the following structure:

Column | Type | Description
-------|------|-------------
venpath_id | int | Auto-incrementing key to denote that this is a unique locate
app_id | str | The source app that provided this data
ad_id | str | The annonymized advertising ID
id_type | str | iOS (idfa) or Android (afid)
country_code | str | The locale setting on the user's phone
device_make | str | The make of the user's device
device_model | str | The model of the user's device
device_os | str | The operating system of the user's device
device_os_version | str | The version of the operating system on the user's device
latitude | float | The geographical latitude
longitude | float | The geographical longitude
timestamp | timestamp | The UTC timestamp / datestamp of when this location point was collected
ip_address | str | The IP address of the user at the time of the data collection
horizontal_accuracy | float | The horizontal accuracy of the location point, in meters
vertical_accuracy | str | The vertical accuracy of the location point, in meters
foreground | bool | true if the ping was collected in the foreground or false if it was collected in the background

## Anciliary Data
The study relies on publicly available data sources as well, which are listed in the table below.

Data | Size | Description | Spatial granularity | Data coverage | Source | URL
-----|------|-------------|---------------------|---------------|--------|----
Parcel level land use | 1.3 GB | Houston Harris County parcel level land use information such as property owner, land use type,and location attributes etc. | Parcel | 2018 | Texas Natural Resources Information System | https://data.tnris.org/collection/2679b514-bb7b-409f-97f3-ee3879f34448
Park and open space | 8 MB | Texas State parcel-based areas of parks and open spaces | Dissolved by park | 2018 | City of Houston GIS (COHGIS) Open Data Portal | https://cohgis-mycity.opendata.arcgis.com/datasets/coh-park-boundary
Physical topology | 1.1 GB | Physical topology associated information including elevation | Related polygon | 2013 | USGS National Elevation Dataset (NED - 1/3 arc-second) | https://catalog.data.gov/dataset/national-elevation-dataset-ned-1-3-arc-second-downloadable-data-collection-national-geospatial
National Flood Hazard Layer | 11 GB | Geographical information of an area of land adjacent to watershed and valley with high likelihood of flooding | Related polygon | 2018 | Federal Emergency Management Agency | https://www.fema.gov/flood-maps/products-tools/national-flood-hazard-layer
Evacuation zone | - | List of zipcodes of planned evacuation zones | - | 2018 | Harris County Emergency Management | https://prepare.readyharris.org/evacuation-map
FEMA shelters | 28 MB | FEMA National Shelter System information with geotagged locations | GPS (X,Y) | 2017 | Rice University Houston Urban Data Platform | https://www.kinderudp.org/#/datasetCatalog/va7b869ng5dv
Major roads | 650 MB | Texas statewide and Houston citywide major roadway information associated with spatial segments, including roadtype, number of lanes etc. | Road segment | 2019 | Texas Departmentof Transportation, Houston Departmentof Transportation | https://gis-txdot.opendata.arcgis.com/datasets/d4f7206d27af4358acb70cb1cc819d10_0, https://cohgis-mycity.opendata.arcgis.com/datasets/coh-major-road
FEMA disaster assistance application | - | Federal aid for residents who suffered damage and losses from Hurricane Harvey | Zipcode | 2017 | Federal Emergency Management Agency | https://www.fema.gov/about/openfema/data-sets
U.S. American Community Survey | 20 MB | 2017 5-year estimate. U.S. demographicand household socioeconomic characteristics data, such as population, age, race, income level, household type etc. | Census Tract | 2017 | U.S. Census Bureau (U.S. Department of Commerce) | https://www.census.gov/programs-surveys/acs/technical-documentation/table-and-geography-changes/2017/5-year.html

# System Specifications
Mobility data were provided via dedicated AWS S3 service and were managed in accordance with NYU Institutional Review Board approval IRB-FY2018-1645 and stored and accessed in a secured environment at [New York University’s Center for Urban Science and Progress](cusp.nyu.edu) (NYU CUSP) [Research Computing Facility](https://datahub.cusp.nyu.edu/) (RCF), which is equipped with High Performance Computing (HPC) infrastructure, controlled access, and restricted connectivity. Raw data was deployed to Hadoop Cloudera cluster of 20 nodes with 256 GB memory and 2.1 GHz CPU speed each with a total of 1280 cores and 5.1 TB memory. Initial data processing was conducted using [Apache PySpark](https://spark.apache.org/docs/latest/api/python/index.html) version 2.4. The following analysis performed on the processed data was conducted on RCF's High Memory computing server with total memory of 1TB and Intel(R) Xeon(R) CPU E5-4640 0 @ 2.40GHz (4x8 cores) using predominantly [Python](https://www.python.org/) version 3.7 and [Quantum GIS](https://www.qgis.org/en/site/index.html) version 3.4 Madeira. Each script file relies on a separate set of libraries, which were listed at the begining of each of the file, including library version used for the particular task.

# License

MIT License

Copyright (c) 2021 Urban Intelligence Lab

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
