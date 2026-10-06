# tgedr-meddra

![Coverage](./coverage.svg)
[![PyPI](https://img.shields.io/pypi/v/tgedr-meddra)](https://pypi.org/project/tgedr-meddra/)



## development
- main requirements:
  - _uv_  
  - _bash_
- Clone the repository like this:

  ``` bash
  git clone git@github.com:jtviegas/meddra
  ```
- cd into the folder: `cd meddra`
- install requirements: `./helper.sh reqs`


# Introduction 
This repository contains the Medical Dictionary for Regulatory Affairs (MedDRA) logic to be used accross data science projects in Safety Surveillance. For more information on MedDRA please visit https://www.meddra.org/.

# rationale

- MedDRA files, in its various versions, are kept in a datacore Volume: `/Volumes/global_safety_ds_bronze/faers/filesets/meddra/`
  ![MedDRA files](assets/files.png)

- the main purpose of the data pipeline at this point is to create a map of preferred term (PT) codes to its names, present and past, so that we can relate it to other datasets. For that we use the files:
  - `pt.asc`
  - `meddra_history_english.asc`

  ![pipeline](assets/pipeline.png)

  The rationale for this mapping can be depicted in the following image...
  ![rationale](assets/pt_rationale.png)

  steps:
    - filter history file on PT `term type` only and on the actions (Add, Update, Delete)
    - join both tables on the PT code (pt_code==term_code) allowing all PT file entries, even if not matched
    - this will relate every PT code to eventually multiple PT names, the ones that were added, updated, and deleted, so that we can keep track of PT names used in hte past elsewhere


  the output is a `delta lake` table, currently in [datacore](https://adb-1820730458762836.16.azuredatabricks.net/explore/data/global_safety_ds_bronze/faers/meddra_preferred_terms?o=1820730458762836%3Fo%3D1820730458762836) that can be accessed in the catalog:
  
  ![table in catalog](assets/table.png)



# Updating to latest MedDRA version
The MedDRA data is updated twice a year. 

When a new update is available the following steps must be carried out to ensure that the repository contains the latest version:
1. Move the existing MedDRA files in **data\medascii** to **data\archieve\medascii_XX.X**, where XX.X is the version number. 
2. Copy the latest MedDRA version to **data\medascii**. The lastest version can be found in **GSdata:\IT\MEDDRA\MedDRA Latest version**
3. Update README.MD with new MedDRA version
4. Run tests
5. Commit and push changes to repository

# Description of MedDRA data
The contents of the MedDRA data files and the data model in described "dist_file_format_XX_English.pdf" located in the [docs/ folder](https://dev.azure.com/novonordiskit/Data%20Science%20Safety%20Surveillance/_git/meddra?path=%2Fdocs)

# future work
- mapping to HLT and HLGT and how to handle the multi-axial nature of medDRA hierarchy
- add columns description in the output dataset
