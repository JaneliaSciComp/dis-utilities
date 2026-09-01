# dis-utilities [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)

[![GitHub last commit](https://img.shields.io/github/last-commit/JaneliaSciComp/dis-utilities.svg)](https://github.com/JaneliaSciComp/dis-utilities)
[![GitHub commit merge status](https://img.shields.io/github/commit-status/badges/shields/master/5d4ab86b1b5ddfb3c4a70a70bd19932c52603b8c.svg)](https://github.com/JaneliaSciComp/dis-utilities)

[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-000000?style=for-the-badge&logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![Bootstrap](https://img.shields.io/badge/Bootstrap-563D7C?style=for-the-badge&logo=bootstrap&logoColor=white)](https://getbootstrap.com/)
[![jQuery](https://img.shields.io/badge/jQuery-0769AD?style=for-the-badge&logo=jquery&logoColor=white)](https://jquery.com/)
[![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

## DOI system and utilities for Data and Information Services

This system automatically discovers papers and datasets published by HHMI Janelia staff and stores them in a MongoDB database. The automated scripts, which are run periodically (nightly or weekly), also make "educated guesses" about metadata that are of strategic interest to Janelia, such as labs, teams, and employees who contributed to the work. Utility scripts allow the librarian to curate these metadata in a semi-automated fashion. A Flask-based application provides a user interface, visualizations, and a REST API.

This repository is split into four sections:

- [api](api/README.md): Web-based user interface and REST API
- [etl](etl/README.md): programs for ETL (Extract-Transform-Load) for creating/maintaining DIS database
- [sync](sync/README.md): programs meant to be periodically run in the backgroud to sync the DIS database from external data sources
- [utility](utility/README.md): utility programs to be run interactively on the command line, for CRUD operations on database collections

## DIS system architecture
![DIS system architecture](DIS_architecture.png?raw=true "DIS system architecture")

The DIS system is based on a MongoDB database with collections to persist DOIs, ORCIDs, and project mappings. Python programs are used for ETL and updates. A Flask-based application provides user interface, visualizations, and a REST API.

The DIS MongoDB database contains several collections:
- *dois*: local persistence of records from Crossref or DataCite along with Janelia metadata
- *orcid*: Janelia authors. Data in this collection is drawn fro ORCID and the HHMI People system.
- *api_endpoint_details*: calls per API endpoint
- *cv*: controlled vocabularies
- *cvterm*: terms for controlled vocabularies
- *dois_to_process*: transient storage for DOIs that are present in secondary systems (e.g. bioRxiv) but not yet available in Crossref/DataCite
- *external_dois*: DOIs not published by Janelia, but reference Janelia in the acknowledgements
- *org_group*: organization groupings
- *processing*: DOI processing event log
- *project_map*: mapping of alternate project names to approved tags
- *search_regex*: regexes for acknowledgement parsing
- *subscription*: journal subscriptions
- *suporg*: supervisory organizations
- *to_ignore*: entities (including DOIs) to ignore

### User interface
Here's a sample DOI:
![DOI page](sample_doi.png?raw=true "DOI page")

### Python command line programs
The Python programs in the [sync](sync/README.md) and [utility](utility/README.md) sections of this repository are meant to be run from the Unix command line, preferably from inside a Python virtual environment. To see which command line parameters may be specified for programs, use --help:

    my_venv/bin/python3 update_dois.py --help

### Common command line parameters
Most of the command line programs have a set of common parameters:

- --manifold: used to specify the MongoDB database manifold (dev or prod)
- --write: actually write to the database. If not specified, no rows will be updated in the MongoDB database
- --verbose: verbose mode for logging - status messages are printed to STDOUT - this is chatty
- --debug: debug mode for logging - debug messages are printed to STDOUT - this is chatty in the extreme

Other common parameters:
- --doi: a single DOI to process
- --file: a file of DOIs to process (one DOI per line)

### Configuration
While this system does use some config files, the database credentials are stored in the <a href="https://github.com/JaneliaSciComp/configurator" target="_blank">Configuration system</a>.

### Production server
The current production server is dis.int.janelia.org. If this changes, you'll need to modify nginx.conf.
