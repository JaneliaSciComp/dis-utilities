# dis-utilities:sync

## ETL programs for DIS system

| Name                   | Description                                                            |
| ---------------------- | ---------------------------------------------------------------------- |
| add_author.py          | Add an author (or authors) to the orcid collection                     |
| add_hq_newsletter.py   | Add jrc_newsletter dates to DOIs                                       |
| add_hq_tags.py         | Add tags from HQ database used to populate janelia.org                 |
| convert_tags.py        | Convert tag lists to tag dictionaries                                  |
| fix_jrc_author.py      | Add jrc_author field to DOIs that don't have it                        |
| fix_middle_names.py    | Expand given names in the orcid collection                             |

### Setup

First, create a Python virtual environment:

    cd sync/bin
    python3 -m venv my_venv

Enter the virtual environment and install necessary libraries:

    source my_venv/bin/activate
    my_venv/bin/pip install -r requirements.txt

Programs can now be run in the virtual environment:

    cd ../../etl/bin
    my_venv/bin/python3 fix_middle_names.py --verbose

### Dependencies

1. The libraries specified in requirements.txt need to be installed.
2. The [Configuration system](https://github.com/JaneliaSciComp/configurator) must be accessible. The following configurations are used:
    - databases
