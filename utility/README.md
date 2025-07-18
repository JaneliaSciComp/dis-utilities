# dis-utilities:utility

## Utility programs for DIS system

| Name                       | Description                                                                         |
| -------------------------- | ----------------------------------------------------------------------------------- |
| add_newsletter.py          | Add, change, or remove a DOI's newsletter date                                      |
| add_preprint.py            | Add a preprint relationship for a particular preprint-article pair                  |
| assign_authors.py          | Add/remove JRC authors for a given DOI                                              |
| create_api_key.pl          | Create a JWT token for a user                                                       |
| delete_doi.py              | Delete one or more DOIs from the dois collection                                    |
| edit_orcid.py              | Edit a record in the orcid collection                                               |
| find_missing_orcids.py     | Find entries in the People system with groups (lab heads) but no ORCID              |
| get_citation.py            | Print citations to the terminal in Janelia newsletter format                        |
| name_match.py              | Interactively curate the list of Janelia authors (jrc_author) for one or more DOIs  |
| search_people.py           | Search for a name in the People system                                              |
| set_alumni.py              | Add an alumni tag to a Janelian's metadata in the orcid collection                  |
| update_name.py             | GUI to update names in the orcid table                                              |
| update_tags.py             | Modify tags (and optionally add newletter date) to DOIs                             |
| weekly_pubs.py             | A wrapper for the whole weekly curation pipeline                                    |


### Setup

These scripts have a number of dependencies, listed in requirements.txt. 
You will need to start by creating a Python virtual environment:

    cd utility/bin
    python3 -m venv my_venv

Then enter the virtual environment and install necessary libraries:

    source my_venv/bin/activate
    my_venv/bin/pip install -r requirements.txt

Programs can now be run in the virtual environment:

    my_venv/bin/python3 add_newsletter.py --verbose

### Dependencies

1. The libraries specified in requirements.txt need to be installed.
2. The [Configuration system](https://github.com/JaneliaSciComp/configurator) must be accessible. The following configurations are used:
    - databases
    - dis
    - rest_services
3. The following keys must be present in the run environment:
    - CONFIG_SERVER_URL: base URL for Configuration system
    - PEOPLE_API_KEY: API key for HHMI People system

### Tips for all utility scripts

* Most of these scripts can be run on either a single doi (--doi) or a file containing a list of dois (--file). For example, `python3 weekly_pubs.py --doi 10.1234/5678` or `python3 weekly_pubs.py --file dois.txt`
* The `--write` flag tells the script that you want to make changes. If the `--write` flag is omitted, a dry run will be performed and changes will not be made.
* The `--verbose` flag will run the script with progress messages.
* The `-h` flag will show a help menu listing the script's command line arguments. For example, `python3 weekly_pubs.py -h`

## Key scripts in the weekly pipeline

### weekly_pubs.py

This is a wrapper script for the whole weekly curation pipeline. 
For a DOI or batch of DOIs, it runs ../../sync/bin/update_dois.py, name_match.py, update_tags.py, and get_citation.py, in that order.
Any of these scripts can also be run on its own.
It will not add DOIs to the database that are already in the database, but it will run the rest of the scripts on those DOIs.

Example usage:

    python3 weekly_pubs.py --doi 10.1038/s41586-024-07939-3 --write --verbose

If you want to simply add a DOI to the database without running the rest of the pipeline, run with the --sync_only flag. As always, you must add the --write flag for the change to persist in the database.
For example:

    python3 weekly_pubs.py --doi 10.1038/s41586-024-07939-3 --write --sync_only

It is better to add DOIs to the database this way, rather than running sync/bin/update_dois.py directly, because this script performs a couple of addition quality checks on the DOIs.

### name_match.py

This script attempts to match paper authors to Janelia employees, based on ORCIDs if possible.
If no ORCID is available for an author, then the script makes a 'best guess' using fuzzy string matching between the author name and names in the People System.
The script will prompt the user to approve these guesses.

This script will update the jrc_author field in the DOI metadata in the dois collection. 
Because of the way the system is set up, the list of Janelia authors on the browser interface will not reflect your changes to jrc_author. 
Rest assured, though, your changes will be stored in the database, as long as you use the --write flag. 
The new Janelia.org uses jrc_author to determine Janelia authors for a paper.

Example usage:

    python3 name_match.py --doi 10.1038/s41586-024-07939-3 --write --verbose

### update_tags.py

This is an interactive script in which the user is prompted to select the appropriate tags for a DOI, and optionally add a newsletter date. 
A reasonable list of best guesses is presented, but the user may select from all possible tags if they wish.
'Tags' is our jargon for labels representing labs, project teams, or support teams. 
Tags are derived from the Janelia authors' HHMI People profiles. 
Tags include HHMI supervisory organization codes ('supOrg codes'), as well as supOrg names and cost center descriptions.
The only tags that really matter, though, are the supOrg codes. 

Our curation conventions:
* It is better NOT to select tags that say '(not a supervisory organization)'. These are just here for informational purposes.
* If a postdoc or research assistant is an author but their group leader is not, we do not tag that DOI with that lab's tag(s).

Example usage:

    python3 update_tags.py --doi 10.1101/2023.07.18.549527 --write

### get_citation.py

This script will print one or more article citations in the Janelia newsletter format. The DOIs must be in the database already. 

Typical usage:

    python3 get_citation.py --doi 10.1038/s41586-024-07939-3

Output looks like this:

    Farrants, H, Shuai, Y, Lemon, WC, Monroy Hernandez, C, Zhang, D, Yang, S, Patel, R, Qiao, G, Frei, MS, Plutkis, SE, Grimm, JB, Hanson, TL, Tomaska, F, Turner, GC, Stringer, C, Keller, PJ, Beyene, AG, Chen, Y, Liang, Y, Lavis, LD, Schreiter, ER. A modular chemigenetic calcium indicator for multiplexed in vivo functional imaging. https://doi.org/10.1038/s41592-024-02411-6.
    Preprint: https://doi.org/10.1101/2023.07.18.549527

Sometimes, you'll want a citation for a DOI that can't be added to the database because it's not in Crossref yet. 
(This can happen with brand-new bioRxiv DOIs.) 
In these cases, you can generate a Janelia-style citation from a RIS file:

    python3 get_citation.py --ris global-neuron-shape-reasoning-with-point-affinity-transformers.ris 

To produce:

    Troidl, J, Knittel, J, Li, W, Zhan, F, Pfister, H, Turaga, S. Global Neuron Shape Reasoning with Point Affinity Transformers. https://doi.org/10.1101/2024.11.24.625067

## Additional useful scripts

### add_newsletter.py

This script lets you add, change, or remove a DOI's newsletter date. 
By default, DOIs do not have a newsletter field at all, so when you add a newsletter date, you create the newsletter field.
On the new janelia.org, the web developers will only post items with a newsletter date on the website.

Usage examples:
    python3 add_newsletter.py --file dois.txt --write --date 2024-11-22
    python3 add_newsletter.py --doi 10.1234/5678 --write --remove

### add_preprint.py

This script will create a preprint relationship for a particular preprint-article pair in the metadata for two DOIs.
Both articles should be in the database already before running this script.

Add the preprint relationship to both DOIs' metadata with one command: 

python3 add_preprint.py --journal 10.1111/2222 --preprint 10.3333/4444 --write 

### set_alumni.py

Use this script to mark a Janelian as alumni in the orcid collection. 
Alumni is a boolean, but in practice, the alumni field is never false. 
If the person is a current Janelia employee, the alumni field simply does not exist. 
If they are a former Janelia employee, alumni=true.
The alumni field is added automatically when an employee ID that we have in the orcid collection is no longer in the People system. 
The purpose of the alumni field is partly just for our own information, and also people with alumni=true are not added to jrc_authors in the dois collection.

Example usage:

    python3 set_alumni.py --orcid 0000-1111-2222-3333 --write 
    python3 set_alumni.py --employee J0123 --write 

To remove an alumni tag, do this: 

    python3 set_alumni.py --orcid 0000-1111-2222-3333 --write --unset 
