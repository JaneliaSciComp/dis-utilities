"""get_citation.py
Get the citation(s) for one or more DOIs through the DIS DB API.
Print to terminal for easy copy-pasting to HughesHub.
"""

import requests
import argparse
import sys
from nameparser import HumanName
from operator import attrgetter
from termcolor import colored
import jrc_common.jrc_common as JRC


class Item:
    def __init__(self, citation=None, preprint=None):
        self.citation = citation  # a string
        self.preprint = (
            preprint  # If the DOI is a journal article, this is a list. else None
        )


def create_item(doi):
    rest = JRC.get_config("rest_services")
    url_base = attrgetter("dis.url")(rest)
    response = get_request(
        f"{url_base}citation/dis/{replace_slashes_in_doi(strip_doi_if_provided_as_url(doi))}"
    )
    if response:
        doi_record = get_doi_record(doi)
        item_type = get_type(doi_record)
        citation = response["data"]
        if "jrc_preprint" in response and item_type == "Journal article":
            return Item(citation=citation, preprint=response["jrc_preprint"])
        else:
            return Item(citation=citation, preprint=None)
    else:
        print(colored((f"WARNING: Unable to retrieve a citation for {doi}"), "yellow"))
        return None


def get_doi_record(doi):
    rest = JRC.get_config("rest_services")
    url_base = attrgetter("dis.url")(rest)
    url = f"{url_base}doi/{replace_slashes_in_doi(strip_doi_if_provided_as_url(doi))}"
    response = get_request(url)
    return response["data"]


def get_type(doi_record):
    if "type" in doi_record:  # crossref
        if doi_record["type"] == "journal-article":
            return "Journal article"
        if doi_record["type"] == "posted-content":
            if doi_record["subtype"] == "preprint":
                return "Preprint"
    else:  # datacite
        return doi_record["types"]["resourceTypeGeneral"]


### Functions for formatting and printing citations


def parse_ris(lines):
    with open(arg.RIS, "r") as inF:
        lines = inF.readlines()
        title = None
        doi = None
        authors = []
        for line in lines:
            try:
                code, content = line.split("  - ")[0], line.split("  - ")[1].strip()
                if code == "T1":
                    title = content
                if code == "DO":
                    doi = content
                if code == "AU":
                    authors.append(HumanName(content))
            except:
                continue

        author_str = ", ".join(
            [f"{name.last}, {''.join(name.initials_list()[:-1])}" for name in authors]
        )
        citation = f"{author_str}. {title}. https://doi.org/{doi}."
        return citation


def print_citation(item):
    if item.preprint:
        print(f"{item.citation}")
        for n in range(len(item.preprint)):
            if n == len(item.preprint) - 1:
                print(f"Preprint: https://doi.org/{item.preprint[n]}\n")
            else:
                print(f"Preprint: https://doi.org/{item.preprint[n]}")
    else:
        print(f"{item.citation}\n")


### Miscellaneous low-level functions


def replace_slashes_in_doi(doi):
    return doi.replace(
        "/", "%2F"
    )  # e.g. 10.1186/s12859-024-05732-7 becomes 10.1186%2Fs12859-024-05732-7


def strip_doi_if_provided_as_url(doi, substring=".org/10.", doi_index_in_substring=5):
    # Find all occurrences of the substring
    occurrences = [i for i in range(len(doi)) if doi.startswith(substring, i)]
    if len(occurrences) > 1:
        print("Warning: Please check that your DOI is formatted correctly.")
        exit(1)  # Exit with a warning code
    elif len(occurrences) == 1:
        doi_index_in_string = occurrences[0]
        stripped_doi = doi[doi_index_in_string + doi_index_in_substring :]
        return stripped_doi
    else:
        return doi


def get_request(url):
    headers = {"Content-Type": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"ERROR: GET request status code: {response.status_code}. Error message: {response.reason}"
        )
        # sys.exit(1)
        return None


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Turn a list of DOIs into a list of citations in Janelia Science News format."
    )
    muexgroup = parser.add_mutually_exclusive_group(required=True)
    muexgroup.add_argument(
        "--doi",
        dest="DOI",
        action="store",
        help="Produce a citation from a single DOI.",
    )
    muexgroup.add_argument(
        "--file",
        dest="FILE",
        action="store",
        help="Produce a citation from a file containing one or more DOIs.",
    )
    muexgroup.add_argument(
        "--ris", dest="RIS", action="store", help="Print citations from a .ris file."
    )

    arg = parser.parse_args()

    items = []
    if arg.DOI:
        items.append(create_item(arg.DOI.strip().lower()))
    if arg.FILE:
        try:
            with open(arg.FILE, "r") as inF:
                for doi in inF.read().splitlines():
                    if (
                        doi.strip()
                    ):  # don't throw an error if you encounter an empty line
                        items.append(create_item(doi.strip().lower()))
        except:
            print(f"Could not process {arg.FILE}")
            raise ImportError

    if arg.RIS:
        print(parse_ris(arg.RIS))
        sys.exit(0)

    items = [i for i in items if i is not None]
    for item in sorted(items, key=lambda i: i.citation):
        print_citation(item)


# debugging: 10.7554/elife.90523 is a journal article with multiple preprints
