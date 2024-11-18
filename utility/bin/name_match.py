"""
Given a DOI, try to identify Janelia authors who don't have ORCIDs and correlate them with employees.

This script ignores the existing jrc_author metadata. It generates a new list of employee IDs,
and finally directly overwrites the previous jrc_author list.

If the author has an ORCID on the paper that isn't in our collection, this script will
create an ORCID record for that person. If appropriate, it will add an employee ID to 
an existing ORCID record.

This script will not create new "employeeId only" records in the ORCID collection.

This script will not include anyone in jrc_author if they're not in the People system.
"""

import sys
import argparse
import re
import itertools
from collections import Counter
from collections.abc import Iterable
from operator import attrgetter
from nameparser import HumanName
from rapidfuzz import fuzz, utils
from unidecode import unidecode
from termcolor import colored
import inquirer
from inquirer.themes import BlueComposure
import jrc_common.jrc_common as JRC
import doi_common.doi_common as doi_common




class Author:
    """ Author objects are constructed solely from the Crossref-provided author information. """
    def __init__(self, name, orcid=None, affiliations=None):
        self.name = name
        self.orcid = orcid
        self.affiliations = affiliations if affiliations is not None else [] # Need to avoid the python mutable arguments trap

class Employee:
    """ Employees are constructed from information found in the HHMI People database. """
    def __init__(self, id=None, job_title=None, email=None, location=None, supOrgName=None, first_names=None, middle_names=None, last_names=None, exists=False):
        self.id = id
        self.job_title = job_title
        self.email = email
        self.location = location
        self.supOrgName = supOrgName
        self.first_names = list(set(first_names)) if first_names is not None else [] # Need to avoid the python mutable arguments trap
        self.middle_names = list(set(middle_names)) if middle_names is not None else []
        self.last_names = list(set(last_names)) if last_names is not None else []
        self.exists = exists

class Guess(Employee):
    """ A Guess is a subtype of Employee that consists of just ONE name permutation 
    (e.g. Gerald M Rubin) and a fuzzy match score (calculated before the guess object is instantiated). """
    def __init__(self, id=None, job_title=None, email=None, location=None, supOrgName=None, first_names=None, middle_names=None, last_names=None, exists=False, name=None, score=None, approved=False):
        super().__init__(id, job_title, email, location, supOrgName, first_names, middle_names, last_names, exists)
        self.name = name
        self.score = score
        self.approved = approved
    def __repr__(self):
        attrs = {k:v for k, v in self.__dict__.items() if v}
        return f"Guess({', '.join(f'{k}={v}' for k, v in attrs.items())})"
        

class MongoOrcidRecord:
    def __init__(self, orcid=None, employeeId=None, exists=False):
        self.orcid = orcid
        self.employeeId = employeeId
        self.exists = exists
    def has_orcid(self):
        return True if self.orcid else False
    def has_employeeId(self):
        return True if self.employeeId else False



### Functions for instantiating objects of my custom classes

def create_author(author_info):
    if 'given' in author_info and 'family' in author_info:
        name = ' '.join((author_info['given'], author_info['family']))
    elif 'name' in author_info: # e.g. if 'FlyLight Project Team' is an author
        name = author_info['name']
    else:
        raise ValueError("ERROR: Neither 'family', 'given', nor 'name' is present in one of the author records.")
    orcid = author_info['paper_orcid'] if 'paper_orcid' in author_info else None
    affiliations = author_info['affiliations'] if author_info['asserted'] == True else None
    return Author(name, orcid, affiliations)


def create_employee(id):
    """ A function to create employee objects. Searches the People system to get employee data. """
    def parse_search_results(search_res, *keys):
        """ A helper function to find employee data in the data structure resulting from People search. """
        if len(keys) > 1:
            return [search_res.get(key, "").strip() if search_res.get(key) else None for key in keys]
        key = keys[0]
        return search_res.get(key, "").strip() if search_res.get(key) else None

    idsearch_results = search_people_api(id, mode='id')
    if idsearch_results:
        job_title = parse_search_results(idsearch_results, 'businessTitle') 
        email = parse_search_results(idsearch_results, 'email') 
        location = parse_search_results(idsearch_results, 'locationName') # will be 'Janelia Research Campus' for janelians
        supOrgName = parse_search_results(idsearch_results, 'supOrgName')
        first_names = parse_search_results(idsearch_results, 'nameFirstPreferred', 'nameFirst')  #e.g. ['Zhe J.', 'James Zhe']
        middle_names = parse_search_results(idsearch_results, 'nameMiddlePreferred', 'nameMiddle') # e.g. [None, None]
        last_names = parse_search_results(idsearch_results, 'nameLastPreferred', 'nameLast') #e.g. ['Liu', 'Liu']
        return(
            Employee(
            id=id,
            job_title=job_title,
            email=email,
            location=location,
            supOrgName = supOrgName,
            first_names=first_names, 
            middle_names=middle_names,
            last_names=last_names,
            exists=True)
        )
    else:
        return Employee(exists=False)


def create_guess(employee, name=None, score=None):
    return(
        Guess(
        employee.id, 
        employee.job_title, 
        employee.email,
        employee.location,
        employee.supOrgName, 
        employee.first_names, 
        employee.middle_names, 
        employee.last_names,
        employee.exists, 
        name, 
        score
        )
    )








### Functions for matching authors to employees



def run_name_match(arg, doi_collection, orcid_collection):
    """ 
    The core procedure of this program.
    Iterates through DOIs (if more than one is given),
    and for each DOI, iterates through authors. If the paper
    has no affiliations at all, the script will try to get
    a corresponding employee for all authors. Otherwise,
    it will only look for a corresponding employee for
    authors with a Janelia affiliation.
    Arguments: 
        arg: command-line arguments from argparse.
        doi_collection: connection to the DOI collection.
        orcid_collection: connection to the orcid collection.
    Returns:
        A bunch of messages printed to the terminal.
    """
    dois = get_dois_from_commandline(arg.DOI, arg.FILE)
    for doi in dois:
        doi_record = doi_common.get_doi_record(doi, doi_collection)
        if not doi_record:
            print(colored( (f'WARNING: Skipping {doi}. No record found in DOI collection.'), 'yellow' ))
        else:
            all_authors = get_author_objects(doi, doi_record, doi_collection)
            revised_jrc_authors = []

            for author in all_authors: 
                if author.possible_employee == True:
                    final_choice = get_corresponding_employee(author, orcid_collection, arg.VERBOSE, arg.WRITE)
                    if final_choice == None:
                        revised_jrc_authors.append(Employee(exists=False))
                    else:
                        revised_jrc_authors.append(final_choice)
                else:
                    revised_jrc_authors.append(Employee(exists=False))

            if len(revised_jrc_authors) != len(all_authors):
                raise ValueError("ERROR: Length of revised_jrc_author doesn't make sense. Did you overlook an author, or add two employeeIds for one author?")

            print("Janelia authors are highlighted below:")
            for i in range(len(revised_jrc_authors)):
                if revised_jrc_authors[i].exists:
                    print(colored(
                        (all_authors[i].name, revised_jrc_authors[i].id), "black", "on_yellow"
                    ))
                else:
                    print(all_authors[i].name)

            if arg.WRITE:
                overwrite_jrc_author(revised_jrc_authors)
            else:
                print(colored(
                    ("WARNING: Dry run successful, no updates were made"), "yellow"
                ))




# Functions for determining which authors we will try to match to employees

def get_author_objects(doi, doi_record, orcid_collection):
    print_title(doi, doi_record)
    all_authors = [ create_author(author_record) for author_record in doi_common.get_author_details(doi_record, orcid_collection)]
    all_authors = set_author_possible_employee_attr(all_authors, orcid_collection)
    # If the paper has affiliations, we will only check those authors with janelia affiliations. Otherwise, we will check all authors.
    print_janelia_authors(all_authors)
    return all_authors

def set_author_possible_employee_attr(all_authors, orcid_collection):
    new_author_list = all_authors
    if not any([a.affiliations for a in all_authors]):
        for i in range(len(new_author_list)):
            setattr(new_author_list[i], 'possible_employee', True)
    else:
        pattern = re.compile(
        r'(?i)(janelia|'  # (?i) means case-insensitive; pattern matches "Janelia" in any form, e.g., "Janelia", "thejaneliafarm", etc.
        r'(ashburn.*(hhmi|howard\s*hughes))|'  # "Ashburn" with "HHMI" or "Howard Hughes"
        r'(hhmi|howard\s*hughes).*ashburn)'  # "HHMI" or "Howard Hughes" with "Ashburn" 
        )
        for i in range(len(new_author_list)):
            setattr(new_author_list[i], 'possible_employee', is_janelian(new_author_list[i], pattern, orcid_collection))
    return new_author_list

def is_janelian(author, pattern, orcid_collection):
    result = False
    if author.orcid:
        if doi_common.single_orcid_lookup(author.orcid, orcid_collection, 'orcid'):
            result = True
    if bool(re.search(pattern, " ".join(author.affiliations))):
        result = True
    return result







# Functions for matching authors to corresponding employees

def get_corresponding_employee(author, orcid_collection, verbose_arg, write_arg): 
    """ 
    The high-level decision tree for trying to find a corresponding employee, 
    given an author. Tries to find them in the orcid collection before
    searching the People system and prompting the user to select an employee.
    Also creates or updates the relevant record in our orcid collection if needed.
    Arguments: 
        author: an author object.
        orcid_collection: connection to the orcid collection.
        verbose_arg: boolean indicating whether the --verbose flag was used.
        write_arg: boolean indicating whether the --write flag was used. 
        (Exclusion of this flag indicates user wants a dry run.)
    Returns:
        A guess object.
    """
    final_choice = None

    if author.orcid:
        mongo_orcid_record = get_mongo_orcid_record(author.orcid, orcid_collection)

        if mongo_orcid_record.exists:
            if mongo_orcid_record.has_employeeId():
                employee = create_employee(mongo_orcid_record.employeeId)
                final_choice = employee
                add_preferred_names_to_complete_orcid_record(author, employee, orcid_collection, verbose_arg)
            else:
                best_guess = guess_employee(author, f"{author.name} has an ORCID on this paper. They are in our ORCID collection, but without an employee ID.", verbose_arg)
                if best_guess.approved:
                    final_choice = best_guess
                    add_id_and_names_to_incomplete_orcid_record(best_guess, author, 'id', orcid_collection, write_arg)

        elif not mongo_orcid_record.exists:
            best_guess = guess_employee(author, f"{author.name} has an ORCID on this paper, but this ORCID is not in our collection.", verbose_arg)
            if best_guess.approved:
                final_choice = best_guess
                mongo_orcid_record = get_mongo_orcid_record(best_guess.id, orcid_collection)
                if mongo_orcid_record.exists: 
                    if not mongo_orcid_record.has_orcid(): # If the author has a never-before-seen orcid, but their employeeId is already in our collection
                        print(f"{author.name} is in our collection, with an employee ID only.")
                        add_id_and_names_to_incomplete_orcid_record(best_guess, author, 'orcid', orcid_collection, write_arg)
                    elif mongo_orcid_record.has_orcid(): # Hopefully this will never get triggered, i.e., if one person has two ORCIDs
                        print(f"{author.name}'s ORCID is {author.orcid} on the paper, but it's {mongo_orcid_record.orcid} in our collection. Aborting attempt to edit their records in our collection.")
                else:
                    print(f"{author.name} has an ORCID on this paper, and they are not in our collection.")
                    create_orcid_record(best_guess, orcid_collection, author, write_arg)

    elif not author.orcid:
        best_guess = guess_employee(author, f"{author.name} does not have an ORCID on this paper.", verbose_arg)
        if best_guess.approved:
            final_choice = best_guess

    return final_choice



def guess_employee(author, inform_message, verbose_arg):
    """
    If a name can't be found in the ORCID collection,
    search the People system, make an educated guess, and 
    prompt the user to confirm that guess.
    Arguments: 
        author: an author object.
        inform_message: an informational message with info
        about this author, to be printed if verbose==True.
        verbose_arg: boolean indicating whether the --verbose flag was used.
    Returns:
        A list of guess objects. This list will never be empty. 
        It may, however, simply contain one 'empty' guess object.
    """
    candidates = propose_candidates(author)
    best_guess = evaluate_candidates(author, candidates, inform_message, verbose_arg)
    return best_guess

def propose_candidates(author):
    """ 
    Given an author object, search the People API for one or more hits. 
    Arguments: 
        author: an author object.
    Returns:
        A list of guess objects. This list will never be empty. It may, however, simply contain one 'empty' guess object.
    """
    candidate_ids = name_search(author)
    if candidate_ids:
        candidate_employees = [create_employee(id) for id in candidate_ids]
        candidate_employees = [e for e in candidate_employees if e.location == 'Janelia Research Campus']
        guesses = fuzzy_match(author, candidate_employees)
        return guesses
    else:
        return [ Guess(exists=False) ]


def name_search(author): 
    """
    Searches all reasonable two-word permutations of a person's name in the 
    People system, using the following heuristic: search the first name
    and the last name separately, and keep the employee IDs of all employees who
    came up in both searches. (The People system only allows searching
    by one word at a time.)
    Arguments: 
        author: An author object.
    Returns:
        A list of employee objects OR None.
    """
    name = HumanName(author.name) # handles suffixes, compound names, etc.
    #^ Two examples: 'Norma Citlalicue Pérez Rosas' / 'Miguel Angel Núñez-Ochoa'
    basic = (name.first, name.last) 
    #^ ('Norma', 'Rosas') / ('Miguel', 'Núñez-Ochoa')
    stripped = (unidecode(name.first), unidecode(name.last)) 
    #^ ('Norma', 'Rosas') / ('Miguel', 'Nunez-Ochoa')
    hyphen_split1 = (name.first, name.last.split('-')[0]) if '-' in name.last else None 
    #^ None / ('Miguel', 'Núñez')
    hyphen_split2 = (name.first, name.last.split('-')[1]) if '-' in name.last else None 
    #^ None / ('Miguel', 'Ochoa')
    strp_hyph1 = (unidecode(name.first), unidecode(name.last.split('-')[0])) if '-' in name.last else None 
    #^ None / ('Miguel', 'Nunez')
    strp_hyph2 = (unidecode(name.first), unidecode(name.last.split('-')[1])) if '-' in name.last else None 
    #^ None / ('Miguel', 'Ochoa')
    two_middle_names1 = (name.first, name.middle.split(' ')[0]) if len(name.middle.split())==2 else None 
    #^ ('Norma', 'Citlalicue') / None
    two_middle_names2 = (name.first, name.middle.split(' ')[1]) if len(name.middle.split())==2 else None
    #^ ('Norma', 'Pérez') / None
    strp_middle1 = (unidecode(name.first), unidecode(name.middle.split()[0])) if len(name.middle.split())==2 else None 
    #^ ('Norma', 'Citlalicue') / None
    strp_middle2 = (unidecode(name.first), unidecode(name.middle.split()[1])) if len(name.middle.split())==2 else None
    #^ ('Norma', 'Perez') / None
    name_tuples = [basic, 
                   stripped, 
                   hyphen_split1, 
                   hyphen_split2, 
                   strp_hyph1, 
                   strp_hyph2, 
                   two_middle_names1, 
                   two_middle_names2, 
                   strp_middle1, 
                   strp_middle2]
    name_tuples = [tup for tup in name_tuples if tup is not None]
    name_tuples = list(set(name_tuples))
    result = []
    for tup in name_tuples:
        search_results1 = search_people_api(tup[0], mode='name') 
        #^ a list of dicts, e.g. a dict for Virginia Scarlett and a dict for Virginia Ruetten
        search_results2 = search_people_api(tup[1], mode='name') 
        #^ a list of dicts, e.g. a dict for Virginia Scarlett and a dict for Scarlett Pitts
        if search_results1 and search_results2:
            employee_ids1 = {item['employeeId'] for item in search_results1}
            employee_ids2 = {item['employeeId'] for item in search_results2}
            common_ids = list(employee_ids1.intersection(employee_ids2))
            common_ids = [id for id in common_ids if id is not None] # you never know...
            if common_ids:
                for id in common_ids:
                    result.append(id)
    candidate_ids = list(set(result))
    if candidate_ids:
        return candidate_ids
    else:
        return None
 

def fuzzy_match(author, candidate_employees):
    """ 
    Arguments: 
        author: an author object.
        candidate_employees: a list of employee objects, or None.
    Returns:
        A list of guess objects. This list will never be empty. It may, however, simply contain one 'non-existent' guess object.
    """
    guesses = []
    if candidate_employees:
        for employee in candidate_employees:
            employee_permuted_names = generate_name_permutations(employee.first_names, employee.middle_names, employee.last_names)
            for name in employee_permuted_names:
                guesses.append(create_guess(employee, name=name)) 
                # Each employee will generate several guesses, e.g. Virginia T Scarlett, Virginia Scarlett, Ginnie Scarlett
    if guesses:
        for guess in guesses:
            guess.score = fuzz.token_sort_ratio(author.name, guess.name, processor=utils.default_process) 
            #^processor will convert the strings to lowercase, remove non-alphanumeric characters, and trim whitespace
        high_score = max( [g.score for g in guesses] )
        winners = [ g for g in guesses if g.score == high_score ]
        return winners
    else:
        return [ Guess(exists=False) ]



def evaluate_candidates(author, candidates, inform_message, verbose=False):
    """ 
    A function that lets the user manually evaluate the best-guess employee for a given author. 
    Arguments:
        author: an author object.
        candidates: a non-empty list of guess objects. 
        inform_message: an informational message will be printed to the terminal if verbose==True OR if some action is needed.
            one of:
                f"{author.name} is in our ORCID collection, but without an employee ID."
                f"{author.name} has an ORCID on this paper, but this ORCID is not in our collection."
                f"{author.name} does not have an ORCID on this paper."
        verbose: a boolean, passed from command line.
    Returns:
        A guess object. If this guess.exists == False, this indicates that the guess was rejected, either by the user or automatically due to low score.
    """    
    if len(candidates) > 1:
        print(inform_message)
        print(f"Multiple high scoring matches found for {author.name}:")
        # Some people appear twice in the HHMI People system. Sometimes this is just bad bookkeeping, 
        # and sometimes it's because two employees have the same exact name.
        # inquirer gives us no way of knowing whether the user selected the first instance of 'David Clapham' or the second instance of 'David Clapham'.
        # We are appending numbers to the names to make them unique, and then using the index of the selected object to grab the original object.
        repeat_names = [name for name, count in Counter(guess.name for guess in candidates).items() if count > 1]
        selection_list = []
        counter = {}
        for guess in candidates:
            if guess.name in repeat_names:
                if guess.name in counter:
                    selection_list.append(Guess(id=guess.id,job_title=guess.job_title,email=guess.email,location=guess.location,supOrgName = guess.supOrgName,first_names=guess.first_names,middle_names=guess.middle_names,last_names=guess.last_names, exists=guess.exists,
                                                name=guess.name+f'-{counter[guess.name]+1}',score=guess.score))
                    counter[guess.name] += 1
                else:
                    selection_list.append(Guess(id=guess.id,job_title=guess.job_title,email=guess.email,location=guess.location,supOrgName = guess.supOrgName,first_names=guess.first_names,middle_names=guess.middle_names,last_names=guess.last_names, exists=guess.exists,
                                                name=guess.name+f'-1',score=guess.score))
                    counter[guess.name] = 1
            else:
                selection_list.append(guess)
        for guess in selection_list:
            print(colored(f"{guess.name}, ID: {guess.id}, job title: {guess.job_title}, supOrgName: {guess.supOrgName}, email: {guess.email}", 'black', 'on_yellow'))
        quest = [inquirer.Checkbox('decision', 
                                   carousel=True, 
                                   message="Choose a person from the list", 
                                   choices=[guess.name for guess in selection_list] + ['None of the above'], 
                                   default=['None of the above'])]
        ans = inquirer.prompt(quest, theme=BlueComposure()) # returns {'decision': a list}, e.g. {'decision': ['Virginia Scarlett']}
        while len(ans['decision']) > 1: 
            print('Please choose only one option.')
            quest = [inquirer.Checkbox('decision', 
                            carousel=True, 
                            message="Choose a person from the list", 
                            choices=[guess.name for guess in selection_list] + ['None of the above'], 
                            default=['None of the above'])]
            ans = inquirer.prompt(quest, theme=BlueComposure()) 
        if ans['decision'] != ['None of the above']:
            index = [guess.name for guess in selection_list].index(ans['decision'][0])
            winner = candidates[index]
            winner.approved = True
            return winner
        elif ans['decision'] == ['None of the above']:
            print(f"No action will be taken for {author.name}.\n")
            return Guess(exists=False) 

    elif len(candidates) == 1:
        best_guess = candidates[0]
        if not best_guess.exists:
            if verbose:
                print(f"A Janelian named {author.name} could not be found in the HHMI People API. No action to take.\n")
            return(best_guess)
        if float(best_guess.score) < 85.0:
            if verbose:
                print(inform_message)
                print(
                    f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, supOrgName: {best_guess.supOrgName}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}\n"
                    )
            return Guess(exists=False)
        elif float(best_guess.score) > 85.0:
            print(inform_message)
            print(colored(
                f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, supOrgName: {best_guess.supOrgName}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}",
                "black", "on_yellow"
                ))
            quest = [inquirer.List('decision', 
                                   message=f"Add {best_guess.name} to this paper's Janelia authors?", 
                                   choices=['Yes', 'No'])]
            ans = inquirer.prompt(quest, theme=BlueComposure())
            if ans['decision'] == 'Yes':
                best_guess.approved = True
                return best_guess
            else:
                print(f"No action will be taken for {author.name}.\n")
                return Guess(exists=False)





### Functions to search and write to databases



def add_preferred_names_to_complete_orcid_record(author, employee, orcid_collection, verbose_arg):
    doi_common.add_orcid_name(
        lookup=author.orcid, 
        lookup_by='orcid', 
        given=first_names_for_orcid_record(author, employee), 
        family=last_names_for_orcid_record(author, employee), 
        coll=orcid_collection)
    if verbose_arg:
        print( f"{author.name} has an ORCID on this paper. They are in our ORCID collection, with both an ORCID an employee ID.\n" )

def add_id_and_names_to_incomplete_orcid_record(employee, author, to_add, orcid_collection, write_arg):
    if to_add not in {'id', 'orcid'}:
        raise ValueError("to_add argument to add_id_and_names_to_incomplete_orcid_record() must be either 'orcid' or 'id'.")
    if to_add == 'id':
        if not doi_common.single_orcid_lookup(
            employee.id, 
            orcid_collection, 
            'employeeId'):
            if write_arg:
                doi_common.update_existing_orcid(
                    lookup=author.orcid, 
                    lookup_by='orcid', 
                    coll=orcid_collection, 
                    add=employee.id)
                doi_common.add_orcid_name(
                    lookup=author.orcid, 
                    lookup_by='orcid', 
                    coll=orcid_collection, 
                    given=first_names_for_orcid_record(author, employee), 
                    family=last_names_for_orcid_record(author, employee))
        else:
            print(f'ERROR: {author.name} has at least two records in our orcid collection. Aborting attempt to add employeeId {employee.id} to existing record for this ORCID: {author.orcid}')
    if to_add == 'orcid':
        if not doi_common.single_orcid_lookup(
            author.orcid, 
            orcid_collection, 
            'orcid'):
            if write_arg:
                doi_common.update_existing_orcid(
                    lookup=employee.id, 
                    lookup_by='employeeId', 
                    add=author.orcid)
                doi_common.add_orcid_name(
                    lookup=employee.id, 
                    lookup_by='employeeId', 
                    coll=orcid_collection, 
                    given=first_names_for_orcid_record(author, employee), 
                    family=last_names_for_orcid_record(author, employee))
        else:
            print(f'ERROR: {author.name} has two records in our orcid collection. Aborting attempt to add orcid {author.orcid} to existing record for this employeeId: {employee.id}')

def create_orcid_record(best_guess, orcid_collection, author, write_arg):
    if write_arg:
        doi_common.add_orcid(
            best_guess.id, 
            orcid_collection, 
            given=first_names_for_orcid_record(author, best_guess), 
            family=last_names_for_orcid_record(author, best_guess), orcid=author.orcid)
        print(f"Record created for {author.name} in orcid collection.")

def generate_name_permutations(first_names, middle_names, last_names):
        middle_names = [n for n in middle_names if n not in ('', None)] 
        # some example middle_names from HHMI People system: [None], ['D.', ''], ['Marie Sophie'], ['', '']
        permutations = set()
        # All possible first names + all possible last names
        for first_name, last_name in itertools.product(first_names, last_names):
            permutations.add(
                f"{first_name} {last_name}"
            )
        # All possible first names + all possible middle names + all possible last names
        if middle_names:
            for first_name, middle_name, last_name in itertools.product(first_names, middle_names, last_names):
                permutations.add(
                    f"{first_name} {middle_name} {last_name}"
                )
        # All possible first names + all possible middle initials + all possible last names
            for first_name, middle_name, last_name in itertools.product(first_names, middle_names, last_names):
                middle_initial = middle_name[0]
                permutations.add(
                    f"{first_name} {middle_initial} {last_name}"
                )
        return list(sorted(permutations))

def first_names_for_orcid_record(author, employee):
    result = generate_name_permutations(
        [HumanName(author.name).first]+employee.first_names, 
        [HumanName(author.name).middle]+employee.middle_names,
        [HumanName(author.name).last]+employee.last_names
    )
    h_result = [HumanName(n) for n in result]
    return list(set([' '.join((n.first,n.middle)).strip() for n in h_result]))

def last_names_for_orcid_record(author, employee):
    result = generate_name_permutations(
        [HumanName(author.name).first]+employee.first_names, 
        [HumanName(author.name).middle]+employee.middle_names,
        [HumanName(author.name).last]+employee.last_names
    )
    h_result = [HumanName(n) for n in result]
    return list(set([n.last for n in h_result]))


def get_mongo_orcid_record(search_term, orcid_collection):
    if not search_term:
        return(MongoOrcidRecord(exists=False))
    else:
        result = ''
        if len(search_term) == 19: #ORCIDs are guaranteed to be 16 digits (plus the hyphens)
            result = doi_common.single_orcid_lookup(search_term, orcid_collection, 'orcid')
        else:
            result = doi_common.single_orcid_lookup(search_term, orcid_collection, 'employeeId')
        if result:
            if 'orcid' in result and 'employeeId' in result:
                return(MongoOrcidRecord(orcid=result['orcid'], employeeId=result['employeeId'], exists=True))
            if 'orcid' in result and 'employeeId' not in result:
                return(MongoOrcidRecord(orcid=result['orcid'], exists=True))
            if 'orcid' not in result and 'employeeId' in result:
                return(MongoOrcidRecord(employeeId=result['employeeId'], exists=True))
        else:
            return(MongoOrcidRecord(exists=False))

def search_people_api(query, mode):
    response = None
    if mode not in {'name', 'id'}:
        raise ValueError("ERROR: HHMI People API search mode must be either 'name' or 'id'.") 
    if mode == 'name':
        response = JRC.call_people_by_name(query)
    elif mode == 'id':
        response = JRC.call_people_by_id(query)
    return response

def strip_orcid_if_provided_as_url(orcid):
    prefixes = ["http://orcid.org/", "https://orcid.org/"]
    for prefix in prefixes:
        if orcid.startswith(prefix):
            return orcid[len(prefix):]
    return orcid

def overwrite_jrc_author(revised_jrc_authors):
    id_list = [e.id for e in revised_jrc_authors]
    id_list = [id for id in id_list if id]
    payload = {'jrc_author': id_list}
    doi_common.update_jrc_fields(doi, doi_collection, payload)
    print(colored( ('jrc_author field has been updated.'), 'green' ))






### Miscellaneous low-level functions and variables

def get_dois_from_commandline(doi_arg, file_arg):
    dois = [doi_arg.lower()] if doi_arg else [] # .lower() because our collection is case-sensitive
    if file_arg:
        try:
            with open(file_arg, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.strip().lower())
        except Exception as err:
            print(f"Could not process {file_arg}")
            exit()
    return dois

def print_title(doi, doi_record):
    if 'titles' in doi_record: # DataCite
        print(f"{doi}: {doi_record['titles'][0]['title']}")
    else: # Crossref
        print(f"{doi}: {doi_record['title'][0]}")

def print_janelia_authors(all_authors):
    print(", ".join( [a.name for a in all_authors if a.possible_employee == True] ))

def flatten(xs): # https://stackoverflow.com/questions/2158395/flatten-an-irregular-arbitrarily-nested-list-of-lists
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x


DB = {}
PROJECT = {}

def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Database
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        manifold = 'prod'
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    try:
        rows = DB['dis'].project_map.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PROJECT[row['name']] = row['project']

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
            LOGGER.critical(msg)
            sys.exit(-1 if msg else 0)






# -----------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
    description = "Given a DOI, use fuzzy name matching to correlate Janelia authors who don't have ORCIDs to Janelia employees. Update ORCID records as needed.")
    muexgroup = parser.add_mutually_exclusive_group(required=True)
    muexgroup.add_argument('--doi', dest='DOI', action='store',
                         help='Curate janelia authors for a single DOI.')
    muexgroup.add_argument('--file', dest='FILE', action='store',
                         help='Curate janelia authors for multiple DOIs in a file.')
    parser.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    parser.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    parser.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write results to database. If --write is missing, no changes to the database will be made.')
    
    arg = parser.parse_args()
    LOGGER = JRC.setup_logging(arg) 

    # Connect to the database
    initialize_program()
    orcid_collection = DB['dis'].orcid
    doi_collection = DB['dis'].dois

    run_name_match(arg, doi_collection, orcid_collection)



#For bug testing
# import name_match as nm
# nm.initialize_program()
# orcid_collection = nm.DB['dis'].orcid
# doi_collection = nm.DB['dis'].dois
# doi = '10.1101/2024.09.16.613338'
# doi_record = nm.doi_common.get_doi_record(doi, doi_collection)
# all_authors = nm.get_author_objects(doi, doi_record, doi_collection)

