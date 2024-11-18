# before running, put utility/bin in path like so:
# export PYTHONPATH="${PYTHONPATH}:/groups/scicompsoft/home/scarlettv/dis-utilities/utility/bin"

# Run like so:
# python3 test2_authors_to_candidates.py <dir_name>
# python3 test2_authors_to_candidates.py single_author


import db_connect
import tc_common
import jrc_common.jrc_common as JRC
import doi_common.doi_common as doi_common
import sys

try:
    import name_match as nm
except:
    print('ERROR: Could not import name_match.py. Is it in your PYTHONPATH?')
    sys.exit(0)


# Boilerplate: initialize DB connection
db_connect.initialize_program()
LOGGER = JRC.setup_logging(db_connect.DummyArg()) 
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

#Boilerplate: create a TestCase object (attributes come from config file)
config = tc_common.TestCase()
config.read_config(sys.argv[1])


author_details = doi_common.get_author_details(doi_common.get_doi_record(config.doi, doi_collection), doi_collection)  #IMPORTANT: NEED TO UPDATE THE SECOND ARG HERE... SOON
all_authors = [ nm.create_author(author_record) for author_record in author_details]

ids = []
for a in all_authors:
    name = nm.HumanName(a.name)
    ids.append(nm.name_search(name.first, name.last))


if set(nm.flatten(ids)) == set(config.candidate_ids()): # use a set because order doesn't matter
    print('Pass: initial candidate employee IDs')
else:
    print(f'Fail: initial candidate employee IDs\nExpected:{config.candidate_ids()}\nReturned:{ids}')




guess_lists = [nm.propose_candidates(a) for a in all_authors]
target = config.guesses() # Guess lists from file, represented as one string

# for i in range(min(len(str(guess_lists)), len(target))):
#     if str(guess_lists)[i] != target[i]:
#         print(f"Difference at position {i}: {str(guess_lists)[i]} vs {target[i]}")
#         break

if str(guess_lists) == target:
    print('Pass: initial proposed guesses')
else:
    print(f'Fail: initial proposed guesses\nExpected:{[e for e in target]}\nReturned:{[str(e)for e in guess_lists]}')



