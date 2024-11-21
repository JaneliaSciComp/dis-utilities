# before running, put utility/bin in path like so:
# export PYTHONPATH="${PYTHONPATH}:/groups/scicompsoft/home/scarlettv/dis-utilities/utility/bin"

import os
import db_connect
import jrc_common.jrc_common as JRC

try:
    import name_match as nm
except:
    print('ERROR: Could not import name_match.py. Is it in your PYTHONPATH?')
    sys.exit(0)


db_connect.initialize_program()
LOGGER = JRC.setup_logging(db_connect.DummyArg())
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

dirname = 'orcid_not_on_paper'
doi = '10.1101/2024.09.16.613338'
doi_record = nm.doi_common.get_doi_record(doi, doi_collection)
all_authors = nm.get_author_objects(doi, doi_record, doi_collection)
guesses = [nm.propose_candidates(a) for a in all_authors]
# for sublist in guesses:
#     for guess in sublist:
#         if guess.exists:
#             print(guess.first_names)

if not os.path.exists(dirname):
    os.mkdir(dirname)

if not os.path.exists(f'{dirname}/config.txt'):
    with open(f'{dirname}/config.txt', 'w') as F:
        F.write(f"dirname:{dirname}")
        F.write(f"doi:{doi}")
        ids = []
        for sublist in guesses:
            for guess in sublist:
                if guess.exists:
                    ids.append(guess.id)
        F.write(f"initial_candidate_employee_ids:{','.join(ids)}")
        F.write(f"janelians:{[a.name for a in all_authors if a.check==True]}")
        

