import re

tests = [
    "Janelia Research Campus, Ashburn VA",  # want True
    "2Janelia",  # want True
    "Janelia",  # want True
    "The Janelia Farm",  # want True
    " janelia, ",  # want True
    "thejaneliafarm",  # want True
    "Howard Hughes Medical Institute, Ashburn",  # want True
    "1HHMI, Ashburn, VA",  # want True
    "The Howard Hughes, Ashburn",  # want True
    "howardhughesmedicalinstitute, ashburnva",  # want True
    "Howard Hughes MedicalInstitute, Ashburn",  # want True
    "The Howard Hughes Medical Institute",  # want False
    "HHMI",  # want False
    "Howard Hughes Medical Institute, Seattle, WA",  # want False
    "Janeli",  # want False
]

ans = [True] * 11 + [False] * 4


def is_janelian(affilstr):
    result = False
    pattern = re.compile(
        r"(?i)(janelia|"  # (?i) means case-insensitive; pattern matches "Janelia" in any form, e.g., "Janelia", "thejaneliafarm", etc.
        r"(ashburn.*(hhmi|howard\s*hughes))|"  # "Ashburn" with "HHMI" or "Howard Hughes"
        r"(hhmi|howard\s*hughes).*ashburn)"  # "HHMI" or "Howard Hughes" with "Ashburn"
    )
    if bool(re.search(pattern, affilstr)):
        result = True
    return result


res = [is_janelian(t) for t in tests]

if res == ans:
    print("Pass")
else:
    print("Fail")
    print(f"Result of regex:\n{res}")
