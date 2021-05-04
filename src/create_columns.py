import json
from typing import Dict, List


def set_path(path: str, path_dict: Dict[str, dict]) -> List[str]:
    if len(path_dict) != 0:
        res = []
        for subpath, value in path_dict.items():
            res.extend(set_path(path + "-" + subpath, value))
        return res
    else:
        return [path, ]


def get_short_name(fieldnames_file: str = 'data/column_names_utf_8'):

    with open(fieldnames_file) as file:
        data = json.load(file)

    all_columns = []

    all_columns.extend(set_path("", data['System']))
    all_columns.extend(set_path("", data['Data']))

    privilege_columns = set_path("", data['PrivilegeList'])
    privilege_columns = list(map(lambda x: x[5:], privilege_columns))
    all_columns.extend(privilege_columns)

    all_columns = list(map(lambda x: x[1:], all_columns))
    all_columns = list(map(lambda x: x[:-10] if x.endswith('-TextValue') else x, all_columns))

    return all_columns


def get_full_name(fieldname_file: str = 'data/column_names_utf_8'):
    with open(fieldname_file) as file:
        data = json.load(file)
    all_columns = set_path("", data)
    return all_columns
'''
for elem in data:
    for name in data[elem]:
        print(name)
'''

