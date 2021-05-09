import csv
import time
import os
import xml.etree.ElementTree as ET
from typing import List, Dict


from evtx import PyEvtxParser


IS_DEBUG = False


def set_column_value(column_categories: List[str], value, _row_values: Dict[str, dict]):
    tmp = ""
    if type(value) is str:
        value = value.strip()
    if value in ['', '-']:
        value = 0
    if value is False:
        value = 0
    if value is True:
        value = 1
    for category in column_categories:
        tmp += "-" + category
    if column_categories[0] == "PrivilegeList":
        tmp = tmp[len("-PrivilegeList"):]
        tmp = tmp[5:]
    if column_categories[0] == "System":
        tmp = tmp[len("-System"):]
    if column_categories[0] == "Data":
        tmp = tmp[len("-Data"):]

    if tmp.endswith("-TextValue"):
        tmp = tmp[:-10]
    tmp = tmp[1:]
    # print(tmp)
    assert _row_values.get(tmp) is not None
    _row_values[tmp] = value


def _get_tag_name(tag: str):
    return tag.split("}")[-1]


def _process_system_elem(tag_names: List[str], elem: ET.Element, _row_values):
    if elem.text is not None:
        tmp_tag_names = tag_names.copy()
        tmp_tag_names.append("TextValue")
        set_column_value(tmp_tag_names, elem.text, _row_values)
    if len(elem.attrib) > 0:
        for key, value in elem.attrib.items():
            tmp_tag_names = tag_names.copy()
            tmp_tag_names.append(key)
            set_column_value(tmp_tag_names, value, _row_values)


def _process_data_elem(tag_names: List[str], elem: ET.Element, _row_values):
    if elem.attrib.get('Name') is not None:
        tag_names[-1] = f"{elem.attrib.get('Name')}"
    if len(elem.attrib) > 0:
        for key, value in elem.attrib.items():
            if key == "Name":
                continue
            if key != "EnabledPrivilegeList":
                tmp_tag_name = tag_names.copy()
                tmp_tag_name.append(key)
                set_column_value(tmp_tag_name, value, _row_values)
    if elem.text is not None and tag_names[-1] != "EnabledPrivilegeList":
        tmp_tag_name = tag_names.copy()
        tmp_tag_name.append("TextValue")
        set_column_value(tmp_tag_name, elem.text, _row_values)


def _process_priv_elem(tag_names: List[str], elem: ET.Element, _row_values):
    for row in elem.text.split('\n'):
        tmp_tag_name = tag_names.copy()
        tmp_tag_name.append(row.strip())
        set_column_value(tmp_tag_name, 1, _row_values)


def _get_attr(xml_data: ET.Element, _row_values):

    tag_names = [_get_tag_name(xml_data.tag)]

    if tag_names[0] == "Data":
        if xml_data.attrib.get('Name') == "EnabledPrivilegeList":
            tag_names.insert(0, "PrivilegeList")
        else:
            tag_names.insert(0, "Data")
    else:
        tag_names.insert(0, "System")

    if tag_names[0] == "System":
        _process_system_elem(tag_names, xml_data, _row_values)
    elif tag_names[0] == "Data":
        _process_data_elem(tag_names, xml_data, _row_values)
    elif tag_names[0] == "PrivilegeList":
        _process_priv_elem(tag_names, xml_data, _row_values)

    for elem in xml_data:
        _get_attr(elem, _row_values)


def _calc(text_data, _row_values):
    xml_root: ET.Element = ET.fromstring(text_data)
    _get_attr(xml_root, _row_values)


def _read_evt_logs(func, logs_file: str, result_file: str, column_names: List[str]):
    global_start = time.time()
    start = time.time()
    _row_values = dict()
    if os.path.isfile(result_file):
        os.remove(result_file)
    with open(result_file, 'w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, column_names)
        writer.writeheader()

    log = PyEvtxParser(logs_file)
    for index, record in enumerate(log.records()):
        _row_values.clear()
        _row_values = dict.fromkeys(column_names, 0)
        log_data = record['data']

        func(log_data, _row_values)

        if IS_DEBUG:
            if index == 1000:
                break

        with open(result_file, 'a', encoding='utf-8', newline='') as file:
            writer = csv.DictWriter(file, column_names)
            writer.writerow(_row_values)
        if index % 10000 == 0:
            stop = time.time()
            print(f"{index}: {stop - start} сек.")
            start = time.time()
    print(f"затраченно времени: {time.time() - global_start} сек.")


def run(logs_file: str, result_file: str, column_names: List[str]):
    _read_evt_logs(func=_calc, logs_file=logs_file, result_file=result_file, column_names=column_names)
