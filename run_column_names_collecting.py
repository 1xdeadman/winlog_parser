import json
import time
import click
import xml.etree.ElementTree as ET
from typing import List


from evtx import PyEvtxParser


column_names = {
    "System": {},
    "Data": {},
    "PrivilegeList": {}
}


def get_tag_name(tag: str):
    return tag.split("}")[-1]


def fill_column_names(column_categories: List[str]):
    tmp = column_names
    is_create = False
    for category in column_categories:
        if tmp.get(category) is None:
            is_create = True
            tmp[category] = {}
        tmp = tmp[category]
    if is_create:
        print(column_categories)


def process_system_elem(tag_names: List[str], elem: ET.Element):
    if elem.text is not None:
        tmp_tag_names = tag_names.copy()
        tmp_tag_names.append("TextValue")
        fill_column_names(tmp_tag_names)
    if len(elem.attrib) > 0:
        for key, value in elem.attrib.items():
            tmp_tag_names = tag_names.copy()
            tmp_tag_names.append(key)
            fill_column_names(tmp_tag_names)


def process_data_elem(tag_names: List[str], elem: ET.Element):
    if elem.attrib.get('Name') is not None:
        tag_names[-1] = f"{elem.attrib.get('Name')}"
    if len(elem.attrib) > 0:
        for key, value in elem.attrib.items():
            if key == "Name":
                continue
            if key != "EnabledPrivilegeList":
                tmp_tag_name = tag_names.copy()
                tmp_tag_name.append(key)
                fill_column_names(tmp_tag_name)
    if elem.text is not None and tag_names[-1] != "EnabledPrivilegeList":
        tmp_tag_name = tag_names.copy()
        tmp_tag_name.append("TextValue")
        fill_column_names(tmp_tag_name)


def process_priv_elem(tag_names: List[str], elem: ET.Element):
    for row in elem.text.split('\n'):
        tmp_tag_name = tag_names.copy()
        tmp_tag_name.append(row.strip())
        fill_column_names(tmp_tag_name)


def get_attr(xml_data: ET.Element):

    tag_names = [get_tag_name(xml_data.tag)]

    if tag_names[0] == "Data":
        if xml_data.attrib.get('Name') == "EnabledPrivilegeList":
            tag_names.insert(0, "PrivilegeList")
        else:
            tag_names.insert(0, "Data")
    else:
        tag_names.insert(0, "System")

    if tag_names[0] == "System":
        process_system_elem(tag_names, xml_data)
    elif tag_names[0] == "Data":
        process_data_elem(tag_names, xml_data)
    elif tag_names[0] == "PrivilegeList":
        process_priv_elem(tag_names, xml_data)

    for elem in xml_data:
        get_attr(elem)


def calc(text_data: str):
    xml_root: ET.Element = ET.fromstring(text_data)
    get_attr(xml_root)


def read_evt_logs(func, logs_file: str = 'data/Security.evtx', result_file: str = 'data/column_names_utf_8'):
    global_start = time.time()
    start = time.time()
    log = PyEvtxParser(logs_file)
    for index, record in enumerate(log.records()):
        # if index > 0:
        #     break
        log_data = record['data']
        func(log_data)
        if index % 10000 == 0:
            stop = time.time()
            print(f"{index}: {stop - start} сек.")
            start = time.time()
    try:
        with open(result_file, 'w', encoding='utf-8') as file:
            json.dump(column_names, file)
    except Exception as ex:
        print(ex)

    print(f"затраченно времени: {time.time() - global_start} сек.")


@click.command()
@click.option("--logs_file", '-l', help="Путь с именем до файла логов", required=True)
@click.option("--result_file", '-r', help="Путь с именем для сохранения файла зазоголовков", required=True)
def run(logs_file: str, result_file: str):
    read_evt_logs(calc, logs_file, result_file)


if __name__ == '__main__':
    run()
