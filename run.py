import csv
import Evtx.Evtx as evtx
import Evtx.Views as e_views
import xml.etree.ElementTree as ET
import json

def save_in_file(data):
    with open('log', 'a', encoding='windows-1251') as file:
        file.write(f"||{str(data)}\n")


column_names = {
    "System": {},
    "Data": {},
    "PrivilegeList": {}
}


def get_tag_name(tag: str):
    return tag.split("}")[-1]


def fill_column_names(column_categories: list[str]):
    tmp = column_names
    is_create = False
    for category in column_categories:
        if tmp.get(category) is None:
            is_create = True
            tmp[category] = {}
        tmp = tmp[category]
    if is_create:
        print(column_categories)
    """
    if column_names.get(column_category) is None:
        column_names[data_name] = data_value
        tmp = f"{data_name}: {data_value}"
        print(tmp)
        save_in_file(tmp)
    """


def fill_column_value(data_name, data_value):
    if column_names.get(data_name) is None:
        column_names[data_name] = data_value
        tmp = f"{data_name}: {data_value}"
        print(tmp)
        save_in_file(tmp)


def process_system_elem(tag_names: list[str], elem: ET.Element):
    if elem.text is not None:
        tmp_tag_names = tag_names.copy()
        tmp_tag_names.append("TextValue")
        fill_column_names(tmp_tag_names)
    if len(elem.attrib) > 0:
        for key, value in elem.attrib.items():
            tmp_tag_names = tag_names.copy()
            tmp_tag_names.append(key)
            fill_column_names(tmp_tag_names)


def process_data_elem(tag_names: list[str], elem: ET.Element):
    if elem.attrib.get('Name') is not None:
        tag_names[-1] = f"({elem.attrib.get('Name')})"
    if len(elem.attrib) > 0:
        for key, value in elem.attrib.items():
            if key == "Name":
                continue
            if key != "EnabledPrivilegeList":
                tmp_tag_name = tag_names.copy()
                tmp_tag_name.append(key)
                fill_column_names(tmp_tag_name)
    else:
        if tag_names[-1] != "EnabledPrivilegeList":
            tmp_tag_name = tag_names.copy()
            tmp_tag_name.append("TextValue")
            fill_column_names(tmp_tag_name)


def process_priv_elem(tag_names: list[str], elem: ET.Element):
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


def get_attr_values(xml_data: ET.Element):

    is_empty = True
    tag_names = [get_tag_name(xml_data.tag)]
    # print("tag:", tag_name)
    if tag_names[0] == "Data":
        if xml_data.attrib.get('Name') == "EnabledPrivilegeList":
            tag_names.insert(0, "PrivilegeList")
        else:
            tag_names.insert(0, "Data")

    else:
        tag_names.insert(0, "System")

    if xml_data.attrib.get('Name') is not None:
        if tag_names[0] == "Data":
            tag_names[-1] = f"({xml_data.attrib.get('Name')})"
        else:
            tag_names[-1] += f"_{xml_data.attrib.get('Name')}"

    if xml_data.text is not None:
        # print("    text:", xml_data.text)
        if xml_data.attrib.get('Name') != "EnabledPrivilegeList":
            fill_column_names(tag_names, xml_data.text)
        else:
            for row in xml_data.text.split('\n'):
                fill_column_names(tag_names.append(row.strip()), True)
        is_empty = False
    if len(xml_data.attrib) > 0:
        # print("    attribs:")
        is_empty = False
        for key, value in xml_data.attrib.items():
            if key == "Name":
                continue
            # fill_column_names(f"{tag_name}||{key}", value)
            if xml_data.attrib.get('Name') != "EnabledPrivilegeList":
                fill_column_names(f"{tag_name}||{key}", value)

            # print(f"        {key}: {value}")
    for elem in xml_data:
        get_attr(elem)


def calc(text_data):
    xml_root: ET.Element = ET.fromstring(text_data)
    get_attr(xml_root)


def read_evt_logs(func):
    with evtx.Evtx('data/Security.evtx') as log:
        print(e_views.XML_HEADER)
        for index, record in enumerate(log.records()):
            # if index > 0:
            #     break
            log_data = record.xml()
            func(log_data)
            if index % 1000 == 0:
                print(index)
    try:
        with open('column_names_utf_8', 'w', encoding='utf-8') as file:
            json.dump(column_names, file)
    except Exception as ex:
        print(ex)
    try:
        with open('column_names_windows-1251', 'w', encoding='windows-1251') as file:
            json.dump(column_names, file)
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    read_evt_logs(calc)
