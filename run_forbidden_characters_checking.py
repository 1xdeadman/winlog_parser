import csv
from src import create_columns
import click
from pprint import pprint
import collections
import os
from time import time

tmp_file = "check_forbidden_characters.tmp"


def save_data(data):
    with open(tmp_file, 'a', encoding='utf-8') as file:
        file.write(f'{data}\n')


@click.command()
@click.option(
    "--fieldnames",
    '-f',
    default="data/column_names_utf-8",
    help="Путь с именем  до файла с именами колонок",
    required=True
)
@click.option(
    "--result_file",
    '-r',
    default='data/result.csv',
    help="Путь с именем для сохранения файла csv",
    required=True
)
@click.option(
    "--columns_file",
    '-c',
    default='data/check_forbidden_characters.log',
    help="Путь с именем для сохранения подсчитанных по колонкам запретных символов",
    required=True
)
def run(fieldnames: str, result_file: str, columns_file: str):
    full_start = time()
    column_names = create_columns.get_short_name(fieldnames)
    with open(result_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, column_names)
        start = time()
        for index, line in enumerate(reader):
            if '-' in line.values() or '`' in line.values():
                for key, value in line.items():
                    if value in ['-', '`']:
                        save_data(f"{key}: {value}")
                        # print(key, value)
            if index % 50000 == 0:
                print(f"Считано - {index} строк. Потрачено - {round(time() - start, 4)} sec")
                start = time()
    print(f'\n-- full time: {time() - full_start}')
    count_columns(columns_file)


def count_columns(columns_file: str = 'data/check_forbidden_characters.log'):
    with open(tmp_file, 'r', encoding='utf-8') as file:

        columns = collections.Counter(map(lambda x: x.strip()[:-3], file.readlines()))
        print()
        pprint(columns)
        print("len:", len(columns))
    os.remove(tmp_file)
    with open(columns_file, 'w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, list(columns.keys()))
        writer.writeheader()
        writer.writerow(dict(columns))


if __name__ == '__main__':
    run()
