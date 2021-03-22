from src import collect_data, create_columns
import click


@click.command()
@click.option("--fieldnames", '-f', help="Путь с именем  до файла с именами колонок", required=True)
@click.option("--logs_file", '-l', help="Путь с именем  до файла логов", required=True)
@click.option("--result_file", '-r', help="Путь с именем для сохранения файла csv", required=True)
def run(fieldnames: str, logs_file: str, result_file: str):
    column_names = create_columns.get_short_name(fieldnames)
    collect_data.run(logs_file, result_file, column_names)
    #pprint(column_names)
    # print("len:", len(column_names))


if __name__ == '__main__':
    run()