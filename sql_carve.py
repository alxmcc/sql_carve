import re
import os
import mmap
import sqlite3
import subprocess

def sql_carve(input_file, output_dir, max_size):
    header = bytes.fromhex('53514c69746520666f726d6174203300')
    source_file = mmap.mmap(os.open(input_file, os.O_RDONLY), 0, access=mmap.ACCESS_READ)

    for match in re.finditer(re.escape(header), source_file):
        chunk = source_file[match.start():match.start()+max_size]
        database_size = db_size(chunk)

        if database_size >= max_size or database_size == 0:
            database_name = 'carved_' + str(match.start()) + '_' + str(match.start()+max_size) + '.db'
            write_carve(chunk[0:max_size], database_name, output_dir)
            print('{} - calculated size from header: {} - carving max size.'.format(database_name, database_size))

        else:
            database_name = 'carved_' + str(match.start()) + '_' + str(match.start()+database_size) + '.db'
            write_carve(chunk[0:database_size], database_name, output_dir)
            print('{} - calculated size from header: {}'.format(database_name, database_size))


def write_carve(data, filename, output_dir):
    carve_wrt = open(output_dir + "/" + filename, 'wb')
    carve_wrt.write(data)
    carve_wrt.close()


def db_size(database):
    page_size = int.from_bytes(database[16:18], byteorder='big')
    page_count = int.from_bytes(database[28:32], byteorder='big')
    return page_size * page_count


def tables_in_db(conn):
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [
            v[0] for v in cursor.fetchall()
            if v[0] != "sqlite_sequence"
        ]
        cursor.close()
    except:
        tables = "invalid"
    return tables


def dump_malformed_db(db_file, sqlite3_path, save_location):
    with open(save_location, 'a') as f:
        subprocess.call([sqlite3_path, db_file, '.dump'], stdout=f)
        f.write(str(db_file) + '\n')
        f.write('=' * 50 + '\n')
        f.write('\n\n')


def get_files(dir_name):
    list_of_files = os.listdir(dir_name)
    all_files = list()
    for entry in list_of_files:
        full_path = os.path.join(dir_name, entry)
        if os.path.isdir(full_path):
            all_files = all_files + get_files(full_path)
        else:
            all_files.append(full_path)
    return all_files


carve_dir = 'E:/unallocated'
max_db_size = 100000000
save_dir = 'E:/carved_db'
sqlite3_path = 'C:/Users/x/bin/sqlite3.exe'
db_results = 'E:/db_results.txt'
malformed_results = 'E:/malformed_results.txt'

files = get_files(carve_dir)
malformed_list = []
for file in files:
    sql_carve(file, save_dir, max_db_size)

carved_dbs = get_files(save_dir)
f = open(db_results, 'a')
for carved_db in carved_dbs:
    print('processing carved db: {}...'.format(carved_db))
    db = sqlite3.connect(carved_db)
    tables = tables_in_db(db)
    cursor = db.cursor()

    if type(tables) == list:
        f.write('=' * 120 + '\n')
        f.write(str(carved_db) + ',' + str(tables) + '\n\n')

        for table in tables:
            try:
                cursor.execute("SELECT * FROM {}".format(table))
                rows = cursor.fetchall()
                f.write('{} table rows: \n'.format(table))
                for row in rows:
                    f.write(str(row) + '\n')
            except:
                if carved_db not in malformed_list:
                    malformed_list.append(carved_db)
                    dump_malformed_db(carved_db, sqlite3_path, malformed_results)
                    f.write('{} table is malformed. Dumped to {}. \n'.format(table, malformed_results))
                else:
                    f.write('{} table is malformed. DB already dumped.'.format(table))
            f.write('\n\n')
    else:
        if carved_db not in malformed_list:
            malformed_list.append(carved_db)
            dump_malformed_db(carved_db, sqlite3_path, malformed_results)

    cursor.close()
f.close()
print('complete.')
