import os
import csv
import zipfile
import gzip
import shutil
import json


class FileHandler:
    @staticmethod
    def unzip_files(file_list, folder_path):
        buffer_size = 2048
        down_files = []

        for file in file_list:
            file_type = file['name'].split('.')[-1].lower()
            file_path = os.path.join(folder_path, file['prefix'], file['name'])

            if file_type == "zip":
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(folder_path)
                    for file_info in zip_ref.infolist():
                        new_file_path = os.path.join(folder_path, file_info.filename)
                        down_files.append({'name': file_info.filename, 'local_absolute_path': new_file_path})

            elif file_type == "gz":
                new_file_path = os.path.join(folder_path, file['name'].rsplit('.', 1)[0])
                with gzip.open(file_path, 'rb') as f_in:
                    with open(new_file_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out, length=buffer_size)
                down_files.append({'name': file['name'].rsplit('.', 1)[0], 'local_absolute_path': new_file_path})

            else:
                raise Exception(f"Failed to unzip downloaded file: {file['name']} Unsupported archive type.")

        return down_files

    @staticmethod
    def merge_files(master_files, merged_path, merged_name, encoding='utf-8'):
        header_line = None
        out_file_path = os.path.join(merged_path, merged_name)

        if not os.path.exists(merged_path):
            os.makedirs(merged_path)

        with open(out_file_path, 'w', newline='', encoding=encoding) as out_file:
            writer = None

            for m_file in master_files:
                file_path = m_file['local_absolute_path']
                with open(file_path, 'r', encoding=encoding) as in_file:
                    reader = csv.reader(in_file)
                    header = next(reader)

                    if header_line is None:
                        header_line = header
                        writer = csv.writer(out_file)
                        writer.writerow(header_line)

                    for row in reader:
                        writer.writerow(row)

    @staticmethod
    def convert(source_path, dest_path, encoding='utf-8'):
        with open(source_path, 'r', encoding=encoding) as json_file:
            data = json.load(json_file)

        if not data:
            os.remove(dest_path)
            return False

        with open(dest_path, 'w', newline='', encoding=encoding) as csv_file:
            writer = csv.writer(csv_file)
            headers = data[0].keys()
            writer.writerow(headers)

            for row in data:
                writer.writerow(row.values())

        return True
