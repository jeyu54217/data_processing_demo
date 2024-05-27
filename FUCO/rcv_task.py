
'''
The Receive Server reads the xml and xml.OK files from the specified paths under the Formatting and Image directories in the RCV machine. 
It then determines the order based on the xml file names and stores the result to be transmitted to the Pres-Workflow for reference. 
Here’s how it works:
    1. In the Formatting directory, the LIS reads the date folder names, 
       retrieves all files and folders under them, and if there's a .OK file, 
       it records this in the download configuration file (json).
    2. In the Formatting directory, the IMG reads the xml file names folder, 
       retrieves all files and folders under them, and gets the names of xml1 and xml2 files. 
       If there is a .OK file, it records this in the download configuration file (json).
In addition to retrieving the xml and xml.OK files, 
it also reads the "foot" under xml1 to get clause codes and compares them against the clause codes in the Clause directory. 
If there is a matching clause code folder, it retrieves the files under that folder, first getting the tiff files, then checks if the same tiff files have corresponding .lock files. If they do, the process stops.
'''


import json
import os
import time
import re
import csv
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime, date
from distutils.util import strtobool
from os.path import dirname, abspath
from socket import gethostbyaddr
from smb.SMBConnection import SMBConnection
from contrib import server_log, exc_get, img_order_sort



class ReceiveServer:
    def __init__(self):
        self.now = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.today = date.today().strftime("%Y%m%d")
        self.code = 0
        self.msg = ''
        self.dir = dirname(abspath(__file__))
        self.conf_dir = f'{self.dir}/rcv_conf.json'
        os.environ.setdefault('rcv_conf', self.conf_dir)
        self.load_config()
        self.connect_to_smb()
    
    def load_config(self):
        """Load configuration from the JSON file."""
        try:
            self.code, self.conf = contrib.conf_get(os.environ['rcv_conf'])
            self.lis_path = self.conf['LOCAL_LIS_PATH']
            self.img_path = self.conf['LOCAL_IMG_PATH']
            self.download_set_path = self.conf['download_set_path']
            self.lis_backup_local = self.conf['lis_backup_local']
            self.clause = self.conf['LOCAL_CLAUSE']
            self.pres_m_smb = self.conf['PRes_Master_SMB']
            self.email_path = self.conf['EMAIL_PATH']
            self.clause_get = self.conf['CLAUSE_GET']
            self.clause_download_path = self.conf['CLAUSE_DOWNLOAD_PATH']
            self.xml2_data = self.conf['xml2_data']
            self.backup_xml = self.conf['Backup_xml']
            self.backup_xml_path = self.conf['Backup_xml_path']
            self.load_balance = bool(strtobool(self.conf['LOAD_BALANCE']))
            self.img_today_ass = bool(strtobool(self.conf['IMG_TODAY_ASS']))
            self.tmp_fold = self.conf['TMP_FOLD']
            self.dst_data_path = self.pres_m_smb['dst_data_path']
            self.wk_data_download_path = self.pres_m_smb['wk_data_download_path']
            self.wk_order_code_check_path = self.pres_m_smb['wk_order_code_check_path']
            self.exec_inter = int(self.conf['Execute_interval'])
            self.log_path = self.conf['LOG_PATH']
        except:
            self.code, self.msg = 1, f'Execute receive server data assign failed: Please check {exc_get()}'

    def connect_to_smb(self):
        """Connect to the SMB server."""
        try:
            dst_name = gethostbyaddr(self.pres_m_smb['info']['ip'])
            self.dst_conn = SMBConnection(self.pres_m_smb['info']['username'], self.pres_m_smb['info']['password'], 
                                          self.pres_m_smb['info']['server_name'], self.pres_m_smb['info']['remote_name'])
            self.dst_conn.connect(self.pres_m_smb['info']['ip'], self.pres_m_smb['info']['port'], timeout=30)
            self.root_share_folder = self.dst_data_path.split('/')[1]
        except:
            self.code, self.msg = 1, f'Connect to SMB failed: Please check {exc_get()}'

    def clause_check_download(self, xml_file_names):
        """Check and download clause files."""
        xml2_data_list = []
        for filename, options in xml_file_names.items():
            if options['tag'] in ['xml2++', 'xml']:
                xml2_path = f'{self.xml2_data}/{options["root_folder"]}'
                os.makedirs(xml2_path, exist_ok=True)
                xml2_full_path = f'{xml2_path}/{filename}'
                xml2_data_list.append(xml2_full_path)
                src_path_split = options['src_path'].split('/')
                src_path_xml2 = f'{"/".join(src_path_split[2:])}/{filename}'
                with open(xml2_full_path, 'wb') as f_obj:
                    self.dst_conn.retrieveFile(src_path_split[1], src_path_xml2, f_obj)

        xml2_clause_match = {filename: options['root_folder'] for filename, options in xml_file_names.items() if options['tag'] in ['xml2', 'xml']}
        clause_list_match = {}
        img_tif_map = {}

        for filename_ in xml2_data_list:
            with open(filename_, encoding='utf-8') as f:
                try:
                    tree = ET.parse(f)
                    root = tree.getroot()
                    for elem in root.iter():
                        clause_list = []
                        if elem.tag in ['EndorsementClause', 'RiskCode']:
                            clause_list.append(elem.text)
                            clause_list = list(set([i for i in clause_list if i]))
                            clause_list_match.setdefault(filename_.split('_')[-2], []).extend(clause_list)
                        if elem.tag == 'ContNo':
                            n = os.path.basename(filename_)
                            cont_no = str(elem.text)
                            img_tif_map[f'{cont_no}.tif'] = {
                                "src_path": f'{xml_file_names[n]["src_path"]}/{cont_no}',
                                "root_folder": f'{n.split(".")[0]}.xml',
                                "tag": 'img'
                            }
                except ET.ParseError as e:
                    server_log(self.log_path).error(e)

        clause_all_folders = list(set([clause for path, claus_list in clause_list_match.items() for clause in claus_list]))

        clause_check_match = {}
        clause_folder_split = self.clause.split('/')
        for clause in clause_all_folders:
            clause_folders = [s.filename for s in self.dst_conn.listPath(clause_folder_split[1], '/'.join(clause_folder_split[2:]))]
            if clause in clause_folders:
                clause_full_folder = f'{"/".join(clause_folder_split[2:])}/{clause}'
                clause_files = self.dst_conn.listPath(clause_folder_split[1], clause_full_folder, timeout=30)
                if clause_files:
                    clause_check_match[clause_full_folder] = [filename.filename for filename in clause_files if filename.filename not in ['.', '..']]

        os.makedirs(self.clause_download_path, exist_ok=True)
        for tiff_path, tif_list in clause_check_match.items():
            for tif in tif_list:
                clause_path = f'{self.clause_download_path}/{tif}'
                remote_clause_tif = f'{tiff_path}/{tif}'
                with open(clause_path, 'wb') as tif_obj:
                    self.dst_conn.retrieveFile(clause_folder_split[1], remote_clause_tif, tif_obj)

        return img_tif_map

    def lis_files_check(self):
        """Check LIS files."""
        code = 0
        msg = ''
        today_lis_sharelist = []
        lis_filepath_match = {}

        try:
            for path in self.lis_path:
                path_split = path.split('/')
                lis_sharelist = self.dst_conn.listPath(path_split[1], '/'.join(path_split[2:]), timeout=30)
                for s_name in lis_sharelist:
                    if s_name.filename == self.today:
                        path_split.append(s_name.filename)
                        today_lis_sharelist += self.dst_conn.listPath(path_split[1], '/'.join(path_split[2:]), timeout=30)
                today_lis_sharelist = [f_name for f_name in today_lis_sharelist if f_name.filename not in ['.', '..']]
                for f_name in today_lis_sharelist:
                    f_name_split = f_name.filename.split('_')
                    if f_name_split[1] in path_split:
                        lis_filepath_match[f_name.filename] = f'{path}/{self.today}'
            if not today_lis_sharelist:
                server_log(self.log_path).info('There are no files in LIS path for today.')
            lis_filepath_match = {k: v for k, v in lis_filepath_match.items() if '.xml' in k.lower()}
        except:
            code, msg = 1, f'Execute receive server data assign failed: Please check {traceback.format_exc()}'
            server_log(self.log_path).error(msg)

        lis_okfile_match = {}
        if code == 0 and lis_filepath_match:
            try:
                ok_check_list = [k[:-3] for k in lis_filepath_match.keys() if '.ok' in k.lower()]
                ok_filenames = [k for k in lis_filepath_match.keys() if '.ok' in k.lower()]
                xml_check_list = [k for k in lis_filepath_match.keys() if k.lower().endswith('.xml')]
                ok_xml_list = [ok_filename for ok_filename in ok_check_list if ok_filename in xml_check_list]
                ok_OKfilenames = [file for file in ok_filenames if file[:-3] in ok_xml_list]

                for filename, path in lis_filepath_match.items():
                    tag = ''
                    root_folder = ''
                    if filename in ok_xml_list:
                        tag = 'xml'
                        root_folder = filename
                    elif filename in ok_OKfilenames:
                        tag = 'OK'
                        root_folder = filename[:-3]
                    lis_okfile_match[filename] = {'src_path': path, 'root_folder': root_folder, 'tag': tag}

                xml_data_list = []
                if bool(strtobool(self.backup_xml)):
                    for filename, options in lis_okfile_match.items():
                        os.makedirs(self.lis_backup_local, exist_ok=True)
                        xml_full_path = f'{self.lis_backup_local}/{filename}'
                        xml_data_list.append(xml_full_path)
                        src_path_split = options['src_path'].split('/')
                        src_path_xml = f'{"/".join(src_path_split[2:])}/{filename}'
                        with open(xml_full_path, 'wb') as d_obj:
                            self.dst_conn.retrieveFile(src_path_split[1], src_path_xml, d_obj)

                    for filename, options in lis_okfile_match.items():
                        local_backup = os.listdir(self.lis_backup_local)
                        dst_backup_fullpath = f'{self.backup_xml_path}/{filename}'
                        dst_backup_fullpath_split = dst_backup_fullpath.split('/')
                        if filename in local_backup:
                            local_backup_path = f'{self.lis_backup_local}/{filename}'
                            with open(local_backup_path, 'rb') as u_obj:
                                self.dst_conn.storeFile(dst_backup_fullpath_split[1], '/'.join(dst_backup_fullpath_split[2:]), u_obj)

                for filename, options in lis_okfile_match.items():
                    features = filename.split('_')
                    root = filename if options['tag'] == 'xml' else filename[:-3]
                    info = {
                        'DPC': features[1],
                        'PRY': features[4],
                        'DATE': self.today,
                        'root': root,
                        'PN': 'LIS'
                    }
                    lis_okfile_match[filename]['feature'] = info

                cond_list = []
                for filename, feature in lis_okfile_match.items():
                    cond_list = [v for k, v in feature['feature'].items() if k in ['PRY', 'DATE', 'DPC']]
                    cond_list[0], cond_list[1], cond_list[2] = cond_list[0], cond_list[2], cond_list[1]
                    random_code = filename.split('_')[-1] if feature['tag'] == 'xml' else filename.split('_')[-1][:-3]
                    cond_list.append(random_code)
                    cond_string = '_'.join(cond_list)
                    feature['feature']['order_pri'] = cond_string

                cond_string_list = list(set([feature['feature']['order_pri'] for feature in lis_okfile_match.values()]))
                cond = [[*cond_string.split('_')] for cond_string in cond_string_list]

                if cond:
                    code, msg = img_order_sort(cond)
                    if code == 0:
                        cond_info = msg
                        for feature in lis_okfile_match.values():
                            feature['feature']['order_pri'] = cond_info[feature['feature']['order_pri']]

                tmp_json = f'{self.now}LIS.json'
                json_path = f'{self.download_set_path}/{tmp_json}'
                new_LIS = {k: v for k, v in lis_okfile_match.items() if k and v['root_folder']}
                img_tif_map = self.clause_check_download(new_LIS)
                root_dir_in_smb = [f'{i.filename}.tif' for i in today_lis_sharelist]
                for k in img_tif_map:
                    if k in root_dir_in_smb:
                        new_LIS[k] = img_tif_map[k]
                lis_okfile_match = new_LIS

                with open(json_path, 'w') as json_file:
                    json.dump(lis_okfile_match, json_file)

                total_items_check = {}
                for k, v in lis_okfile_match.items():
                    key = v['root_folder'].replace('.xml', '').replace('.XML', '').replace('.OK', '')
                    if key not in total_items_check:
                        total_items_check[key] = {}
                    if v.get('tag') == 'OK':
                        total_items_check[key]['OK'] = True
                    if v.get('tag') == 'xml':
                        total_items_check[key]['xml'] = True

                for k, v in total_items_check.items():
                    v['PN'] = 'LIS'

                code, msg = 0, 'Execute receive server IMG data assign success'
            except:
                code, msg = 1, f'Condition sorting failed: Please check {traceback.format_exc()}'
                server_log(self.log_path).error(msg)

        return code, total_items_check

    def img_files_check(self):
        """Check IMG files."""
        code = 0
        msg = ''
        froot_folder = []
        img_allfiles_match = {}
        xml_folder_files_match = {}

        try:
            for path in self.img_path:
                path_split = path.split('/')
                img_sharelist = self.dst_conn.listPath(path_split[1], '/'.join(path_split[2:]), timeout=30)
                img_sharelist = [s_name.filename for s_name in img_sharelist if s_name.filename not in ['.', '..'] and '.' not in s_name.filename]
                xml_folder_match = {path: img_sharelist}

            for path, folder_list in xml_folder_match.items():
                for folder in folder_list:
                    temp_path = f'{path}/{folder}'
                    temp_path_split = temp_path.split('/')
                    folder_files = self.dst_conn.listPath(temp_path_split[1], '/'.join(temp_path_split[2:]), timeout=30)
                    folder_files = [folder.filename for folder in folder_files if folder.filename not in ['.', '..']]
                    xml_folder_files_match[temp_path] = folder_files

            for path, files_list in xml_folder_files_match.items():
                for filename in files_list:
                    img_files_match = {}
                    if any(ext in filename.split('.') for ext in ['xml', 'XML']):
                        img_files_match[filename] = {'src_path': path, 'root_folder': path.split('/')[-1]}
                        img_allfiles_match.update(img_files_match)
                    elif '.' not in filename:
                        img_folder_match = {filename: {'src_path': f'{path}/{filename}', 'root_folder': path.split('/')[-1], 'tag': 'folder'}}
                        img_allfolder_match.update(img_folder_match)

            for filename, options in img_allfiles_match.items():
                if filename.lower().endswith('.ok'):
                    options['tag'] = 'OK'
                elif filename.split('_')[-1][0] == '1':
                    options['tag'] = 'xml1'
                elif filename.split('_')[-1][0] == '2':
                    options['tag'] = 'xml2'
                else:
                    options['tag'] = ''

            xml1_files = [k for k, v in img_allfiles_match.items() if v['tag'] == 'xml1']
            xml_ok_files = [k[:-3] for k, v in img_allfiles_match.items() if v['tag'] == 'OK']
            xml1_files = [xml1 for xml1 in xml1_files if xml1 in xml_ok_files]
            xml2_files = [k for k, v in img_allfiles_match.items() if v['tag'] == 'xml2']
            xml2_files = [xml2 for xml2 in xml2_files if xml2 in xml_ok_files]
            xml2_txml1_files = [f'{ "_".join(xml2.split("_")[:-1])}_1{"".join(xml2.split("_")[-1][1:])}' for xml2 in xml2_files]
            fxml1_files = [xml1 for xml1 in xml1_files if xml1 in xml2_txml1_files]
            froot_folder = [v['root_folder'] for k, v in img_allfiles_match.items() if k in fxml1_files]
            fimg_files_match = {k: v for k, v in img_allfiles_match.items() if v['root_folder'] in froot_folder]

            if froot_folder:
                cond_string_list = []
                for filename, options in fimg_files_match.items():
                    filename_split = filename.split('_')
                    info = {
                        'DPC': filename_split[1],
                        'PRY': filename_split[4],
                        'DATE': self.today,
                        'root': filename,
                        'PN': 'IMG'
                    }
                    options['feature'] = info

                cond_list = []
                for filename, feature in fimg_files_match.items():
                    cond_list = [v for k, v in feature['feature'].items() if k in ['PRY', 'DATE', 'DPC']]
                    cond_list[0], cond_list[1], cond_list[2] = cond_list[0], cond_list[2], cond_list[1]
                    random_code = feature['root_folder'].split('_')[-1]
                    cond_list.append(random_code)
                    cond_string = '_'.join(cond_list)
                    feature['feature']['order_pri'] = cond_string
                    cond_string_list.append(cond_string)

                cond_string_list = list(set(cond_string_list))
                cond = [[*cond_string.split('_')] for cond_string in cond_string_list]

                if cond:
                    code, msg = img_order_sort(cond)
                    if code == 0:
                        cond_info = msg
                        for feature in fimg_files_match.values():
                            feature['feature']['order_pri'] = cond_info[feature['feature']['order_pri']]
                        img_allfiles_match.update(fimg_files_match)

            for filename, options in img_folder_match.items():
                img_imag_match = {}
                full_path = options['src_path']
                full_path_split = full_path.split('/')
                img_tif_files = self.dst_conn.listPath(full_path_split[1], '/'.join(full_path_split[2:]), timeout=30)
                for filename in img_tif_files:
                    if any(ext in filename.filename for ext in ['.tif', '.tiff']):
                        img_imag_match[filename.filename] = {'src_path': options['src_path'], 'root_folder': full_path_split[-2], 'tag': 'img'}
                img_allfiles_match.update(img_imag_match)

            os.makedirs(self.xml2_data, exist_ok=True)
            xml2_data_list = []
            for filename, options in fimg_files_match.items():
                if options['tag'] == 'xml2':
                    xml2_path = f'{self.xml2_data}/{options["root_folder"]}'
                    os.makedirs(xml2_path, exist_ok=True)
                    xml2_full_path = f'{xml2_path}/{filename}'
                    xml2_data_list.append(xml2_full_path)
                    src_path_split = options['src_path'].split('/')
                    src_path_xml2 = f'{"/".join(src_path_split[2:])}/{filename}'
                    with open(xml2_full_path, 'wb') as f_obj:
                        self.dst_conn.retrieveFile(src_path_split[1], src_path_xml2, f_obj)

            clause_list_match = {}
            for xml2 in xml2_data_list:
                with open(xml2, encoding='utf-8') as f:
                    try:
                        tree = ET.parse(f)
                        root = tree.getroot()
                        for elem in root.iter():
                            clause_list = []
                            if elem.tag in ['EndorsementClause', 'RiskCode']:
                                clause_list.append(elem.text)
                                clause_list = list(set([i for i in clause_list if i]))
                                clause_list_match.setdefault(xml2.split('_')[-2], []).extend(clause_list)
                    except ET.ParseError as e:
                        server_log(self.log_path).error(e)

            clause_all_folders = list(set([clause for path, claus_list in clause_list_match.items() for clause in claus_list]))
            clause_check_match = {}
            clause_folder_split = self.clause.split('/')
            for clause in clause_all_folders:
                clause_folders = [s.filename for s in self.dst_conn.listPath(clause_folder_split[1], '/'.join(clause_folder_split[2:]))]
                if clause in clause_folders:
                    clause_full_folder = f'{"/".join(clause_folder_split[2:])}/{clause}'
                    clause_files = self.dst_conn.listPath(clause_folder_split[1], clause_full_folder, timeout=30)
                    if clause_files:
                        clause_check_match[clause_full_folder] = [filename.filename for filename in clause_files if filename.filename not in ['.', '..']]

            os.makedirs(self.clause_download_path, exist_ok=True)
            for tiff_path, tif_list in clause_check_match.items():
                for tif in tif_list:
                    clause_path = f'{self.clause_download_path}/{tif}'
                    remote_clause_tif = f'{tiff_path}/{tif}'
                    with open(clause_path, 'wb') as tif_obj:
                        self.dst_conn.retrieveFile(clause_folder_split[1], remote_clause_tif, tif_obj)

            total_items_check = {}
            tmp_json = f'{self.now}IMG.json'
            json_path = f'{self.download_set_path}/{tmp_json}'
            new_IMG = {key: value for key, value in img_allfiles_match.items() if not (key.lower().endswith(".tif") and value['root_folder'] not in img_allfiles_match.keys())}

            with open(json_path, 'w') as json_file:
                json.dump(new_IMG, json_file)

            for k, v in new_IMG.items():
                key = v['root_folder'].replace('.xml', '').replace('.XML', '').replace('.OK', '')
                if key not in total_items_check:
                    total_items_check[key] = {}
                if v.get('tag') == 'OK':
                    total_items_check[key]['OK'] = True
                if v.get('tag') == 'img':
                    total_items_check[key]['img'] = True
                if v.get('tag') == 'xml1':
                    total_items_check[key]['xml1'] = True
                if v.get('tag') == 'xml2':
                    total_items_check[key]['xml2'] = True

            for k, v in total_items_check.items():
                v['PN'] = 'IMG'

            code, msg = 0, 'Execute receive server IMG data assign success'
        except:
            code, msg = 1, f'Execute receive server data assign failed: Please check {traceback.format_exc()}'
            server_log(self.log_path).error(msg)

        return code, total_items_check

    def run(self):
        """Main method to run the server."""
        complete_files = []
        uncomplete_files = []
        lis_code, lis_msg = self.lis_files_check()
        if lis_code == 0 and lis_msg:
            complete_files += [k for k, v in lis_msg.items() if v.get('xml') and v.get('OK')]
            uncomplete_files += [k for k, v in lis_msg.items() if not v.get('xml') or not v.get('OK')]
            success_files_items.update(lis_msg)

        time.sleep(2)
        img_code, img_msg = self.img_files_check()
        if img_code == 0 and img_msg:
            complete_files += [k for k, v in img_msg.items() if v.get('xml1') and v.get('xml2') and v.get('img') and v.get('OK')]
            uncomplete_files += [k for k, v in img_msg.items() if not v.get('xml1') or not v.get('xml2') or not v.get('OK') or not v.get('img')]
            success_files_items.update(img_msg)

        if success_files_items:
            server_log(self.log_path).info(success_files_items)
        else:
            server_log(self.log_path).info('There is no data in rcv Formatting.')

        complete_msg = 'complete:' + ','.join(complete_files) if complete_files else ''
        uncomplete_msg = 'uncomplete:' + ','.join(uncomplete_files) if uncomplete_files else ''

        group_com_filename = [{'group': m.group(0), 'content': root, 'type': 'com'} for root in complete_files if (m := re.match(r'\D{4,9}', root.split('_')[0]))]

        if group_com_filename:
            com_eml_path = f'{self.email_path}/{self.now}_com.csv'
            with open(com_eml_path, 'w', newline='\n') as f:
                writer = csv.DictWriter(f, fieldnames=['group', 'content', 'type'])
                writer.writerows(group_com_filename)

        if lis_code == 0 or img_code == 0:
            if lis_msg or img_msg:
                download_path = f'{self.wk_data_download_path}/data_download_start.OK'
                with open(download_path, 'w') as f:
                    f.write('""')

        order_code_path = f'{self.wk_order_code_check_path}/order_code_check.OK'
        with open(order_code_path, 'w') as d:
            d.write('""')

        time.sleep(self.exec_inter)


def main():
    server = ReceiveServer()
    server.run()


if __name__ == '__main__':
    main()