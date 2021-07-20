'''
Module to convert xml to csv
'''
import xml.etree.ElementTree as ET
import os
import zipfile
import logging
import requests
import boto3
import pandas as pd


class Steeleye():
    '''
    Contains methods to download XML data fromgiven URL and then transform the same into custom CSV
    '''
    def __init__(self, init_url):
        self.xml_namespaces = {
                    'n1': 'urn:iso:std:iso:20022:tech:xsd:head.003.001.01',
                    'n2':'urn:iso:std:iso:20022:tech:xsd:auth.036.001.02'
                }
        self.elict = ['Id', 'FullNm', 'ClssfctnTp', 'NtnlCcy', 'CmmdtyDerivInd', 'Issr']
        self.cwd = os.getcwd()
        self.init_url = init_url
        self.xml_content = ''
        self.xml_zip_url = ''
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger()

    def load_initial_xml(self):
        '''
        Takes reference to current object as input and saves the loaded initial xml
        in an instance variable named xml_content
        '''
        try:
            resp = requests.get(self.init_url)
            self.xml_content = str(resp.content).replace('\\n', '')\
                                .replace("'","")\
                                .replace("b<","<")
            self.logger.info("Initial XML Loaded Successfully")
        except (requests.exceptions.RequestException, KeyError) as exception:
            self.logger.warning("Initial XML Failed To Load")
            self.logger.warning(str(exception))
            raise exception

    def extract_download_link(self):
        '''
        Takes reference to current object as input and saves the url of DLTINS zip
        in an instance variable named xml_zip_url
        '''
        try:
            response = ET.fromstring(self.xml_content)
            result = response.find('result')
            for doc in result:
                if doc.find("./str[@name='file_type']").text == 'DLTINS':
                    self.xml_zip_url = doc.find("./str[@name='download_link']").text
                    self.logger.info("DLTINS Download Link Extraction Successful")
                    break
            if not self.xml_zip_url:
                raise ET.ParseError("DLTINS Download Link Not Found")
        except ET.ParseError as parse_error:
            self.logger.warning(str(parse_error))
            raise parse_error

    def download_and_extract_zip(self):
        '''
        Takes reference to current object as input, downloads the DLTINS zip
        and extracts the downloaded zip in current working directory
        '''
        try:
            zip_stream = requests.get(self.xml_zip_url, stream=True)
            with open(os.path.join(self.cwd,'DLTINS.zip'), 'wb') as zip_file:
                for chunk in zip_stream.iter_content(chunk_size=128):
                    zip_file.write(chunk)
            with zipfile.ZipFile(os.path.join(self.cwd,'DLTINS.zip'), 'r') as zip_ref:
                zip_ref.extractall(self.cwd)
            self.logger.info("DLTINS Zip Downloaded and Extracted Successfully")
        except (zipfile.BadZipFile, zipfile.LargeZipFile) as exception:
            self.logger.warning(str(exception))
            raise exception

    def get_row(self, fin_instrm) -> dict:
        '''
        Takes reference to current object along with reference to fin_instrm element as input
        and returns the extracted data in the form of a dictionary
        '''
        edict = dict()
        try:
            for key_name in self.elict:
                key = 'FinInstrmGnlAttrbts.' + key_name
                node = ""
                if key_name == 'Issr':
                    search_str = './n2:TermntdRcrd/n2:' + key_name
                else:
                    search_str = './n2:TermntdRcrd/n2:FinInstrmGnlAttrbts/n2:' + key_name
                node = fin_instrm.find(search_str, self.xml_namespaces)
                if node is None:
                    continue
                else:
                    edict[key] = node.text
            if edict:
                self.logger.info("Row Extracted Successfully")
        except ET.ParseError as parse_error:
            self.logger.warning(str(parse_error))
            raise parse_error
        return edict

    def upload_to_s3(self):
        '''
        Takes reference to current object as input and uploads the CSV file to S3
        '''
        s3_resource = boto3.resource('s3')
        s3_resource.meta.client.upload_file(os.path.join(self.cwd,'steeleye.csv'), \
                                    'mybucket', \
                                    'steeleye.csv')

    def xml2csv(self):
        '''
        Takes reference to current object as input, then calls all other methods in a sequence
        '''
        try:
            self.load_initial_xml()
            self.extract_download_link()
            self.download_and_extract_zip()
            xml_dir = self.cwd
            xml_dir_files = os.listdir(xml_dir)
            xml_file  = ''
            for xml_dir_file in xml_dir_files:
                if xml_dir_file.endswith(".xml") and "DLTINS" in xml_dir_file:
                    xml_file = xml_dir_file
                    break
            xml_tree = ET.parse(xml_file)
            xml_root = xml_tree.getroot()
            search_str = './n1:Pyld/n2:Document/n2:FinInstrmRptgRefDataDltaRpt/n2:FinInstrm'
            row_list = [self.get_row(fin_instrm) \
                        for fin_instrm in xml_root.findall(search_str, self.xml_namespaces)]
            output_df = pd.DataFrame(row_list)
            output_df.to_csv(os.path.join(self.cwd,'steeleye.csv'), index = False, encoding="utf-8")
            self.logger.info("CSV Formed Successfully")
        except ET.ParseError as exception:
            self.logger.warning(str(exception))
            raise exception
        except (requests.exceptions.RequestException, KeyError) as exception:
            raise exception
        except (zipfile.BadZipFile, zipfile.LargeZipFile) as exception:
            raise exception
