import steeleye
import pandas as pd

url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'
test_zip_url = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
s = steeleye.Steeleye(url)

def initial_xml_loader_test():
    with open('test.xml', 'r') as f:
        test_xml = f.read()
    s.load_initial_xml()
    assert test_xml == s.xml_content, "Load Initial XML test failed"

def dltins_url_extractor_test():
    s.extract_download_link()
    assert test_zip_url == s.xml_zip_url, "Extract DLTINS Zip URL test failed"

def xml2csv_transformer_test():
    s.xml2csv()
    with open('steeleye_test.csv', 'r', encoding ='utf8') as f:
        csv_test = f.read()
    with open('steeleye.csv', 'r', encoding ='utf8') as f:
        csv_new = f.read()
    assert csv_test == csv_new, "Transform XML To CSV test failed"

if __name__ == "__main__":
    initial_xml_loader_test()
    dltins_url_extractor_test()
    xml2csv_transformer_test()
    print("Everything passed")