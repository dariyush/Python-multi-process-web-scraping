#%%
# import azure.functions as func
# from azure.storage.blob import BlockBlobService

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pandas import DataFrame as DF

import datetime
from time import sleep
from tqdm import tqdm

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.propagate = False
fh = logging.FileHandler(filename = __file__ + '.log', mode = 'w')
fmt = '%(levelname)s ; %(asctime)s ; %(message)s'
frmt = logging.Formatter(fmt = fmt, datefmt = '%Y/%m/%d %I:%M:%S')
fh.setFormatter( frmt )
log.addHandler( fh )

debug = False
#%%
def CollectCompanies( br ):
    companyList = []
    for i in tqdm( range(1, 10) ):
        url = f"http://www.annualreports.com/Companies?exch={i}"
        log.info( url )

        br.get(url)

        try:
            tbody = WebDriverWait(br, 30).until(
                EC.presence_of_element_located( (By.XPATH, "//table/tbody") ) )
        except Exception as e:
            log.info( f"{url} ==> {e}" )

        tbody.get_attribute('innerHTML')
        for tr in tbody.find_elements_by_xpath(".//tr"):
            row = {}
            td = tr.find_elements_by_xpath(".//td")
            row['CompanyNameAr'] = td[0].text
            row['UrlAr'] = td[0].find_element_by_xpath(".//a").get_attribute('href')
            companyList.append( row )
        
        if debug:
            break

    return DF(companyList)

#%%
def CollectCompanyInfo( ):
    log.info('Collecting companies info!')

    options = webdriver.FirefoxOptions()
    options.headless = True
    options.set_preference("security.sandbox.content.level", 5)
    br = webdriver.Firefox(options=options)

    companies = CollectCompanies( br )

    for i in tqdm( companies.index ):
        log.info(companies.at[i,'UrlAr'])

        br.get( companies.at[i,'UrlAr'] )
        sleep(2)

        ### scrape company logo URL
        try:
            xpath = ".//div[starts-with(@class,'company-header')]/p/img"
            el = WebDriverWait(br, 30)\
                .until(EC.presence_of_element_located( (By.XPATH, xpath) ) )
            companies.at[i,'LogoURL'] = el.get_attribute('src')
            companies.at[i,'LogoAlt'] = el.get_attribute('alt')
        except Exception as e:
            print('!!! No logo', e)
            companies.at[i,'LogoURL'] = f"ERROR {str(e)}"
            companies.at[i,'LogoAlt'] = f"ERROR {str(e)}"

        ### scrape company info
        try:
            xpath = "//section[@class='preport-info']"
            company_info = WebDriverWait(br, 30)\
                .until(EC.presence_of_element_located( (By.XPATH, xpath) ) )
        except Exception as e:
            log.info(f"!!! No info for {companies.at[i,'UrlAr']} , Error: {e}")
            continue

        xpaths = {
        ".//div[@class='size-hq-table']/div[@class='col1']": ('Employees','text'),
        ".//div[@class='size-hq-table']/div[@class='col2']": ('HQLocation','text'),
        ".//div[@class='client-logo']/a": ('Website','href'),
        ".//p": ('Description','text'),

        ".//ul/li/a": ("SocialLinks", 'href'),
        ".//dl/dt": ("Property", 'text'),
        ".//dl/dd": ("PropertyValue", 'text'),
        }

        for xpath, (col, attr) in xpaths.items():

            if attr == 'text':
                for j, el in enumerate(company_info.find_elements_by_xpath(xpath) ):
                    try:
                        companies.at[i,col+f'_{j}'] = el.text
                    except Exception as e:
                        companies.at[i,col+f'_{j}'] = f"ERROR {str(e)}"
            else:
                for j, el in enumerate(company_info.find_elements_by_xpath(xpath) ):
                    try:
                        companies.at[i,col+f'_{j}'] = el.get_attribute(attr)
                    except Exception as e:
                        companies.at[i,col+f'_{j}'] = f"ERROR {str(e)}"

        if debug and i: 
            break

    br.quit()
    return companies
#%%
if __name__ == "__main__":
    companies = CollectCompanyInfo()

    now = datetime.datetime.now().date().isoformat()
    fName = f"companies{now}.csv"

    companies.to_csv(fName, index=False, encoding="utf-8-sig" )

    log.info( f"Done!" )

