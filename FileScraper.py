# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 09:31:16 2017
@author: a.mohammadi
"""
from multiprocessing import Pool
import pandas as pd, pyodbc, requests, re, hashlib, logging
from os.path import isfile
from os import SEEK_END, remove
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from urllib.parse import urlparse
from time import sleep
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

year_pattern = r'[1][9][9][0-9]|[2][0][0-2][0-9]'

log = logging.getLogger('ARS')
log.propagate = False
log.setLevel( logging.DEBUG )
fh = logging.FileHandler("_log_")
formatter = logging.Formatter('%(levelname)s _,_ %(asctime)s _,_ %(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)

def GetConnection(server, database):
    return pyodbc.connect( ''.join(
                [r'DRIVER={ODBC Driver 13 for SQL Server};',
                 r'Trusted_Connection=yes;',
                 r'SERVER=%s;' %server,
                 r'DATABASE=%s;' %database,]) )
# %%
def collect_ar_company_list():
    options = Options()
    options.headless = True
    options.set_preference("security.sandbox.content.level", 5)
    browser = webdriver.Firefox(options=options)

    company_list = []

    for i in range(1, 10):
        annual_reports_webpage = 'http://www.xxxxxxxxxxx.com/Companies?exch=%s' %i
        print(annual_reports_webpage)

        browser.get(annual_reports_webpage)

        try:
            tbody = WebDriverWait(browser, 30).until(
                EC.presence_of_element_located( (By.XPATH, "//table/tbody") ) )
        except Exception as e:
            print(annual_reports_webpage, e)

        tbody.get_attribute('innerHTML')

        for tr in tbody.find_elements_by_xpath(".//tr"):
            row = {}
            td = tr.find_elements_by_xpath(".//td")
            row['CompanyNameAr'] = td[0].text
            row['UrlAr'] = td[0].find_element_by_xpath(".//a").get_attribute('href')
            company_list.append( row )

    return pd.DataFrame(company_list)

#%%
def collect_ar_companies_info():
    print('Collecting companies info!')

    companies = collect_ar_company_list()

    options = Options()
    options.headless = True
    options.set_preference("security.sandbox.content.level", 5)
    browser = webdriver.Firefox(options=options)

    for i in companies.index:
        print(companies.at[i,'UrlAr'])

        browser.get( companies.at[i,'UrlAr'] )
        sleep(2)

        ### scrape company logo URL
        try:
            xpath = ".//div[starts-with(@class,'company-header')]/p/img"
            el = WebDriverWait(browser, 30)\
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
            company_info = WebDriverWait(browser, 30)\
                .until(EC.presence_of_element_located( (By.XPATH, xpath) ) )
        except Exception as e:
            print('!!! No info for', companies.at[i,'UrlAr'], e)
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

    browser.quit()
    return companies

#companies.to_csv('companies in xxxxxxxxx all info.csv')

#%%
def CollectCompanyFiles(company):
    files = []
    options = Options()
    options.headless = True
    options.set_preference("security.sandbox.content.level", 5)
    browser = webdriver.Firefox(options=options)

    try:
        browser.get( company['UrlAr'] )
        xpath = "//article[@class='report']"
        reports = WebDriverWait(browser, 30)\
                .until(EC.presence_of_element_located( (By.XPATH, xpath) ) )
    except:
        log.exception("message")
        browser.quit()
        return pd.DataFrame(files)

#### Scrape the most recent annual report
    xpath = ".//div[@class='content-holder']//a[@href]"
    for a in reports.find_elements_by_xpath(xpath):
        # a = reports.find_elements_by_xpath(xpath)[1]
        try:
            file = company.copy()
            file['URL'] = a.get_attribute('href')
            file['Title'] = reports.find_element_by_xpath(".//h3").text
            file = CollectFileInfo(file)
            if file['Error'] == '':
                files.append(file.copy())
                break
        except:
            log.exception("message")
        
  #### Scrape the rest of annual report      
    xpath = ".//div[@class='content-holder content-archive']//a[@href]"
    for a in reports.find_elements_by_xpath(xpath):
        try:
            # a = reports.find_elements_by_xpath(xpath)[0]
            file = company.copy()
            file['URL'] = a.get_attribute('href') 
            file['Title'] = a.get_attribute('title')
            file = CollectFileInfo( file )
            if file['Error'] == '':
                files.append(file.copy())
        except:
            log.exception("message")

    browser.quit()
    return pd.DataFrame(files)

#%%
def IsInKorcula(file):
    with GetConnection('ddddddd','SafeEC') as connection:
        query = f"""select top 1 [ID] from [KorculaFile]
                        where [MD5] = N'{file['Hash']}' """
        dummy = pd.read_sql(query, connection)
    if len(dummy):
        return True
    return False
        
#%%
def CollectFileInfo(file):
    file['Error'] = ''
    file['ConsumerType'] = 'C'
    file['CategoryID'] = 4
    file['Period'] = ( re.findall(year_pattern, file['Title']) or ['',] )[0]
    
    r = requests.get(file['URL'], stream=True, timeout=300)
    
    file['URL'] = r.url
    file['Name'] = file['URL'].split('/')[-1]
    file['Name'] = file['Name']+'.pdf' \
                    if not file['Name'].lower().endswith('.pdf') else file['Name']
    
    # Check if file['URL'] has already been downloaded correctly.
    with GetConnection('ffffffff','ggggggggg') as connection:
        query = f"""select top 1 [ID], [MD5] from [dbo].[KorculaFileRecommendation]
                    where [OriginalPath] = N'{file['URL']}' """
        dummy = pd.read_sql(query, connection).to_dict('records')
    if len(dummy): 
        file['ID'], file['Hash'] = dummy[0]['ID'], dummy[0]['MD5']
        file['InKorcula'] = IsInKorcula(file)
        file['Path'] = fr"\\ggggggggggg\{file['ID']}.pdf"
        file['AlreadyDownloaded'] = AlreadyDownloaded(file)
        if file['AlreadyDownloaded']:
            return file
        
    # Check the Hash and download
    md5hash = hashlib.md5()     
    for i, chunk in enumerate(r.iter_content(chunk_size=1024*1024)): #break
        if i==0:
            if not chunk.startswith(b'%PDF'):
                file['Error'] = 'Not a PDF file!'
                return file
            # Hash for first 1MB
            md5hash.update( chunk )
            file['Hash'] = md5hash.hexdigest()
            file['InKorcula'] = IsInKorcula(file)
            
            file =  GetKRStatus( file )
            file['AlreadyDownloaded'] = AlreadyDownloaded(file)
            if file['AlreadyDownloaded']:
                return file

        with open(file['Path'], 'ab') as fd:
            fd.write(chunk)
             
    return file
#%%
def AlreadyDownloaded(file):
    if isfile( file['Path'] ):
        with open(file['Path'], 'rb') as fh:
            if fh.read(100).startswith(b'%PDF'):
                fh.seek(-100, SEEK_END)
                if b'%%EOF' in fh.read():
                    return True
    
        remove( file['Path'] )
        
    return False
#%%
def GetKRStatus( file ):
    with GetConnection('ggggggggg','ddddddddd') as connection:
        query = f"""select top 1 [ID] from [dbo].[KorculaFileRecommendation]
                    where [MD5] = N'{file['Hash']}' """
        kfr_ids = pd.read_sql(query, connection).to_dict('records')
        
    if len(kfr_ids): 
        file['ID'] = kfr_ids[0]['ID']
    else:
        
        with GetConnection('ggggggg','dddddd') as connection:
            with connection.cursor() as cursor:
                file_name = re.sub(r'\'', '', file['Name'])
                file_title = re.sub(r'\'', '', file['Title'])
                file_url = re.sub(r'\'', '', file['URL'])[:850] #indexing limitation
                query = f"""SET NOCOUNT ON
                            insert into [dbo].[hhhhhhhhhhhhhh]
                            ([UpdateBy], [UpdateDate], [Type], [ConsumerID],
                            [CategoryID], [Period],[FileName] ,
                            [OriginalPath] ,[MD5], [Title],
                            [AIApproved], [Deleted], [MarketTicker])
                            values (
                            N'AI.ar', GETDATE(), N'{file['ConsumerType']}',
                            {file['CoID']}, {file['CategoryID']},
                            N'{file['Period']}', N'{file_name}',
                            N'{file_url}', N'{file['Hash']}', N'{file_title}',
                            0, 0, N'{file['MarketTicker']}')
                            SELECT cast( SCOPE_IDENTITY() as int) ID
                        """
                cursor.execute(query)
                file['ID'] = cursor.fetchall()[0][0]
                connection.commit()
        
    file['Path'] = fr"\\ttttttttttt\{file['ID']}.pdf"
    
    return file

#%%
def UpdateKR(df):
    if len(df) < 1:
        return 1

    with GetConnection('fffff','ggggggggg') as connection:
        with connection.cursor() as cursor:
            
            for f in df[(df['CoID']!=-1)
                        & (df['CategoryID']!=-1)
                        & (df['ID']!=-1) 
                        & (df['Period']!='')  
                        & (df['InKorcula']==0)
                        ].to_dict('records'):
                
                file_name = re.sub(r'\'', '', f['Name'])
                file_title = re.sub(r'\'', '', f['Title'])
                file_url = re.sub(r'\'', '', f['URL'])[:850]
                query = f"""update [dbo].[hhhhhhhhhh]
                            set [UpdateDate] = GETDATE(),
                            [Type] = N'{f['ConsumerType']}',
                            [ConsumerID]={f['CoID']},
                            [CategoryID] = {f['CategoryID']},
                            [Period] = N'{f['Period']}',
                            [FileName] = N'{file_name}',
                            [OriginalPath] = N'{file_url}',
                            [MD5] = N'{f['Hash']}',
                            [Title] = N'{file_title}',
                            [AIApproved] = 1, [Deleted] = 0,
                            [UpdateBy] = 'AI.ar',
                            [MarketTicker] = N'{f['MarketTicker']}'
                            where [ID] = {f['ID']} """
                            
                cursor.execute(query)
                connection.commit()

    return 1

#%%
def ProcessCompany(company):
        # company = companies.to_dict('records')[8] %%EOF\r\n
        # company = companies[companies['UrlAr']=='http://www.xxxxxxxxx.com/Company/bitauto'].to_dict('records')[0]
        # file = df_files.to_dict('records')[0]
        # import os; os.remove(r"\\hhhhhhhhhhh\4449009.pdf") 
    try:
        df_files = CollectCompanyFiles( company )
        UpdateKR(df_files)
    except:
        log.exception("message")  

    return 1

#%%
def multiprocessing(companies):

    p = Pool(processes=10)
    for results in tqdm( 
        p.imap_unordered(ProcessCompany, companies.to_dict('records'), 20), 
        total=len(companies)):
        pass

    print('All done!')

    return 1

#%%
def collect_companies():
    
#    companies = collect_ar_companies_info()
#    companies = pd.read_csv('companies in xxxxxxxxx all info 20170708.csv',
#                            encoding = "ISO-8859-1")
#    companies = companies.drop(['Unnamed: 0'], axis=1)#[:5]
    companies['MarketAr'] = companies['PropertyValue_1'].apply(lambda s: s.split()[0])
    companies['TickerAr'] = companies['PropertyValue_0']
    companies['CompanyUrlAr'] = companies['Website_0']

    companies['MarketTicker'] = companies['MarketAr'] + ':' + companies['TickerAr']
    companies['CoID'] = -1
    companies['CoNm'] = ''
#    companies.to_csv('companies in xxxxxxxxxx all info 20170814.csv', index=False)

    #### Match to tttttt companies
    query = """SELECT c.CoID, c.CoNm, c.Website, se.[ttttttttt], c.TickerCode1b Ticker
              FROM [SafeEc].[dbo].[Company] c
              left join [SafeEc].[dbo].[StockExchange] se
                  on c.TickerCode1 = se.ID
              where se.[tttttttt] is not null and c.TickerCode1b <> ''
        union
        SELECT c.CoID, c.CoNm, c.Website, se.[ttttttttttttt], c.TickerCode2b Ticker
            FROM [SafeEc].[dbo].[Company] c
            left join [SafeEc].[dbo].[StockExchange] se
            on c.TickerCode2 = se.ID
            where se.[ttttttt] is not null and c.TickerCode2b <> ''
        union
        SELECT c.CoID, c.CoNm, c.Website, se.[tttttttttttt], c.TickerCode3b Ticker
            FROM [SafeEc].[dbo].[Company] c
            left join [SafeEc].[dbo].[StockExchange] se
            on c.TickerCode3 = se.ID
            where se.[tttttttttt] is not null and c.TickerCode3b <> ''"""

    with GetConnection('ttttttt','tttttttt') as connection:
        companies_info = pd.read_sql(query, connection)
    companies_info['MarketTicker'] = companies_info['xxxxxx'] + ':' + companies_info['Ticker']

    for i in companies_info.index:
        company_website = companies_info.at[i, 'Website']
        try:
            company_domain = urlparse(company_website).netloc.split('www.')[::-1][0]
            companies_info.at[i,'Domain'] = company_domain
        except Exception as e:
            print(i, e)
            continue

    for i in companies.index:
        company_website = companies.at[i,'CompanyUrlAr']
        try:
            company_domain = urlparse(company_website).netloc.split('www.')[::-1][0]
            companies.at[i,'CompanyDomainAr'] = company_domain
        except Exception as e:
            print(i, e)
            continue

    for i in companies.index:
        market_ticker = companies.at[i,'MarketTicker']
        matches = companies_info[companies_info['MarketTicker'] == market_ticker]

        if not len(matches):
            company_domain = companies.at[i,'CompanyDomainAr']
            matches = companies_info[companies_info['Domain'] == company_domain]
            if not len(matches):
                continue

        companies.at[i,'CoID'] = matches['CoID'].iloc[0]
        companies.at[i,'CoNm'] = matches['CoNm'].iloc[0]

    return companies[companies['CoID']!=-1].reset_index(drop=True)

#columns = ['CompanyNameAr', 'UrlAr', 'CoID', 'CoNm', 'MarketTicker']
#companies[columns].to_csv('company info for mp file scraping.csv', index = False)


#%%
if __name__ == "__main__":
##################
    companies = pd.read_csv('company info for mp file scraping 20190306.csv').fillna('')
    multiprocessing(companies)
    
##################
    
#    companies = collect_companies()
#    company = companies.to_dict('records')[0]

################
#    for company in companies.to_dict('records'):
#        if company['CompanyNameAr'] == 'A.M. Castle & Co.':
#            print(company)
#            break
#    df_links = collect_company_file_links(company)
#    df_hashed = set_hash(df_links, company)
#    df_fid = set_file_id(df_hashed, company)
#    update_db(df_fid, company)
