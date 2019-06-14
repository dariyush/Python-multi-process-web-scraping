
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


# %%
def CollectARComCoList():
    options = Options()
    options.headless = True
    options.set_preference("security.sandbox.content.level", 5)
    browser = webdriver.Firefox(options=options)

    company_list = []

    for i in range(1, 10):
        annual_reports_webpage = 'http://www.annualreports.com/Companies?exch=%s' %i
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