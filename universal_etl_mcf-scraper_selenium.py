"""
project: MyCareerFuture scraper
version: 1.2.2
runtime env: aws docker container (does not requires secret.py, S3 access authentication would be handle by IAM
description: this is a multi-thread scraper of mycareerfuture that would be deploy in aws and its' save output as csv into aws S3 bucket
notes: this script will not work outside of AWS and docker container
"""

#   ######  ##  ##  #####    ####   #####   ######
#     ##    # ## #  ##  ##  ##  ##  ##  ##    ##  
#     ##    # ## #  #####   ##  ##  #####     ##  
#     ##    #    #  ##      ##  ##  ## ##     ##  
#   ######  #    #  ##       ####   ##  ##    ##  

from selenium import webdriver
import pandas as pd
import numpy as np
import time
import datetime
import concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm

#   #####     ##     ####   ######       #####   ####   #####   ######
#   ##  ##   #  #   ##   #  ##          ##      ##  ##  ##  ##  ##
#   #####   ######    ##    #####       ##      ##  ##  ##  ##  #####
#   ##  ##  ##  ##  #   ##  ##          ##      ##  ##  ##  ##  ##
#   #####   ##  ##   ####   ######       #####   ####   #####   ######

class base_code:
    def __init__():
        pass

    def collect_data(driver: webdriver, website: str) -> pd.DataFrame:
        """
        This function would scrape the website via BeautifulSoup and output it in a DataFrame format.
        BeautifulSoup extraction of information from the website is much faster then using webdriver approach via, css, class, or xpath scrapping.
        """
        html = driver.page_source

        soup = BeautifulSoup(html, 'html.parser')

        jobtitle = str(soup.h1.string)

        jobid = str(soup.select('span.black-60.db.f6.fw4.mv1')[0].string)

        if soup.find_all("div", {"class": "salary tr-l"})[0].text != 'Salary undisclosed':
            salary = soup.find_all("div", {"class": "salary tr-l"})[0].text.replace('$','').replace(',','').replace('Monthly','').split('to')
            minsalary = salary[0]
            maxsalary = salary[1]
        else:
            minsalary = ''
            maxsalary = ''

        company = str(soup.find_all('p')[1].text)

        try:
            companyadd = str(soup.find_all('p', id='address')[0].text)
        except:
            companyadd = ''

        employtype = str(soup.find_all('p', id='employment_type')[0].text)

        seniority = str(soup.find_all('p', id='seniority')[0].text)

        try:
            experience = str(soup.find_all('p', id='min_experience')[0].text.split(' ')[0])
        except:
            experience = ''

        jobcategories = str(soup.find_all('p', id='job-categories')[0].text)

        posteddate = str(soup.find_all('span', id='last_posted_date')[0].text.replace('Posted ',''))

        expirydate = str(soup.find_all('span', id='expiry_date')[0].text.replace('Closing on ',''))

        jobdescription = str(soup.find_all('div',id='description-content')[0].text)
        #jobdescription = []
        #for each in soup.find_all('div',id='description-content'):
            #jobdescription.append(each.text)

        skills = list()
        for each in soup.find_all('label'):
            if each.text.lower() == '':
                pass
            elif each.text.lower() =='read more':
                pass
            elif each.text.lower() == 'show more':
                pass
            else:
                skills.append(str(each.text.lower()))
        
        currenturl = website
        
        website_list = [[jobid,company,companyadd,jobtitle,minsalary,maxsalary,employtype,seniority,experience,jobcategories,jobdescription,skills,posteddate,expirydate,currenturl]]
        website_df = pd.DataFrame(website_list,columns=['Job_ID','Company','Location','Job_Title','Minimum_Salary','Maximum_Salary','Employment_Type','Seniority','Minimum_Experiences','Job_Categories','Description','Skills','Last_Update_Date','Expiry_Date','Website_Link'])
        
        soup.decompose()
        
        return website_df

    def get_max_search() -> list:
        """
        This function is use to generate the list of number of new job page, and the return list would be use in threadpoolexecutor
        """
        driver = thread_setup.setup_driver_headless()
        try:
            driver.get('https://www.mycareersfuture.gov.sg/search?sortBy=new_posting_date&page=0')
            time.sleep(2)
            web_totalsearch = driver.find_elements_by_css_selector('span#search-result-headers.pl2.pl0-ns.f5.black-80.fw4.db.lh-copy')
            web_search_number = int(web_totalsearch[0].text.split(" ")[0].replace(",",""))
            web_total_page = web_search_number//20
        except:
            web_total_page = 0

        web_total_page_list = []
        for each in range(web_total_page):
            web_total_page_list.append(each)
        
        return web_total_page_list

    def get_new_jobs_df(input_df: pd.DataFrame) -> pd.DataFrame:
        """
        This function is to output a dataframe that holds new unique website link that is needed to be scrape.
        """
        scrapped_website_df = s3_bucket.s3_load_website_link()
        #scrapped_website_df = pd.read_csv('mycareersfuture_scrapped_website_link.csv')
        combined_website_df = pd.merge(input_df,  
                            scrapped_website_df,  
                            on ='Website_Link',
                            how ='left')

        filtered_website_df = combined_website_df.loc[combined_website_df['Job_ID'].isnull()]
        output_df = filtered_website_df[['Website_Link']].copy()
        output_df.reset_index(drop=True, inplace=True)
        #df_output = df_filtered.dropna(axis=1, how='any', thresh=None, subset=None, inplace=False)
        print(f'Found {output_df.shape[0]} new jobs that was not in scrapped database')
        return output_df

#   ######  ##  ##  #####   ######    ##    #####        ####   ######  ######  ##  ##  #####
#     ##    ##  ##  ##  ##  ##       #  #   ##  ##      ##   #  ##        ##    ##  ##  ##  ##
#     ##    ######  #####   #####   ######  ##  ##        ##    #####     ##    ##  ##  #####
#     ##    ##  ##  ## ##   ##      ##  ##  ##  ##      #   ##  ##        ##    ##  ##  ##
#     ##    ##  ##  ##  ##  ######  ##  ##  #####        ####   ######    ##     ####   ##

class thread_setup:
    def __init__():
        pass

    def setup_driver_headless() -> webdriver:
        """
        This function is use as the base for all webdriver request
        """
        # Load up chrome driver that would be use to scrap dynamic data
        #path = r'/usr/local/bin/Chromedriver.exe'
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument("--log-level=3")
        options.add_argument("disable-infobars"); # disabling infobars
        options.add_argument("--disable-extensions"); # disabling extensions
        options.add_argument("--disable-gpu"); # applicable to windows os only
        options.add_argument("--disable-dev-shm-usage"); # overcome limited resource problems
        options.add_argument("--no-sandbox"); # Bypass OS security model
        #driver = webdriver.Chrome(executable_path=path, options=options)
        driver = webdriver.Chrome(options=options)
        # Setup the basic wait time within the driver to 5 seconds
        driver.implicitly_wait(5)
        return driver

    def setup_threaded_workers_search(main_list: list) -> pd.DataFrame:
        """
        Multi-thread function to collect the jobs website link.
        The collected link would be use in subsequent function 'setup_threaded_workers_scraper', which would use the website link to scrap the data.
        """
        #threaded_df = pd.DataFrame
        partitions = 4
        splitted_list = np.array_split(main_list,partitions)
        drivers = [thread_setup.setup_driver_headless() for _ in range(partitions)]
        with concurrent.futures.ThreadPoolExecutor(max_workers= partitions) as executor:        
            threaded_df = executor.map(thread_setup.get_handles_new_link, splitted_list, drivers)
        
        [driver.quit() for driver in drivers]
        
        output_df = thread_setup.threaded_df_output(threaded_df)

        return output_df

    def get_handles_new_link(splitted_list: list, driver: webdriver) -> pd.DataFrame:
        """
        The function that would run in multi-threaded function 'setup_threaded_workers_search'
        """
        sitemaplink = list()

        #for each_page in splitted_list:
        for each_page in tqdm(splitted_list):
            try:
                #print(f'Processing Page: {each_page} of {(splitted_list[-1])}')
                #start = time.perf_counter()
                driver.get('https://www.mycareersfuture.gov.sg/search?sortBy=new_posting_date&page='+str(each_page))
                #if repeats >= 5000:
                #    break
                sitemapbuilder = driver.find_elements_by_css_selector('a.bg-white.mb3.w-100.dib.v-top.pa3.no-underline.flex-ns.flex-wrap.JobCard__card___22xP3')
                for each in sitemapbuilder:
                    each_link_raw = each.get_attribute('href')
                    each_link_adj = each_link_raw.replace('?source=MCF&event=Search','')
                    sitemaplink.append(each_link_adj)
                #    if df_exist_website['Website_Link'].str.contains(each.get_attribute('href')).any():
                #        repeats += 1
            except:
                pass
        
        #print(f'This thread had generated a total of {len(sitemaplink)} links')
        df_output = pd.DataFrame(list(zip(sitemaplink)),columns=['Website_Link'])
        
        return df_output

    def setup_threaded_workers_scraper(main_df: pd.DataFrame) -> pd.DataFrame:
        """
        Multi-thread function to scape the data from the website.
        """
        output_df=pd.DataFrame()
        partitions = 4
        splitted_df = np.array_split(main_df,partitions)
        drivers = [thread_setup.setup_driver_headless() for _ in range(partitions)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=partitions) as executor:
            threaded_df = executor.map(thread_setup.get_handles_scraper,splitted_df,drivers)
        
        [driver.quit() for driver in drivers]
        
        output_df = thread_setup.threaded_df_output(threaded_df)

        return output_df

    def get_handles_scraper(splitted_df: pd.DataFrame, driver: webdriver):
        """
        The function that would run in multi-threaded function 'setup_threaded_workers_scraper'
        """
        #splitted_df.reset_index(inplace=True)
        df_output = pd.DataFrame()
        #for index, row in splitted_df.iterrows():
        for index, row in tqdm(splitted_df.iterrows(), total=splitted_df.shape[0]):
            try:
                #print(f'Scrapping Job: {index} of {(splitted_df.index[-1])}')
                driver.get(row['Website_Link'])
                time.sleep(3)
                while driver.find_elements_by_css_selector('span.black-60.db.f6.fw4.mv1') == []:
                    driver.get(row['Website_Link'])
                    time.sleep(3)

                df_temp = base_code.collect_data(driver, row['Website_Link'])
                frames = [df_output, df_temp]
                df_output = pd.concat(frames)
            except:
                pass

        return df_output

    def threaded_df_output(input_gen):
        """
        This function is use to convert the generator output from the multi-threaded function into a single Dataframe.
        """
        output_df = pd.DataFrame()
        for each_gen_df in input_gen:
            frames = [output_df, each_gen_df]
            #output_df = pd.concat(frames)
            output_df = pd.concat(
                        frames,
                        axis=0,
                        join="outer",
                        ignore_index=True,
                        keys=None,
                        levels=None,
                        names=None,
                        verify_integrity=False,
                        copy=True,
                    )
        return output_df

#    ####   #####       #####   ##  ##   #####  ##  ##  ######  ######
#   ##   #      ##      ##  ##  ##  ##  ##      ## ##   ##        ##  
#    ####   #####       #####   ##  ##  ##      ####    #####     ##  
#   #   ##      ##      ##  ##  ##  ##  ##      ## ##   ##        ##  
#    ####   #####       #####    ####    #####  ##  ##  ######    ##  

class s3_bucket:
    def __init__():
        pass

    def load_configuration():
        # setting the S3 bucket name
        aws_s3_bucket = 'mycareerfuture-scraper'
        # setting the S3 object name / file name
        key = f'mycareersfuture_config.txt'

        # try to check if S3 have existing file with the same key naming convention, if exist load the csv and concat it with input_df before saving back to S3
        # if except FileNotFoundError is raise, input_df would be save as per key naming convention.
        try:
            # reading of mycareersfuture_config.txt directly from s3, achieved via usage of s3fs
            pass
        except FileNotFoundError:
            pass

    def s3_save_instance_df(input_df: pd.DataFrame):
        """
        aws s3 saving of scrapped data base on individual run time
        this approach would create single csv file every day
        """
        # setting the filler for the naming convention
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        # setting the S3 bucket name
        aws_s3_bucket = 'mycareerfuture-scraper'
        # setting the S3 object name / file name
        key = f'mycareersfuture_scrapped_multi_{month}_{day}_{year}.csv'

        # try to check if S3 have existing file with the same key naming convention, if so load the csv and concat it with input_df before saving back to S3
        # if except FileNotFoundError is raise, it would save as per key naming convention.
        try:
            # reading of csv directly from s3 via pandas, achieved via usage of s3fs
            exist_df = pd.read_csv(
                f"s3://{aws_s3_bucket}/{key}",
            )

            # Setting the list of dataframe
            frames = [exist_df, input_df]
            # Combining the website_new and website_old dataframe into one dataframe 
            input_df = pd.concat(
                                frames,
                                axis=0,
                                join="outer",
                                ignore_index=True,
                                keys=None,
                                levels=None,
                                names=None,
                                verify_integrity=False,
                                copy=True,
                            )
        except FileNotFoundError:
            print('File Not Found, Saving new file into S3')
        
        # uploading of csv directly to s3 via pandas, achieved via usage of s3fs
        input_df.to_csv(
            f"s3://{aws_s3_bucket}/{key}",
        )

    def s3_load_website_link() -> pd.DataFrame:
        """
        aws s3 loading of website link csv file in the s3 bucket
        """
        # importing the full scrapped data from s3 bucket
        # setting the S3 bucket name
        aws_s3_bucket = 'mycareerfuture-scraper'
        # setting the S3 object name / file name
        key = 'mycareersfuture_scrapped_website_link.csv'

        # reading of csv directly from s3 via pandas, achieved via usage of s3fs
        output_df = pd.read_csv(
            f"s3://{aws_s3_bucket}/{key}",
        )

        return output_df

    def s3_save_website_link(input_df: pd.DataFrame) -> pd.DataFrame:
        """
        aws s3 saving of website link that was scrapped
        this would continous update the csv file to store all website link that was scrapped
        these website link form as a core reference to identify scrapped or new links
        """
        # filter out the website link from scrapped data
        current_website_df = input_df[['Job_ID','Website_Link']]

        # reading of csv directly from s3 via pandas, achieved via usage of s3fs
        scrapped_website_df = s3_bucket.s3_load_website_link()

        # Setting the frames for the dataframe that are going to be combinded
        frames = [scrapped_website_df, current_website_df]

        # Combining the website_new and website_old dataframe into one dataframe 
        output_df = pd.concat(
                            frames,
                            axis=0,
                            join="outer",
                            ignore_index=True,
                            keys=None,
                            levels=None,
                            names=None,
                            verify_integrity=False,
                            copy=True,
                        )

        # remove duplicate so that only unique website link and Job_ID would be store into the database
        output_df.drop_duplicates(subset=['Job_ID','Website_Link'],inplace=True)

        # importing the full scrapped data from s3 bucket
        # setting the S3 bucket name
        aws_s3_bucket = 'mycareerfuture-scraper'
        # setting the S3 object name / file name
        key = 'mycareersfuture_scrapped_website_link.csv'

        # uploading of csv directly to s3 via pandas, achieved via usage of s3fs
        output_df.to_csv(
            f"s3://{aws_s3_bucket}/{key}",
            index=False,
        )

        return output_df

#   ##  ##    ##    ######  ##   #
#   # ## #   #  #     ##    ###  #
#   # ## #  ######    ##    # ## #
#   #    #  ##  ##    ##    #  ###
#   #    #  ##  ##  ######  #   ##

def main():
    """
    this is the main function of this web scraper for mycareerfuture
    """
    web_total_page_list = base_code.get_max_search() # firstly to identify the number of pages of jobs that in mycareerfuture website
    #web_total_page_list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20] # debug input to limit search parameter
    df_total_job = thread_setup.setup_threaded_workers_search(web_total_page_list) # running the multi-threaded process to scrape the href in each page
    df_total_job = base_code.get_new_jobs_df(df_total_job) # perform filtering of scrapped website link that had already being scrapped

    #print(df_total_job.shape)
    print(f'{df_total_job.shape[0]} Website link identified')
    
    df_final_output = thread_setup.setup_threaded_workers_scraper(df_total_job) # running the multi-threaded process to scrape each job

    #print(df_final_output.shape)
    print(f'{df_final_output.shape[0]} Website scrapped')

    s3_bucket.s3_save_instance_df(df_final_output) # run the function to save the scrapped data into s3 bucket
    print('S3 Bucket Per Instance DataFrame Saved')

    s3_bucket.s3_save_website_link(df_final_output) # run the function to update the website link that scrapped into s3 bucket
    print('S3 Bucket Website Link Updated')

#                   ##   #    ##     ####   ######
#                   ###  #   #  #   # ## #  ##
#                   # ## #  ######  # ## #  #####
#                   #  ###  ##  ##  #    #  ##
#   ######  ######  #   ##  ##  ##  #    #  ######  ######  ######

if __name__ == '__main__':
    main()