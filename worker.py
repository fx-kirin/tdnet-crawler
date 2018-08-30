#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 zenbook <zenbook@zenbook-XPS>
#
# Distributed under terms of the MIT license.

"""

"""
from __future__ import with_statement
import os
import requests
import sqlite3
import fire
import datetime
import time
import logging
from io import StringIO, BytesIO
from mkdir_p import mkdir_p
from contextlib import closing
from bs4 import BeautifulSoup
import pdfminer
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.pdfpage import PDFPage

OUTPUT_DIR = os.path.expanduser('~/tdnet_data/')

def record_data(session, cur, conn, current_date, pdf_dir, xbrl_dir, soup):
    if soup.find(id='pager-box-top'):
        trs = soup.find(id='main-list-table').find_all('tr')
        for tr in trs:
            tds = tr.find_all('td')
            pdf = tds[3].find('a').get('href')
            item_id = pdf[:-4]
            query = 'SELECT count(*) FROM td_net WHERE item_id=%s;'%(item_id)
            is_found = cur.execute(query).fetchone()[0]
            if is_found == 0:
                tmp = tds[0].text
                date = datetime.datetime(current_date.year, current_date.month, current_date.day, int(tmp[0:2]), int(tmp[3:5]))
                stock_code = tds[1].text[0:4]
                stock_code_long = tds[1].text
                company_name = tds[2].text.strip()
                title = tds[3].text
                with open('%s/%s.pdf'%(pdf_dir, item_id), 'wb') as f:
                    result = session.get('https://www.release.tdnet.info/inbs/%s.pdf'%(item_id))
                    time.sleep(0.5)
                    pdf_content = result.content
                    f.write(result.content)
                pdf = '%s/%s.pdf'%(pdf_dir, item_id)
                xbrl = tds[4].text.strip()
                if xbrl != '':
                    xbrl = tds[4].find('a').get('href')
                    with open('%s/%s'%(xbrl_dir, xbrl), 'wb') as f:
                        result = session.get('https://www.release.tdnet.info/inbs/%s'%(xbrl))
                        time.sleep(0.5)
                        f.write(result.content)
                xbrl = '%s/%s'%(xbrl_dir, xbrl)
                security = tds[5].text.strip()
                refresh_info = tds[6].text.strip()
                
                codec = 'utf-8'
                rsrcmgr = PDFResourceManager()
                outfp = BytesIO()
                laparams = LAParams()
                laparams.detect_vertical = True
                
                device = TextConverter(rsrcmgr, outfp, codec=codec, laparams=laparams)
                fp = BytesIO(pdf_content)
                interpreter = PDFPageInterpreter(rsrcmgr, device)
                try:
                    for page in PDFPage.get_pages(fp):
                        interpreter.process_page(page)
                except pdfminer.pdfdocument.PDFTextExtractionNotAllowed:
                    logging.warn('Text extraction is not allowed. %s'%(item_id))
                except pdfminer.pdfdocument.PDFEncryptionError:
                    logging.warn('Text extraction couldn\'t be handled. %s'%(item_id))
                    
                content = outfp.getvalue()
                outfp.close()
                fp.close()
                device.close()
                
                query = '''
                    INSERT INTO td_net(
                        date, item_id, stock_code, stock_code_long,
                        company_name, title, content, xbrl, pdf, security, refresh_info)
                    VALUES("%s", "%s", "%s", "%s", "%s", '%s', "%s", "%s", "%s", "%s", "%s")
                '''%(date, item_id, stock_code, stock_code_long, company_name, title, 
                    content.decode('utf8'), xbrl, pdf, security, refresh_info)
                try:
                    cur.execute(query)
                except sqlite3.OperationalError:
                    logging.warn('Encoding Problem on %s'%(item_id))
                    content = ''
                    query = '''
                        INSERT INTO td_net(
                            date, item_id, stock_code, stock_code_long,
                            company_name, title, content, xbrl, pdf, security, refresh_info)
                        VALUES("%s", "%s", "%s", "%s", "%s", '%s', "%s", "%s", "%s", "%s", "%s")
                    '''%(date, item_id, stock_code, stock_code_long, company_name, title, 
                        content.decode('utf8'), xbrl, pdf, security, refresh_info)
                conn.commit()

def main(date_range=1):
    session = requests.Session()
    adapters = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount("http://", adapters)
    session.mount("https://", adapters)
    
    pdf_root_dir = OUTPUT_DIR+'/pdf'
    xbrl_root_dir = OUTPUT_DIR+'/xbrl'
    mkdir_p(OUTPUT_DIR)
    mkdir_p(pdf_root_dir)
    mkdir_p(xbrl_root_dir)
    dbname = '%s/sqlite3.db'%(OUTPUT_DIR)
    with closing(sqlite3.connect(dbname)) as conn:
        cur = conn.cursor()
        query = '''
        CREATE TABLE IF NOT EXISTS td_net (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            date DATETIME, 
            item_id text, 
            stock_code text, 
            stock_code_long text, 
            company_name text, 
            title text,
            content text,
            xbrl text,
            pdf text,
            security text,
            refresh_info text
        );
        '''
        cur.execute(query)
        
        now = datetime.datetime.now()
        start_date = datetime.datetime(now.year, now.month, now.day)
        current_date = start_date
        for i in range(date_range):
            current_date -= datetime.timedelta(days=1)
            logging.info('Collecting data on %s'%(current_date))
            datestring = current_date.strftime('%Y%m%d')
            result = session.get('https://www.release.tdnet.info/inbs/I_list_001_%s.html'%(datestring))
            time.sleep(0.5)
            soup = BeautifulSoup(result.content, 'lxml')
            if soup.find(id='pager-box-top'):
                page_size = len(soup.find(id='pager-box-top').find_all('div')) - 3
                pdf_dir = pdf_root_dir + '/%s'%(datestring)
                xbrl_dir = xbrl_root_dir + '/%s'%(datestring)
                mkdir_p(pdf_dir)
                mkdir_p(xbrl_dir)
                record_data(session, cur, conn, current_date, pdf_dir, xbrl_dir, soup)
                for a in range(page_size - 1):
                    result = session.get('https://www.release.tdnet.info/inbs/I_list_0%02d_%s.html'%(a + 2, datestring))
                    time.sleep(0.5)
                    soup = BeautifulSoup(result.content, 'lxml')
                    record_data(session, cur, conn, current_date, pdf_dir, xbrl_dir, soup)
        conn.commit()

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s|%(threadName)s|%(levelname)s : %(message)s', level=logging.INFO)
    fire.Fire(main)
