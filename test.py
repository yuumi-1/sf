import re

from database.connector import MySQLConnector
from main import PDFProcessor

if __name__ == "__main__":
    url='https://licai-oss-bucket-1301073378.cos.ap-guangzhou.myqcloud.com/Financial_announcement_tmp/cqrcb/%E6%B8%9D%E5%86%9C%E5%95%86%E7%90%86%E8%B4%A2%E5%85%B4%E6%97%B6%EF%BC%88%E6%9C%80%E7%9F%AD%E6%8C%81%E6%9C%891%E4%B8%AA%E6%9C%88%EF%BC%89%E6%AF%8F%E6%97%A5%E5%BC%80%E6%94%BE1%E5%8F%B7%E5%8F%91%E8%A1%8C%E5%85%AC%E5%91%8A_21GSGK11201_2021-12-29_Ncc8ulLktd.pdf'
    processor = PDFProcessor()
    processor.process_pdf(url)
    # str='募集规模(元) 88,302,053.00 '
    # str_=re.search(r'规模\s*(.+)',str)
    # str_=str_.group(1)
    # print(str_)
    # print(str_[3:].strip())