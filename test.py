import io
import re
import warnings
import logging

import pdfplumber
import pymysql
import requests
import matplotlib.pyplot as plt
from dbutils.pooled_db import PooledDB
from numpy.lib.recfunctions import drop_fields

from config import DB_CONFIG_Local
from main import PDFProcessor
from processors.bank_parser import ABCParser

if __name__ == "__main__":

    # 获取 pdfminer 的日志记录器
    pdfminer_logger = logging.getLogger('pdfminer')
    # 设置日志级别为 ERROR，忽略 WARNING 及以下级别的日志
    pdfminer_logger.setLevel(logging.ERROR)
    url = "https://licai-report-1301073378.cos.ap-guangzhou.myqcloud.com/Financial_announcement/cebwm/2021-06/%E9%98%B3%E5%85%89%E6%A9%99%E4%BC%98%E9%80%89%E5%85%A8%E6%98%8E%E6%98%9F%E7%90%86%E8%B4%A2%E4%BA%A7%E5%93%81%E5%8F%91%E8%A1%8C%E5%85%AC%E5%91%8A_EW0101_2021-06-09.pdf"
    processor = PDFProcessor()
    processor.pool = PooledDB(
            creator=pymysql,
            **DB_CONFIG_Local
        )
    text = processor.extract_text_from_pdf(url)
    print("="*50)
    processor.process_pdf(url,123456,"test")
    print("=" * 50)