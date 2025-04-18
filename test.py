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
    url = "https://licai-report-1301073378.cos.ap-guangzhou.myqcloud.com/Financial_announcement/cebwm/2023-02/阳光金15M丰利27期理财产品发行公告_EW1011_2023-02-15.pdf"
    processor = PDFProcessor()
    processor.pool = PooledDB(
            creator=pymysql,
            **DB_CONFIG_Local
        )
    text = processor.extract_text_from_pdf(url)
    print("="*50)
    processor.process_pdf(url,123456,"test")
    print("=" * 50)