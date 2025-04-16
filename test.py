import io
import re
import warnings
import logging

import pdfplumber
import requests
import matplotlib.pyplot as plt
from numpy.lib.recfunctions import drop_fields

from main import PDFProcessor


if __name__ == "__main__":

    # 获取 pdfminer 的日志记录器
    pdfminer_logger = logging.getLogger('pdfminer')
    # 设置日志级别为 ERROR，忽略 WARNING 及以下级别的日志
    pdfminer_logger.setLevel(logging.ERROR)
    url = "https://licai-oss-bucket-1301073378.cos.ap-guangzhou.myqcloud.com/Financial_announcement/ewealth/中国农业银行理财产品发行公告（2019年3月6日）_AD193603_2019-03-06_LCkemCzsbb.pdf"
    processor = PDFProcessor()
    text = processor.extract_text_from_pdf(url)
    print("="*50)
    processor.process_pdf(url)
    print("=" * 50)

