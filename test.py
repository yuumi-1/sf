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
    url = "https://licai-oss-bucket-1301073378.cos.ap-guangzhou.myqcloud.com/Financial_announcement_tmp/cqrcb/渝农商理财渝快宝7号发行公告_24GSGK12807_2024-01-25.pdf"
    processor = PDFProcessor()
    text = processor.extract_text_from_pdf(url)
    print("="*50)
    processor.process_pdf(url,1,"123")
    print("=" * 50)

