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
    url = "https://licai-report-1301073378.cos.ap-guangzhou.myqcloud.com/Financial_announcement/ewealth/2024-08/农银理财农银安心·天天利同业存单及存款增强4号理财产品（对公低波悦享）发行公告（产品销售代码：NYADTTLTCZQDGDB4）_NYADTTLTCZQDGDB4_2024-08-28.pdf"
    processor = PDFProcessor()
    text = processor.extract_text_from_pdf(url)
    print("="*50)
    processor.process_pdf(url)
    print("=" * 50)

