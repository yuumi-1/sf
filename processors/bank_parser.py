import io
import re
from typing import Dict, Tuple, Optional, Any, Union
import datetime

import pdfplumber
import requests

from .base_parser import BaseBankParser


class CQRCBParser(BaseBankParser):
    """重庆农商行(CQRCB)产品公告解析器"""

class ABCParser(BaseBankParser):
    """中国农业银行(ABC)农银理财产品公告解析器"""

    def extract_text_from_pdf(self, url: str):
        """从PDF文件中提取文本内容"""
        response = requests.get(url)
        tables = []

        try:
            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                for page in pdf.pages:
                    table = page.extract_table()
                    if table is not None:
                        tables.extend(table)
            for i in range(len(tables)):
                for j in range(len(tables[i])):
                    if tables[i][j]:
                        tables[i][j] = tables[i][j].replace('\n', '')
            tables=self.check_data(tables)
            return tables
        except Exception as e:
            print(e)
            print(url)

    def parse_product_info(self, text) -> Tuple[Dict, Dict]:
        """解析提取的文本，返回两个表的数据"""
        try:
            data = self.list_to_dict(text)
            product_info = {
                'reg_code': data.get('全国银行业理财产品登记系统登记编码'),
                'prd_code': data.get('产品代码') or data.get('产品销售代码'),
                'prd_name': data.get('产品名称'),
                'amount_raised': data.get('募集规模'),
                'product_start_date': data.get('成立日'),
                'product_end_date': data.get('到期日'),
                'fund_custodian': None
            }
            benchmark_info = {
                'reg_code': product_info['reg_code'],
                'prd_code': product_info['prd_code'],
                'prd_name': product_info['prd_name'],
                'perf_benchmark': None,
                'perf_benchmark_max': None,
                'perf_benchmark_min': None,
                'start_date': product_info['product_start_date']  # 默认使用产品起始日期
            }
            return product_info, benchmark_info
        except Exception as e:
            pass

    def check_data(self,data):
        new_header = []
        header = data[0]
        for item in header:
            # 检查是否存在重复字符
            if item[::2] == item[1::2] or item[::2][:-1] == item[1::2]:
                # 如果存在重复字符，取偶数索引字符
                new_header.append(item[::2])
            else:
                # 如果不存在重复字符，保持原样
                new_header.append(item)
        data[0] = new_header
        return data

    def list_to_dict(self, table):
        headers = table[0]
        data = {}
        for i, header in enumerate(headers):
            value = table[1][i]
            value = re.sub(r"[“”]","",value)
            if "募集" in header:
                if "万元" in header:
                    value = float(value.replace("，", ""))
                    data["募集规模"] = value * 10000
                elif "亿元" in header:
                    value = float(value.replace("，", ""))
                    data["募集规模"] = value * 100000000
                else:
                    value = float(re.sub(r"[,，]", "", value))
                    data["募集规模"] = value
            elif "成立日" in header:
                dt = datetime.datetime.strptime(value, "%Y/%m/%d")
                data[header] = dt
            else:
                data[header] = value
        return data



