import re
from abc import ABC, abstractmethod
from typing import Dict, Tuple, Optional
import datetime
import requests
import io
import PyPDF2

class BaseBankParser(ABC):
    """所有银行解析器的抽象基类"""

    def extract_text_from_pdf(self, url: str):
        """从PDF文件中提取文本内容"""

        text = ""
        # 下载PDF文件
        response = requests.get(url)
        response.raise_for_status()  # 检查请求是否成功
        try:
            with io.BytesIO(response.content) as file:
                reader = PyPDF2.PdfReader(file)
                for page in reader.pages:
                    text += "\n"
                    text += page.extract_text()
        except Exception as e:
            print(f"提取PDF文本失败: {e}")
        # print(text)
        return text

    def parse_product_info(self, text) -> Tuple[Dict, Dict]:
        """解析提取的文本，返回两个表的数据"""
        # 产品基本信息
        product_info = {
            'reg_code': self._extract_by_pattern(text, r'登记编码\s*(\w+)'),
            'prd_code': self._extract_by_pattern(text, r'(?:代码|编号)\s*(\w+)'),
            'prd_name': self._extract_by_pattern(text, r"产品名称\s*([\s\S]+?)(?=\n.*产品代码|产品编号|$)").replace("\n",""),
            'amount_raised': self._parse_decimal(text, r'(?:规模|募集金额)\s*(.+)'),
            'product_start_date': self._parse_date(text, r'(?:产品起始日期|成立日)\s+(.+)'),
            'product_end_date': self._parse_date(text, r'(?:终止日期|产品期限|产品到期日|产品结束日期)\s+(.+)'),
            'fund_custodian': self._extract_by_pattern(text, r'(?:托管机构|托管人|托管账户开户行)\s*(.+)')
        }
        benchmark = self._parse_benchmark(text, r'(?:基准|收益率)\s*(.+)'),
        benchmark_max = benchmark[0]['max']
        benchmark_min = benchmark[0]['min']
        try:
            _benchmark_max = float(benchmark_max) / 100
            _benchmark_min = float(benchmark_min) / 100
        except Exception as e:
            _benchmark_max = None
            _benchmark_min = None
        # 业绩比较基准信息
        benchmark_info = {
            'reg_code': product_info['reg_code'],
            'prd_code': product_info['prd_code'],
            'prd_name': product_info['prd_name'],
            # 'perf_benchmark': self._extract_by_pattern(text, r'(?:基准(?:（年化）)?|收益率)\s*(.+)'),
            'perf_benchmark': self._extract_by_pattern(text, r'(?:基准(?:\(年化\))?\s*)(.*)'),
            'perf_benchmark_max': _benchmark_max,
            'perf_benchmark_min': _benchmark_min,
            'start_date': product_info['product_start_date']  # 默认使用产品起始日期
        }
        # print(text)
        # print(benchmark_info,"\n")
        # print(product_info)
        return product_info, benchmark_info

    def _extract_by_pattern(self, text: str, pattern: str) -> Optional[str]:
        """使用正则表达式提取文本"""
        match = re.search(pattern, text)
        try:
            if "份额" in match.group(1):
                _match = re.findall(r'([A-Z]份额[：:]\d+\.\d+%(?:-\d+\.\d+%)?)', text, re.MULTILINE)
                if not _match:
                    pattern = r"([A-Z]份额:).*?(\1.*?)(?=[A-Z]份额:|$)"
                    _text = re.search(r"基准\s*([\s\S]+?)(?=\n.*备注|$)", text).group(1).replace("\n", "")
                    _match = re.findall(pattern, _text)
                    result = " ".join(value for name, value in _match)
                    return result
                result = " ".join(_match)
                return result
            else:
                return match.group(1).strip()
        except Exception as e:
            return "未披露"

    def _parse_decimal(self, text: str, pattern: str) -> Optional[float]:
        """解析十进制数字"""

        try:
            match = self._extract_by_pattern(text, pattern)
            if match.startswith("人民币"):
                match = match[3:]
            if match.endswith('万'):
                num_str = match[:-1]  # 去除"万"字
                multiplier = 10000
                return float(match[:-1].replace(',', '')) * multiplier
            elif match.endswith('万元'):
                num_str = match[:-2]  # 去除"万元"字
                multiplier = 10000
                return float(re.sub(r'[,，]', '', match[:-2])) * multiplier
            elif match.startswith('(元)'):
                multiplier = 1
                return float(re.sub(r'[,，]', '', match[4:])) * multiplier

            else:
                num_str = match
                multiplier = 1
                return float(match[:-1].replace(',', '')) * multiplier

        except Exception as e:
            return None

    def _parse_benchmark(self, text: str, pattern: str) -> dict:
        """基准率解析"""
        match = self._extract_by_pattern(text, r'(?:基准|收益率)\s*(.+)')
        values = re.finditer(r'\d+\.\d+', match)
        L=[]
        for value in values:
            L.append(value.group())
        if len(L) == 0:
            return {'min': None, 'max': None}
        return {'min': min(L), 'max': max(L)}

    def _parse_date(self, text: str, pattern: str) -> Optional[datetime]:
        """解析中文日期字符串"""
        try:
            # 先提取原始日期字符串（可能含期限）
            date_str = self._extract_by_pattern(text, pattern)

            # 处理中文日期范围（x年x月x日-x年x月x日）
            if "-" in date_str and "年" in date_str and "月" in date_str and "日" in date_str:
                date_parts = date_str.split("-")
                if len(date_parts) == 2:
                    return datetime.datetime.strptime(date_parts[1].strip(), "%Y年%m月%d日").date()

            # 处理独立中文日期（x年x月x日）
            if "年" in date_str and "月" in date_str and "日" in date_str:
                return datetime.datetime.strptime(date_str, "%Y年%m月%d日").date()

            # 处理yyyy-mm-dd格式
            if "-" in date_str and len(date_str.split("-")) == 3:
                parts = date_str.split("-")
                if len(parts[0]) == 4:  # 确保是yyyy-mm-dd格式
                    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

            return None

        except Exception as e:
            return None

