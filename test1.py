import datetime
import io
from venv import logger

import requests
import re
from typing import Optional, Dict, Tuple
import webbrowser
import PyPDF2
import pymysql
from dbutils.pooled_db import PooledDB

from database.connector import MySQLConnector


class PDFProcessor:

    def __init__(self):
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=200,
            host='localhost',
            user='root',
            password='123456',
            database='testdb2',
            port=3306
        )

        # self.pool = PooledDB(
        #     creator=pymysql,
        #     maxconnections=200,
        #     host='gz-cynosdbmysql-grp-p1gmpvp3.sql.tencentcdb.com',
        #     user='ly',
        #     password='7qc04tTU',
        #     database='files_library_tmp',
        #     port=21844
        # )


    def process_pdf(self, pdf_path: str):
        """处理PDF文件的完整流程"""
        text = self.extract_text_from_pdf(pdf_path)
        if not text:
            print("未提取到有效文本")
            return
        product_data, benchmark_data = self.parse_product_info(text)
        if self.save_to_database(product_data, benchmark_data):
            pass
        else:
            print("数据保存失败")

    def extract_text_from_pdf(self, url: str) -> str:
        """从PDF文件中提取文本内容"""
        text = ""
        # 下载PDF文件
        response = requests.get(url)
        response.raise_for_status()  # 检查请求是否成功
        try:
            with io.BytesIO(response.content) as file:
                reader = PyPDF2.PdfReader(file)
                for page in reader.pages:
                    text += page.extract_text()
        except Exception as e:
            print(f"提取PDF文本失败: {e}")
        # print(text)
        return text


    def parse_product_info(self, text: str) -> Tuple[Dict, Dict]:
        """解析提取的文本，返回两个表的数据"""
        # 产品基本信息
        product_info = {
            'reg_code': self._extract_by_pattern(text, r'登记编码\s*(\w+)'),
            'prd_code': self._extract_by_pattern(text, r'(?:代码|编号)\s*(\w+)'),
            'prd_name': self._extract_by_pattern(text, r'产品名称\s*(.+)'),
            'amount_raised': self._parse_decimal(text, r'规模\s*(.+)'),
            'product_start_date': self._parse_date(text, r'(?:产品起始日期|成立日)\s+(.+)'),
            'product_end_date': self._parse_date(text, r'(?:终止日期|产品期限|产品结束日期)\s+(.+)'),
            'fund_custodian': self._extract_by_pattern(text, r'(?:托管机构|产品托管账户开户行)\s*(.+)')
        }
        benchmark=self._parse_benchmark(text, r'(?:基准|收益率)\s*(.+)'),
        benchmark_max=benchmark[0]['max']
        benchmark_min=benchmark[0]['min']
        try:
            _benchmark_max = float(benchmark_max[:-1]) / 100
            _benchmark_min = float(benchmark_min[:-1]) / 100
        except Exception as e:
            _benchmark_max = None
            _benchmark_min = None
        # 业绩比较基准信息
        benchmark_info = {
            'reg_code': product_info['reg_code'],
            'prd_code': product_info['prd_code'],
            'prd_name': product_info['prd_name'],
            'perf_benchmark': self._extract_by_pattern(text, r'(?:基准|收益率)\s*(.+)'),
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
                _match = re.findall(r'(^.*[A-Z]份额.*$)', text, re.MULTILINE)
                result = " ".join(_match)
                return result
            else:
                return match.group(1).strip()
        except Exception as e:
            return "未披露"

    def _parse_decimal(self,text: str, pattern: str) -> Optional[float]:
        """解析十进制数字"""

        try:
            match = self._extract_by_pattern(text, pattern)
            if match.endswith('万'):
                num_str = match[:-1]  # 去除"万"字
                multiplier = 10000
                return float(match[:-1].replace(',', '')) * multiplier
            elif match.endswith('万元'):
                num_str = match[:-2]  # 去除"万元"字
                multiplier = 10000
                return float(re.sub(r'[,，]','',match[:-2])) * multiplier
            elif match.startswith('(元)'):
                multiplier = 1
                return float(re.sub(r'[,，]', '', match[4:])) * multiplier

            else:
                num_str = match
                multiplier = 1
                return float(match[:-1].replace(',', '')) * multiplier

        except Exception as e:
            return None

    def _parse_benchmark(self,text: str, pattern: str) -> dict:
        """基准率解析"""
        patterns = [
            r'([A-Z]份额:\d+\.\d+%[^%]*[A-Z]份额:\d+\.\d+%)',  # A份额:a% B份额:b%
            r'(\d+\.\d+%\s*-\s*\d+\.\d+%)',  # a% - b%
            r'(\d+\.\d+%)'  # 单一值
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match :
                values = re.findall(r'\d+\.\d+%', match.group())
                return {
                    'min': min(values, key=lambda x: float(x[:-1])),
                    'max': max(values, key=lambda x: float(x[:-1]))
                }
        return {'min': None, 'max': None}
        # _match = re.search(pattern, text)
        # match=_match.group(1).strip()
        # if "-" in match:
        #     _parts = match.split("-")
        #     _parts_min=_parts[0].strip()
        #     _parts_max=_parts[1].strip()
        #     return {'min': _parts_min, 'max': _parts_max}
        # if "份额" in match:
        #     _match = re.findall(r'(\d+\.\d+)%', text,re.MULTILINE)
        #     _parts_min=_match[0]
        #     try:
        #         _parts_max=_match[1]
        #     except IndexError:
        #         _parts_max = _match[0]
        #     return {'min': _parts_min, 'max': _parts_max}
        #
        # else:
        #     try:
        #         _parts_min=match
        #         _parts_max=match
        #         return {'min': _parts_min, 'max': _parts_max}
        #     except IndexError:
        #         return None


    def _parse_date(self,text: str, pattern: str) -> Optional[datetime]:
        """解析中文日期字符串"""
        try:
            # 先提取原始日期字符串（可能含期限）
            date_str = self._extract_by_pattern(text, pattern)

            #处理中文日期范围（x年x月x日-x年x月x日）
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

            return datetime.date(9999,12,31)

        except Exception as e:
            # 可添加日志：logging.warning(f"日期解析失败: {e}, 原始文本: {text}")
            return None

    def save_to_database(self, product_data: Dict, benchmark_data: Dict) -> bool:
        """将数据保存到数据库"""
        conn = self.pool.connection()
        try:
            cursor = conn.cursor()

            # 检查记录是否已存在
            cursor.execute(
                "SELECT 1 FROM product_announcement WHERE reg_code = %s AND prd_code = %s",
                (product_data['reg_code'], product_data['prd_code'])
            )
            record_exists_product_announcement = cursor.fetchone() is not None

            if not record_exists_product_announcement:
                # 插入新记录（设置 create_time 和 update_time）
                product_sql = """
                INSERT INTO product_announcement (
                    reg_code, prd_code, prd_name, amount_raised, 
                    product_start_date, product_end_date, fund_custodian,
                    create_time, update_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """
                product_data = [
                    product_data['reg_code'],
                    product_data['prd_code'],
                    product_data['prd_name'],
                    product_data['amount_raised'],
                    str(product_data['product_start_date']),
                    str(product_data['product_end_date']),
                    product_data['fund_custodian'],
                ]


            else:
                # 更新现有记录（仅更新 update_time）
                product_sql = """
                UPDATE product_announcement
                SET 
                    prd_name = %s,
                    amount_raised = %s,
                    product_start_date = %s,
                    product_end_date = %s,
                    fund_custodian = %s,
                    update_time = NOW()
                WHERE reg_code = %s AND prd_code = %s
                """
                # 调整参数顺序以匹配 UPDATE 语句
                product_data = [
                    product_data['prd_name'],
                    product_data['amount_raised'],
                    str(product_data['product_start_date']),
                    str(product_data['product_end_date']),
                    product_data['fund_custodian'],
                    product_data['reg_code'],  # WHERE 条件
                    product_data['prd_code']  # WHERE 条件
                ]
            if product_data[3] == 'None':
                product_data[3] = 'Null'
                print(product_data[3])
            cursor.execute(product_sql, product_data)
            conn.commit()

            cursor.execute(
                "SELECT 1 FROM performance_benchmark WHERE reg_code = %s AND prd_code = %s",
                (benchmark_data['reg_code'], benchmark_data['prd_code'])
            )
            record_exists_performance_benchmark = cursor.fetchone() is not None
            if not record_exists_performance_benchmark:
                # 插入新记录（设置 create_time 和 update_time）
                benchmark_sql = """
                INSERT INTO performance_benchmark (
                    reg_code, prd_code, prd_name, perf_benchmark,
                    perf_benchmark_max, perf_benchmark_min, start_date,
                    update_time, create_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """
                benchmark_data = (
                    benchmark_data['reg_code'],
                    benchmark_data['prd_code'],
                    benchmark_data['prd_name'],
                    benchmark_data['perf_benchmark'],
                    benchmark_data['perf_benchmark_max'],
                    benchmark_data['perf_benchmark_min'],
                    benchmark_data['start_date'],
                )

            else:
                # 更新现有记录（仅更新 update_time）
                benchmark_sql = """
                UPDATE performance_benchmark
                SET 
                    prd_name = %s,
                    perf_benchmark = %s,
                    perf_benchmark_max = %s,
                    perf_benchmark_min = %s,
                    start_date = %s,
                    update_time = NOW()
                WHERE reg_code = %s AND prd_code = %s
                """
                # 调整参数顺序以匹配 UPDATE 语句
                benchmark_data = (
                    benchmark_data['prd_name'],
                    benchmark_data['perf_benchmark'],
                    benchmark_data['perf_benchmark_max'],
                    benchmark_data['perf_benchmark_min'],
                    benchmark_data['start_date'],
                    benchmark_data['reg_code'],  # WHERE 条件
                    benchmark_data['prd_code']  # WHERE 条件
                )
            cursor.execute(benchmark_sql, benchmark_data)
            conn.commit()
            return True

        except Exception as e:
            print(f"数据库操作失败: {e}")
            print(pdf_url)
            # print(product_data)
            conn.rollback()
            logger.error(f"DB Error: {e}")  # 建议使用logging
        finally:
            conn.close()

if __name__ == "__main__":
    conn=MySQLConnector()
    total_count = conn.execute_query("SELECT COUNT(*) as cnt FROM est_file_tasks")[0]['cnt']
    print(f"总数据: {total_count} 条")
    sql_count = (0,1)
    path = conn.execute_query("SELECT local_path FROM est_file_tasks LIMIT %s,%s",sql_count)
    processor = PDFProcessor()
    success_count = 0
    fail_count = 0
    fail=[]
    for i in path:
        pdf_url = i['local_path']
        try:
            processor.process_pdf(pdf_url)
            success_count=success_count+1
            print("-",end="")
        except Exception as e:
            fail.append(e)
            print("\n",end="")
            print(pdf_url)
            fail_count=fail_count+1
    print(f"\n成功：{success_count}条数据，失败：{fail_count}条数据")