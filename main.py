from typing import Dict
from venv import logger

from config import DB_CONFIG
from database.connector import MySQLConnector
from processors import BankParserFactory
import io
import requests
import PyPDF2
import pymysql
from dbutils.pooled_db import PooledDB

class PDFProcessor:
    def __init__(self):
        self.pool = PooledDB(
            creator=pymysql,
            **DB_CONFIG
        )

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

    def process_pdf(self, pdf_url: str):
        """处理单个PDF文件的完整流程"""
        try:
            # 1. 获取合适的解析器
            parser = self._get_parser_for_pdf(pdf_url)

            # 2. 提取和解析数据
            product_data, benchmark_data = parser.parse_product_info(
                parser.extract_text_from_pdf(pdf_url)
            )
            print(product_data)
            print(benchmark_data,"\n")


            # 3. 存储到数据库
            self._save_to_database(product_data, benchmark_data)

            return True
        except Exception as e:
            print(f"处理PDF失败: {e}")
            return False

    def _get_parser_for_pdf(self, pdf_url: str):
        """为PDF获取合适的解析器"""
        # 先下载部分内容用于检测银行类型
        text = self.extract_text_from_pdf(pdf_url)
        print(text)
        bank_id = BankParserFactory.detect_bank(text)
        print(bank_id)
        return BankParserFactory.get_parser(bank_id)

    def _save_to_database(self, product_data: Dict, benchmark_data: Dict) -> bool:
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
            # print(product_data)
            conn.rollback()
            logger.error(f"DB Error: {e}")  # 建议使用logging
        finally:
            conn.close()


if __name__ == "__main__":
    conn = MySQLConnector()
    total_count = conn.execute_query("SELECT COUNT(*) as cnt FROM est_file_tasks")[0]['cnt']
    print(f"总数据: {total_count} 条")
    sql_count = (0, 100)
    path = conn.execute_query("SELECT local_path FROM est_file_tasks LIMIT %s,%s", sql_count)
    processor = PDFProcessor()
    success_count = 0
    fail_count = 0
    fail = []
    for i in path:
        pdf_url = i['local_path']
        try:
            processor.process_pdf(pdf_url)
            success_count = success_count + 1
            print("-", end="")
        except Exception as e:
            fail.append(e)
            print("\n", end="")
            print(pdf_url)
            fail_count = fail_count + 1
    print(f"\n成功：{success_count}条数据，失败：{fail_count}条数据")