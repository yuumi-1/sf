import cProfile
import re
from typing import Dict
from venv import logger
import logging

import snakeviz

from config import DB_CONFIG, DB_CONFIG_Local
from database.connector import MySQLConnector
from processors import BankParserFactory
import io
import requests
import PyPDF2
import pymysql
from dbutils.pooled_db import PooledDB
import warnings
from pdfminer.pdfpage import PDFPage

class PDFProcessor:
    def __init__(self):
        self.pool = PooledDB(
            creator=pymysql,
            # **DB_CONFIG_Local
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
            logger.error(f"提取PDF失败：{e}")
            print(url)
        return text

    def process_pdf(self, pdf_url: str, source_id:int, issuer_name: str) -> str:
        """处理单个PDF文件的完整流程"""
        try:
            # 1. 获取合适的解析器
            parser = self._get_parser_for_pdf(pdf_url)

            # 2. 提取和解析数据
            text = parser.extract_text_from_pdf(pdf_url)
            product_data, benchmark_data = parser.parse_product_info(text)
            product_data['source_id'] = source_id
            benchmark_data['source_id'] = source_id
            product_data['source_type'] = issuer_name
            benchmark_data['source_type'] = issuer_name
            # print(text)
            # print(product_data)
            # print(benchmark_data,"\n")

            # 3. 存储到数据库
            if(self._save_to_database(product_data, benchmark_data, pdf_url)):
                pass
            else:
                return False

            return True
        except Exception as e:
            return False

    def _get_parser_for_pdf(self, pdf_url: str):
        """为PDF获取合适的解析器"""
        # 先下载部分内容用于检测银行类型
        # text = self.extract_text_from_pdf(pdf_url)
        # print(text)
        bank_id = BankParserFactory.detect_bank(pdf_url)
        # print(bank_id)
        return BankParserFactory.get_parser(bank_id)

    def _save_to_database(self, product_data: Dict, benchmark_data: Dict, url) -> bool:
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

            # 插入新记录（设置 create_time 和 update_time）
            if not record_exists_product_announcement:
                product_sql = """
                INSERT INTO product_announcement (
                    reg_code, prd_code, prd_name, amount_raised, 
                    product_start_date, product_end_date, fund_custodian,source_id, source_type,
                    create_time, update_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,NOW(), NOW())
                """
                product_data = [
                    product_data['reg_code'],
                    product_data['prd_code'],
                    product_data['prd_name'],
                    product_data['amount_raised'],
                    product_data['product_start_date'],
                    product_data['product_end_date'],
                    product_data['fund_custodian'],
                    product_data['source_id'],
                    product_data['source_type'],

                ]

            # 更新现有记录（仅更新 update_time）
            else:
                product_sql = """
                UPDATE product_announcement
                SET 
                    prd_name = %s,
                    amount_raised = %s,
                    product_start_date = %s,
                    product_end_date = %s,
                    fund_custodian = %s,
                    source_id = %s,
                    source_type = %s,
                    update_time = NOW()
                WHERE reg_code = %s AND prd_code = %s
                """
                # 调整参数顺序以匹配 UPDATE 语句
                product_data = [
                    product_data['prd_name'],
                    product_data['amount_raised'],
                    product_data['product_start_date'],
                    product_data['product_end_date'],
                    product_data['fund_custodian'],
                    product_data['source_id'],
                    product_data['source_type'],
                    product_data['reg_code'],  # WHERE 条件
                    product_data['prd_code']  # WHERE 条件
                ]
            cursor.execute(product_sql, product_data)
            conn.commit()

            cursor.execute(
                "SELECT 1 FROM performance_benchmark WHERE reg_code = %s AND prd_code = %s",
                (benchmark_data['reg_code'], benchmark_data['prd_code'])
            )
            record_exists_performance_benchmark = cursor.fetchone() is not None
            if benchmark_data['perf_benchmark']:
                if not record_exists_performance_benchmark:
                    # 插入新记录（设置 create_time 和 update_time）
                    benchmark_sql = """
                    INSERT INTO performance_benchmark (
                        reg_code, prd_code, prd_name, perf_benchmark,
                        perf_benchmark_max, perf_benchmark_min, start_date,source_id, source_type ,
                        update_time, create_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """

                    benchmark_data_sql = (
                        benchmark_data['reg_code'],
                        benchmark_data['prd_code'],
                        benchmark_data['prd_name'],
                        benchmark_data['perf_benchmark'],
                        benchmark_data['perf_benchmark_max'],
                        benchmark_data['perf_benchmark_min'],
                        benchmark_data['start_date'],
                        benchmark_data['source_id'],
                        benchmark_data['source_type'],
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
                        source_id = %s,
                        source_type = %s,
                        update_time = NOW()
                    WHERE reg_code = %s AND prd_code = %s
                    """
                    # 调整参数顺序以匹配 UPDATE 语句
                    benchmark_data_sql = (
                        benchmark_data['prd_name'],
                        benchmark_data['perf_benchmark'],
                        benchmark_data['perf_benchmark_max'],
                        benchmark_data['perf_benchmark_min'],
                        benchmark_data['start_date'],
                        benchmark_data['source_id'],
                        benchmark_data['source_type'],
                        benchmark_data['reg_code'],  # WHERE 条件
                        benchmark_data['prd_code']  # WHERE 条件
                    )
                cursor.execute(benchmark_sql, benchmark_data_sql)
                share_list = benchmark_data['perf_benchmark'].split()
                if len(share_list) > 1:
                    # 存在多个份额，分别处理每个份额

                    for share in share_list:
                        share_type = share.split(':')[0]
                        new_prd_code = benchmark_data['prd_code'] + share_type[0]
                        new_prd_name = benchmark_data['prd_name'] + share_type
                        new_benchmark_data = benchmark_data.copy()
                        new_benchmark_data['prd_code'] = new_prd_code
                        new_benchmark_data['prd_name'] = new_prd_name
                        new_benchmark_data['perf_benchmark'] = share
                        try:
                            values = re.finditer(r'\d+\.\d+', new_benchmark_data['perf_benchmark'])
                            L = []
                            for value in values:
                                L.append(value.group())
                            new_benchmark_data['perf_benchmark_max'] = float(max(L)) / 100
                            new_benchmark_data['perf_benchmark_min'] = float(min(L)) / 100
                        except Exception as e:
                            new_benchmark_data['perf_benchmark_max'] = None
                            new_benchmark_data['perf_benchmark_min'] = None
                        cursor.execute(
                            "SELECT 1 FROM performance_benchmark WHERE reg_code = %s AND prd_code = %s AND perf_benchmark = %s",
                            (benchmark_data['reg_code'], benchmark_data['prd_code'], new_benchmark_data['perf_benchmark'])
                        )
                        record_exists_performance_benchmark = cursor.fetchone() is not None
                        if not record_exists_performance_benchmark:
                            # 插入新记录（设置 create_time 和 update_time）
                            benchmark_sql = """
                            INSERT INTO performance_benchmark (
                                reg_code, prd_code, prd_name, perf_benchmark,
                                perf_benchmark_max, perf_benchmark_min, start_date,source_id, source_type ,
                                update_time, create_time
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                            """

                            benchmark_data_sql = (
                                new_benchmark_data['reg_code'],
                                new_benchmark_data['prd_code'],
                                new_benchmark_data['prd_name'],
                                new_benchmark_data['perf_benchmark'],
                                new_benchmark_data['perf_benchmark_max'],
                                new_benchmark_data['perf_benchmark_min'],
                                new_benchmark_data['start_date'],
                                new_benchmark_data['source_id'],
                                new_benchmark_data['source_type'],
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
                                source_id = %s,
                                source_type = %s,
                                update_time = NOW()
                            WHERE reg_code = %s AND prd_code = %s
                            """
                            # 调整参数顺序以匹配 UPDATE 语句
                            benchmark_data_sql = (
                                new_benchmark_data['prd_name'],
                                new_benchmark_data['perf_benchmark'],
                                new_benchmark_data['perf_benchmark_max'],
                                new_benchmark_data['perf_benchmark_min'],
                                new_benchmark_data['start_date'],
                                new_benchmark_data['source_id'],
                                new_benchmark_data['source_type'],
                                new_benchmark_data['reg_code'],  # WHERE 条件
                                new_benchmark_data['prd_code']  # WHERE 条件
                            )
                        cursor.execute(benchmark_sql, benchmark_data_sql)

                conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"DB Error: {e}")
            print(f"{url}")

        finally:
            conn.close()


if __name__ == "__main__":
    # 获取 pdfminer 的日志记录器
    pdfminer_logger = logging.getLogger('pdfminer')
    # 设置日志级别为 ERROR，忽略 WARNING 及以下级别的日志
    pdfminer_logger.setLevel(logging.ERROR)
    conn = MySQLConnector()
    start_count = 2200
    total_count = conn.execute_query("SELECT COUNT(*) as cnt FROM est_file_tasks")[0]['cnt']
    print(f"总数据: {total_count} 条")
    sql_count = (start_count, 200)
    path = conn.execute_query("SELECT id ,issuer_name ,local_path FROM est_file_tasks LIMIT %s,%s", sql_count)
    processor = PDFProcessor()
    success_count = 0
    fail_count = 0
    fail = []
    fail_url=[]
    for i in path:
        pdf_url = i['local_path']
        try:
            if success_count % 100 == 0:
                print(f"\nRow:{success_count // 100}",end='')
            cProfile.run('processor.process_pdf(pdf_url,i[\'id\'],i[\'issuer_name\'])', 'profile_data.prof')
            processor.process_pdf(pdf_url,i['id'],i['issuer_name'])
            success_count = success_count + 1
            print("-", end="")
        except Exception as e:
            fail.append(e)
            print("\n", end="")
            print(pdf_url)
            fail_count = fail_count + 1
            fail_url.append(pdf_url)
    print(f"\n成功：{success_count}条数据，失败：{fail_count}条数据")