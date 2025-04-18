import gc
import io
import camelot
import pdfplumber
import pymysql
from datetime import datetime
import requests


def _save_to_database(data):
    try:
        # 建立 MySQL 数据库连接
        connection = pymysql.connect(
            host='localhost',
            database='testdb3',
            user='root',
            password='123456'
        )
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO product_announcement (
                    reg_code, prd_code, prd_name, amount_raised, 
                    product_start_date, product_end_date, fund_custodian,
                    create_time, update_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """
            for row in data:
                reg_code = row[0].strip()
                prd_code = row[1].strip()
                prd_name = row[2].strip()
                # 处理金额，去除非数字字符并转换为合适的格式
                amount_raised_str = ''.join(filter(str.isdigit, row[3]))
                amount_raised = float(amount_raised_str) if amount_raised_str else 0.0
                # 处理日期
                product_start_date = datetime.strptime(row[4].strip(), '%Y-%m-%d').date()
                product_end_date = datetime.strptime(row[5].strip(), '%Y-%m-%d').date() if row[5].strip() else None
                fund_custodian = row[6].strip() if row[6].strip() else None
                update_time = datetime.now()
                create_time = datetime.now()
                # 插入数据
                cursor.execute(insert_query, (reg_code, prd_code, prd_name, amount_raised,
                                              product_start_date, product_end_date,
                                              fund_custodian, update_time, create_time))
            connection.commit()
            print(f"{cursor.rowcount} 条记录插入成功。")
    except pymysql.Error as e:
        print(f"数据库操作出错: {e}")
    finally:
        if connection:
            connection.close()

def _extract_table(url):
    try:
        # 发送请求获取 PDF 文件内容
        response = requests.get(url)
        response.raise_for_status()

        # 将响应内容以二进制流的形式读取
        pdf_stream = io.BytesIO(response.content)

        # 使用 camelot 读取 PDF 文件
        tables = camelot.read_pdf(pdf_stream, flavor='lattice')
        return tables
    except requests.RequestException as e:
        print(f"请求出错: {e}")
    except Exception as e:
        print(f"发生其他错误: {e}")
    return None


def _process_data(table):
    if table:
        data = table[0].df
        print(data)


if __name__ == "__main__":
    url = "https://licai-oss-bucket-1301073378.cos.ap-guangzhou.myqcloud.com/Financial_announcement_tmp/cqrcb/渝农商理财江渝财富天添金益进封闭式2021年第51007期理财产品发行公告_21GSGF51007_2021-05-25_SeT8c7kSsg.pdf"
    data = _extract_table(url)