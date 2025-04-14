import pymysql
from typing import List, Dict, Any, Optional

class MySQLConnector:

    def __init__(
        self,
        host: str = "gz-cynosdbmysql-grp-p1gmpvp3.sql.tencentcdb.com",
        port: int = 21844,
        user: str = "ly",
        password: str = "7qc04tTU",
        database: str = "files_library_tmp",
        charset: str = "utf8mb4"
    ):
    # def __init__(
    #         self,
    #         host: str = "localhost",
    #         port: int = 3306,
    #         user: str = "root",
    #         password: str = "123456",
    #         database: str = "testdb2",
    #         charset: str = "utf8mb4"
    # ):
        """
        初始化MySQL连接参数
        :param host: 数据库地址
        :param port: 端口 (默认3306)
        :param user: 用户名
        :param password: 密码
        :param database: 数据库名
        :param charset: 字符编码 (默认utf8mb4)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.connection = None

    def connect(self) -> None:
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor  # 返回字典格式结果
            )
            print("数据库连接成功")
        except pymysql.Error as e:
            print(f"数据库连接失败: {e}")
            raise

    def execute_query(
        self,
        sql: str,
        params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        执行查询语句
        :param sql: SQL查询语句
        :param params: 参数化查询的参数
        :return: 查询结果列表(每行是字典)
        """
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                result = cursor.fetchall()
                return result
        except pymysql.Error as e:
            print(f"查询执行失败: {e}")
            raise

    def execute_update(
        self,
        sql: str,
        params: Optional[tuple] = None
    ) -> int:
        """
        执行更新/插入/删除操作
        :param sql: SQL语句
        :param params: 参数化查询的参数
        :return: 受影响的行数
        """
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                affected_rows = cursor.execute(sql, params or ())
                self.connection.commit()
                return affected_rows
        except pymysql.Error as e:
            self.connection.rollback()
            print(f"操作执行失败: {e}")
            raise

    def close(self) -> None:
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            print("MySQL连接已关闭")

    def __enter__(self):
        """支持with上下文管理器"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出with时自动关闭连接"""
        self.close()
