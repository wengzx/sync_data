# -*- coding: utf-8 -*-
import pymysql
import streamlit as st
import logging

import os
import mysql.connector
from mysql.connector import errorcode
from pandas.core.interchange.from_dataframe import primitive_column_to_ndarray


from tqdm import tqdm

import datetime

from pymysql import converters, FIELD_TYPE  #处理特殊函数


conv = converters.conversions
conv[FIELD_TYPE.NEWDECIMAL] = float  # convert decimals to float
conv[FIELD_TYPE.DATE] = str  # convert dates to strings
conv[FIELD_TYPE.TIMESTAMP] = str  # convert dates to strings
conv[FIELD_TYPE.DATETIME] = str  # convert dates to strings
conv[FIELD_TYPE.TIME] = str  # convert dates to strings

# 设置页面标题
st.set_page_config(
        page_title="数据同步平台",
)

# 定义多页面
class MultiApp:

    def __init__(self):
        self.apps = []
        self.app_dict = {}


    def add_app(self, title, func):
        if title not in self.apps:
            self.apps.append(title)
            self.app_dict[title] = func

    def run(self):
        title = st.sidebar.radio(
            # 'Go To',
            '同步数据',
            self.apps,
            format_func=lambda title: str(title))
        self.app_dict[title]()




def UI_test():
    st.title("UI测试")


# 初始化日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)



# 数据库同步类
class DataSyncPlatform:

    def __init__(self):
        # self.config = config
        self.source_conn = None
        self.target_conn = None

        self.source_cursor = None
        self.target_cursor = None


        # self._conn = self.connect_databases(shujuku_name)
        # if (self._conn):
        #     print("链接服务器成功")


    def connect_databases(self,shujuku_name) :
        """连接源数据库和目标数据库"""
        print(shujuku_name)

        try:
            self.source_conn = pymysql.connect(
                # 正式环境数据库
                host='zhidu1.rwlb.rds.aliyuncs.com',  # 连接主机, 默认127.0.0.1
                user='re_puchen_pro_zhidu',  # 用户名
                passwd='ErbLtQhD5gloHwj',  # 密码
                port=3306,  # 端口，默认为3306
                db=shujuku_name,  # 数据库名称
                charset='utf8',  # 字符编码
                conv = conv
            )
            # 生成游标对象 cursor
            self.source_cursor = self.source_conn.cursor()
            print(self.source_cursor)

            # 测试库连接
            self.target_conn = pymysql.connect(
                # 测试环境数据库
                host = "rm-wz91136429opn0crwwo.mysql.rds.aliyuncs.com",
                port =3306 ,
                user = "re_puchen_test",
                password ="R40VeJGkg7yE35k",
                database =shujuku_name,
                charset = 'utf8',
                conv = conv

            )
            self.target_cursor = self.target_conn.cursor()
            print("打印游标对象")
            print(self.target_cursor)

            logger.info("数据库连接成功")


        except mysql.connector.Error as err:
            logger.error(f"数据库连接失败: {err}")
            # current_sync["message"] = f"数据库连接失败: {err}"
            # current_sync["status"] = SyncStatus.ERROR
        else:
            return True


    def get_table_columns(self, table_name):

        print("获取表的列信息")
        """获取表的列信息"""

        aa=[]

        self.source_cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        pp=self.source_cursor.fetchall()
        # pp=[col]
        for col in pp:
            # print(col[0])
            aa.append(col[0])  #将字段推进列表
        print("打印表字段")
        print(aa)
        # op=[col[0] for col in self.source_cursor.description]
        # print(op)
        return aa

    def prepare_target_table(self, shujuku_name,table_name):
        """准备目标表（清空或重建）"""
        try:
            # 清空表
            self.target_cursor.execute(f"TRUNCATE TABLE {shujuku_name}.{table_name}")
            print("已清空目标表")
            logger.info(f"已清空目标表: {shujuku_name}.{table_name}")
        except Exception:
            print("清空目标表失败")


    #增量同步数据
    def sync_data(self,shujuku_name,table_name,sync_method,sku=None):
        """同步单个表数据"""
        print(shujuku_name)
        print(table_name)
        print(sync_method)
        print(sku)

        # 判断是否存在数据库里面
        self.source_cursor.execute("show tables")
        table_list = [tuple[0] for tuple in self.source_cursor.fetchall()]
        print(table_list)
        if table_name not in table_list:
            print(f"{shujuku_name}数据库没有{table_name}这张表")
            message={f"{shujuku_name}数据库没有{table_name}这张表"}

            return message

        logger.info(f"开始同步数据库: {shujuku_name} ")
        logger.info(f"开始同步表: {table_name} ")

        # 获取列信息
        columns = self.get_table_columns(table_name)
        y=0
        for c in columns:
            c = "`" + str(c) + "`"
            columns[y] = c  # 改变元数据
            print(c)
            y = y + 1
        columns = tuple(columns)
        print(columns)

        where_clause = ""
        # 如果增量同步处理方法
        if sync_method=="增量同步":
            where_clause = ""
            self.target_cursor.execute(f"SELECT MAX(id) FROM {shujuku_name}.{table_name}")
            last_value = self.target_cursor.fetchone()[0]
            if last_value:
                where_clause = f"WHERE id > '{last_value}' "

            if sku :
                where_clause = f"WHERE local_sku='{sku}'"
            print("查询条件："+where_clause)

        print(where_clause)
        # 计算源数据总数
        self.source_cursor.execute(f"SELECT COUNT(*) FROM {shujuku_name}.{table_name} {where_clause}")
        total_rows = self.source_cursor.fetchone()[0]
        print("打印总条数:"+str(total_rows))



        # 如果数据总数为0
        if total_rows == 0:
            logger.info(f"表 {shujuku_name}{table_name} 没有需要同步的数据")
            # current_sync["log"].append(f"表 {table_name} 没有需要同步的数据")
            # current_sync["completed_tables"] += 1
            messgae=f"表 {shujuku_name}{table_name} 没有需要同步的数据"
            return messgae

        # 准备目标表
        if sync_method == '全量同步':
            self.prepare_target_table(shujuku_name,table_name)

        # 分批读取和插入
        offset = 0 #偏移量
        inserted_count = 0
        updated_count = 0

        # 进度条
        pbar = tqdm(total=total_rows, desc=f"同步 {table_name}", unit="行")

        batch_size=10 #分多少插入数据

        # 执行分段插入数据
        while offset < total_rows:
            print("开始下一批数据")
            self.source_cursor.execute(
                f"SELECT * FROM {table_name} {where_clause} "
                f"LIMIT {batch_size} OFFSET {offset}"
            )

            batch = self.source_cursor.fetchall()

            for row in batch:

                processed_row = list(row)  #将元数据暂时转化成列表
                print("11111")
                print(processed_row)

                # 处理时间
                key=0
                for value in processed_row:

                    if isinstance(value, datetime.datetime):
                        value = value.strftime('%Y-%m-%d %H:%M:%S')


                    processed_row[key]=value #改变元数据

                    key=key+1  #索引加1
                    # print(key)
                # print("打印修改后的元数据")
                # print(processed_row)



                # 构建插入语句
                processed_row=tuple(processed_row)
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                update_str = ', '.join([f"{col}=VALUES({col})" for col in columns])
                print(update_str)

                print("打印插入字段")
                print(columns_str)



                query = (
                    f"INSERT INTO {shujuku_name}.{table_name} ({columns_str})"
                    f"VALUES {processed_row}"
                    f"ON DUPLICATE KEY UPDATE {update_str}"
                )
                special_query = (
                    f"INSERT INTO {shujuku_name}.{table_name} ({columns_str})"
                    f"VALUES {processed_row}"

                )
                query = query.replace("'None'", "NULL").replace("None", "NULL")
                special_query = query.replace("'None'", "NULL").replace("None", "NULL")
                print("打印执行sql语句")
                print(query)


                # 执行插入/更新
                try:
                    if self.target_cursor.execute(query):
                       print("执行插入成功")
                       message="执行插入成功"

                    else:
                        self.target_cursor.execute(special_query)


                    if self.target_cursor.rowcount == 1:
                        inserted_count += 1
                    else:
                        updated_count += 1

                except mysql.connector.Error as err:
                    logger.error(f"数据插入失败: {err}\nQuery: {query}")


            self.target_conn.commit()   #执行sql语句
            offset += batch_size        #增加偏移量执行下一部分
            pbar.update(len(batch))     #更细进度条

        pbar.close()
        self.source_cursor.close()
        self.target_cursor.close()
        logger.info(
            f"表 {table_name} 同步完成: "
            f"新增 {inserted_count} 行, 更新 {updated_count} 行, "
            f"总计 {inserted_count + updated_count} 行"
        )
        message={f"表 {table_name} 同步完成: "
            f"新增 {inserted_count} 行, 更新 {updated_count} 行, "
            f"总计 {inserted_count + updated_count} 行"}

        return message


def sync_table():
    """同步单个表数据"""
    # table_name = table_config['name']
    # batch_size = table_config.get('batch_size', 1000)
    # sync_mode = table_config.get('mode', 'full')

    # logger.info(f"开始同步表: {table_name} [{sync_mode}模式]")

    # 获取列信息
    # columns = get_table_columns(table_name)
    # sensitive_fields = table_config.get('sensitive_fields', {})
    # print(columns)
    # print(sensitive_fields)



    form = st.form("my_form")

    shujuku_name = form.selectbox("选择需要同步的数据库", ['amazon_sp_data', 'lingxing_data','spider','xt_business_data','xt_user_data', ],  # 也可以用元组
                            index=1)
    table_name = form.text_input('请输入需要同步的表名', max_chars=100, help='最大长度为100字符')

    sku = form.text_input('请输入指定的sku', max_chars=100, help='最大长度为100字符')

    sync_method=form.selectbox("选择同步方式",['增量同步', '全量同步', ],  #也可以用元组
                               index = 1)
    nn = form.form_submit_button("执行同步")

    if nn:
        st.write("开始同步数据了")  #打印提示
        aa=DataSyncPlatform()
        c=aa.connect_databases(shujuku_name)
        print(c)

        mm=aa.sync_data(shujuku_name,table_name,sync_method,sku)
        st.write(mm)



app = MultiApp()    #实例化
app.add_app("同步数据", sync_table)

app.run() #运行服务