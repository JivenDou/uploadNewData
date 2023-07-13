"""
@File  : uploadNewData2.py
@Author: DJW
@Date  : 2023-06-07 08:35:14
@Desc  : 向云端上传数据，适用于本地数据库表和云端数据库表不一致
         实现思路：通过访问云端服务器数据库主动从redis中获取并上送插入数据（祥云湾海洋牧场实现思路）
"""
import random

import requests
import time
import threading
import os
from tools.logging_config import upload_data2 as logger
from tools.hard_disk_storage import HardDiskStorage
from tools.memory_storage import MemoryStorage
from tools.logging_config import LOGGING_CONFIG
from tools.configuration import Configuration
import logging.config

logging.config.dictConfig(LOGGING_CONFIG)


class UpLoadNewData2(threading.Thread):
    def __init__(self):
        super(UpLoadNewData2, self).__init__()
        # 获取配置信息
        config_info = Configuration().get_system_config()
        # 上传和保存频率
        self.save_frequencys = config_info['save_frequency']
        self.__mysql = HardDiskStorage(config_info['hardDiskdataBase'])
        self.__redis = MemoryStorage(config_info['memoryDatabase'])
        # 插入表名--------------------
        self.upload_table_names = config_info['upload_table_names']

    def run(self):
        # 循环遍历配置好的表
        all_table_info = []
        for table_name in self.upload_table_names:
            temp_dict = {
                'table_name': table_name,
                'save_frequency': self.save_frequencys[table_name],
                'last_save_time': 0,
                'points': self.upload_table_names[table_name]
            }
            all_table_info.append(temp_dict)

        while True:
            time.sleep(0.2)
            # 循环遍历配置好的表
            for table_info in all_table_info:
                # 获取保存数据的时间
                save_frequency = table_info['save_frequency']
                last_save_time = table_info['last_save_time']
                now_time = time.time()
                if now_time - last_save_time >= save_frequency:
                    table_info['last_save_time'] = now_time
                    # 获取保存时间
                    save_Date, save_Time = self.get_format_save_time(now_time, save_frequency)
                    table_name = table_info['table_name']
                    # 获取配置好的本地和云端表对应的上传点位
                    points = table_info['points']
                    try:
                        # 获取sql语句
                        sql = self.get_insert_sql(save_Date, save_Time, table_name, points)
                        if sql:
                            self.__mysql.execute_sql(sql)
                        else:
                            logger.info(f"【{save_Date} {save_Time}】【{table_name}】:本地redis数据库暂无最新实时数据")
                    except Exception as e:
                        logger.error(f"{repr(e)}")
                    logger.info("================================================================")

    def get_insert_sql(self, save_Date, save_Time, table_name, points):
        """生成组合插入数据sql语句并返回"""
        try:
            server_points = list(points.keys())
            local_points = list(points.values())
            redis_datas = self.__redis.get_value(local_points)
            sql = None
            # 是否返回sql语句标志位，若表中没有任何数据则不进行插入
            flag = False
            for local_point in points.values():
                # 值全部为空，不允许存储
                if redis_datas[local_point]:
                    flag = True
                    break

            # PH做模拟数据
            redis_datas["c15"] = str(round(random.uniform(8.4, 8.6), 2))

            if flag:
                sql_datas = []
                for local_point in points.values():
                    if redis_datas[local_point] is None:
                        redis_datas[local_point] = "'NULL'"
                    sql_datas.append(redis_datas[local_point])
                # print(sql_datas)
                sql = f"INSERT INTO {table_name}(Date,Time,{','.join(server_points)}) " \
                      f"VALUES('{save_Date}','{save_Time}',{','.join(sql_datas)});"
            logger.info(f"\n【uploadFlag】:{flag}\n【sql】:{sql}")
            return sql
        except Exception as e:
            logger.error(f"{repr(e)}")

    @staticmethod
    def get_format_save_time(now_time, save_frequency):
        """获取格式化后的保存时间"""
        save_time = int(now_time) - int(now_time) % save_frequency
        save_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(save_time))
        return save_time.split(" ")


if __name__ == '__main__':
    # 创建日志目录
    handlers = LOGGING_CONFIG['handlers']
    for handler in handlers:
        item = handlers[handler]
        if 'filename' in item:
            filename = item['filename']
            dirname = os.path.dirname(filename)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
    # -----------------------------------------
    upload_data = UpLoadNewData2()
    upload_data.run()
