"""
@File  : uploadNewData.py
@Author: DJW
@Date  : 2023-03-29 14:35:14
@Desc  : 向云端上传数据，适用于本地数据库表和云端数据库表一致的情况
         实现思路：通过遍历循环查询各个配置好的表的最后一条，发送到服务器uploadNewData.php接口，接口文件同样需要配置好才能上传数据
"""
import requests
import time
import threading
import os
from tools.logging_config import upload_data as logger
from tools.hard_disk_storage import HardDiskStorage
from tools.logging_config import LOGGING_CONFIG
from tools.configuration import Configuration
from geopy.distance import geodesic
import logging.config

logging.config.dictConfig(LOGGING_CONFIG)


class UpLoadNewData(threading.Thread):
    def __init__(self):
        super(UpLoadNewData, self).__init__()
        # 获取配置信息
        config_info = Configuration().get_system_config()
        self.__config = config_info['hardDiskdataBase']
        self._mysql = HardDiskStorage(self.__config)
        self.post_url = config_info['post_url']
        # 查询表名(ais需要特殊处理)--------------------
        self.table_names = eval(config_info['table_names'])
        self.table_name = None
        # 平台坐标
        self.__center = eval(config_info['center'])

    def run(self):
        while True:
            try:
                # 获取、发送、更新数据--------------------
                data_null_flag = 0
                # 遍历站名对应的历史表
                for table_name in self.table_names:
                    self.table_name = table_name
                    sql = f"SELECT * FROM {self.table_name} WHERE is_send = 0 LIMIT 1"
                    data = self._mysql.execute_sql(sql)
                    # # 判空--------------------
                    if not data:
                        # 若为空则继续遍历下一个表
                        if data_null_flag < len(self.table_names) - 1:
                            logger.info(f'{self.table_name}查询数据为0')
                            data_null_flag += 1
                            time.sleep(0.001)
                            continue
                        else:
                            # 若遍历完所有表都为空，则说明数据没有更新了，则等待10秒，避免无意义的查询
                            logger.info(f'{self.table_name}查询数据为0')
                            time.sleep(10)
                            break
                    else:  # 有数据则进行处理上传
                        try:
                            # 格式化发送数据--------------------
                            data = data[0]
                            data['times'] = str(data['times'])
                            del data['is_send']
                            # 若是上传ais实时数据，则需要计算船到平台的距离
                            if self.table_name == "ais_data":
                                data['distance'] = self.get_distance(data)
                            # 查询的数据若为空值，则置NULL
                            for k in data:
                                if data[k] is None:
                                    data[k] = 'NULL'
                            # 发送数据--------------------
                            data['table_name'] = self.table_name
                            self.send_data(data)
                        except Exception as e:
                            logger.error(f"{repr(e)}")
                        time.sleep(0.001)
                # 0.01秒上传一组（遍历一遍所有表的最后一条为一组）
                time.sleep(0.01)
                # time.sleep(2)
            except Exception as e:
                logger.error(f"{repr(e)}")

    def send_data(self, data):
        """发送数据"""
        try:
            # 开始发送数据
            data_id = data['id']
            # print(data)
            # verify=False避免ssl认证
            ret = requests.post(url=self.post_url, json=data, verify=False, timeout=2)
            if ret.status_code == 200:
                logger.info(f'**{self.table_name}** {ret.text} {ret.status_code}')
                # 更新数据
                # print(f"更新 {self.table_name} 数据")
                sql = f"UPDATE {self.table_name} SET is_send=1 WHERE is_send=0 AND id={data_id}"
                self._mysql.execute_sql(sql)
            else:
                logger.error(f'{self.table_name} {ret.text} {ret.status_code} {data}')
        except Exception as e:
            logger.error(f"{repr(e)}")

    def get_distance(self, data):
        """
        获取平台到船只的距离
        :param  data：ais实时数据字典  {'id': 13, 'times': '2022-12-21 13:58:56', 'mmsi': '999994002', 'shipname': 'ATON 4002', 'lon': 181.0, 'lat': 91.0, 'speed': 'NULL', 'course': 'NULL', 'heading': 'NULL', 'status': 'NULL', 'callsign': 'NULL', 'destination': 'NULL', 'shiptype': 'NULL', 'table_name': 'ais_data'}
        :return distance：平台到船只的距离
        """
        # 平台坐标
        center = self.__center
        heading = data['heading']
        lon = data['lon']
        lat = data['lat']
        # 排除错误坐标
        if lon and lat and -180 < lon < 180 and -90 < lat < 90:
            if heading is not None:
                heading = float(heading)
                if heading > 360:
                    heading = None
            data['heading'] = heading
            distance = geodesic(center, (data['lat'], data['lon'])).kilometers
            return distance
        else:
            return None


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
    upload_data = UpLoadNewData()
    upload_data.run()
