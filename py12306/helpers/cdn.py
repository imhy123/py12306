import random
import json
from datetime import timedelta
from os import path

from py12306.cluster.cluster import Cluster
from py12306.config import Config
from py12306.app import app_available_check
from py12306.helpers.api import API_CHECK_CDN_AVAILABLE, HOST_URL_OF_12306
from py12306.helpers.func import *
from py12306.helpers.request import Request
from py12306.log.common_log import CommonLog

class CdnItem:
    """
    对单个CDN服务器的抽象
    单个CDN服务器的权重： 成功率 * 成功率权重 + (1000ms - 最近10次平均延迟) / 1000ms * 延迟权重
    """
    SUCCESS_RATE_WEIGHT = 10000  # 成功率权重
    BASE_DELAY_MS = 1000  # 基准延迟
    AVG_DELAY_WEIGHT = 1000  # 延迟权重

    MAX_DELAY_DATA = 10 # 记录最近10次的延迟

    def __init__(self, host):
        self.host = host
        self.list_index = None

        self.request_count = 0
        self.success_count = 0
        self.delay_data = []  # 最近10次的延迟记录
        self.delay_count = 0
        self.delay_rate_score = 0 # 记录延迟得分，失败时不需要重复计算

        self.weight = 0

    def add_request_result(self, delay, success = True):
        self.request_count += 1

        if success:
            self.success_count += 1

            if len(self.delay_data) >= self.MAX_DELAY_DATA:
                x = self.delay_data.pop(0)
                self.delay_count -= x

            self.delay_count += delay
            self.delay_data.append(delay)

            avg_delay = self.delay_count / len(self.delay_data)

            self.delay_rate_score = (self.BASE_DELAY_MS - avg_delay) / self.BASE_DELAY_MS
            self.delay_rate_score = self.delay_rate_score if self.delay_rate_score > 0 else 0

        success_rate = self.success_count / self.request_count

        self.weight = success_rate  * self.SUCCESS_RATE_WEIGHT + self.delay_rate_score * self.AVG_DELAY_WEIGHT


class CdnItemEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, CdnItem):
            return {'host': obj.host, 'request_count': obj.request_count, 'success_count': obj.success_count, 'weight': obj.weight}
        return json.JSONEncoder.default(self, obj)

@singleton
class Cdn:
    """
    CDN 管理
    """
    items = []
    is_ready = False
    is_alive = True

    safe_stay_time = 0.2
    retry_num = 1
    thread_num = 5
    check_time_out = 3

    lock = threading.Lock()
    cdn_list = []
    cdn_map = {}

    def __init__(self):
        self.cluster = Cluster()
        self.init_config()

    def init_config(self):
        self.check_time_out = Config().CDN_CHECK_TIME_OUT

    def init_data(self):
        self.items = []
        self.is_ready = False

    def destroy(self):
        """
        关闭 CDN
        :return:
        """
        CommonLog.add_quick_log(CommonLog.MESSAGE_CDN_CLOSED).flush()
        self.is_alive = False
        self.init_data()

    def update_cdn_status(self, auto=False):
        if auto:
            self.init_config()
            if Config().is_cdn_enabled():
                self.run()
            else:
                self.destroy()

    @classmethod
    def run(cls):
        self = cls()
        app_available_check()
        self.is_alive = True
        self.start()
        pass

    def start(self):
        if not Config.is_cdn_enabled(): return
        self.load_items()
        CommonLog.add_quick_log(CommonLog.MESSAGE_CDN_START_TO_CHECK.format(len(self.items))).flush()
        self.restore_items()

        self.is_ready = True

        create_thread_and_run(self, 'watch_cdn', False)
        # for i in range(self.thread_num):  # 多线程
        #     print("[%s] start create thread" % threading.current_thread().name)
        #     create_thread_and_run(jobs=self, callback_name='check_available', wait=False)

    def load_items(self):
        with open(Config().CDN_ITEM_FILE, encoding='utf-8') as f:
            for line, val in enumerate(f):
                host = val.rstrip('\n')

                cdn_item = CdnItem(host)
                cdn_item.list_index = len(self.cdn_list)

                self.cdn_map[host] = cdn_item
                self.cdn_list.append(cdn_item)

                self.items.append(host)

    def restore_items(self):
        """
        恢复已有数据
        :return: bool
        """
        result = False
        if path.exists(Config().CDN_ENABLED_AVAILABLE_ITEM_FILE):
            with open(Config().CDN_ENABLED_AVAILABLE_ITEM_FILE, encoding='utf-8') as f:
                result = f.read()
                try:
                    result = json.loads(result)
                except json.JSONDecodeError as e:
                    result = {}

        if result:
            version = result.get('version', 1)
            if version <= 1:
                CommonLog.add_quick_log(CommonLog.MESSAGE_CDN_VERSION_OUTDATED).flush()
                return False

            self.last_check_at = result.get('last_check_at', '')
            if self.last_check_at:
                self.last_check_at = str_to_time(self.last_check_at)

            restore_cdn_list = result.get('cdn_list', [])
            for x in restore_cdn_list:
                cdn_item = self.cdn_map[x.get('host')]
                cdn_item.request_count = x.get('request_count')
                cdn_item.success_count = x.get('success_count')
                cdn_item.weight = x.get('weight')

            self.resort_cdn_list()

            CommonLog.add_quick_log(CommonLog.MESSAGE_CDN_RESTORE_SUCCESS.format(self.last_check_at )).flush()
            return True

        return False

    def resort_cdn_list(self):
        """
        对cdn_list进行全部重排序，仅用于从文件中恢复时的情况
        """
        self.cdn_list.sort(key=lambda x: x.weight, reverse=True)
        for i in range(len(self.cdn_list)):
            self.cdn_list[i].list_index = i

    def save_available_items(self):       
        self.last_check_at = time_now()
        data = {'version': 2,  'last_check_at': str(self.last_check_at),
                'cdn_list': self.cdn_list}

        with open(Config().CDN_ENABLED_AVAILABLE_ITEM_FILE, 'w') as f:
            f.write(json.dumps(data, cls=CdnItemEncoder))

    def watch_cdn(self):
        """
        监控 cdn 状态，自动重新检测
        :return:
        """
        while True:
            stay_second(600)

            if self.is_alive:  # 重新检测
                print("开始保存CDN信息")

                self.save_available_items()
            
    @classmethod
    def report_cdn_result(cls, host, delay, success = True):
        self = cls()
        with self.lock:
            cdn_item = self.cdn_map[host]
            if not cdn_item:
                print("%s cdn not found!" % host)
                return

            old_weight = cdn_item.weight

            cdn_item.add_request_result(delay, success)

            new_weight = cdn_item.weight

            # 冒泡排序
            if cdn_item.list_index > 0 and new_weight > self.cdn_list[cdn_item.list_index - 1].weight:
                for i in range(cdn_item.list_index - 1, -1, -1):
                    if new_weight > self.cdn_list[i].weight:
                        self.cdn_list[i].list_index = i + 1
                        self.cdn_list[i + 1] = self.cdn_list[i]
                        self.cdn_list[i] = cdn_item
                    else:
                        break
            else:
                if cdn_item.list_index < len(self.cdn_list) -1 and new_weight <= self.cdn_list[cdn_item.list_index + 1].weight:
                    for i in range(cdn_item.list_index + 1, len(self.cdn_list)):
                        if new_weight <= self.cdn_list[i].weight:
                            self.cdn_list[i].list_index = i - 1
                            self.cdn_list[i - 1] = self.cdn_list[i]
                            self.cdn_list[i] = cdn_item
                        else:
                            break     

    @classmethod
    def get_cdn(cls):
        self = cls()
        # if self.is_ready and self.available_items:
        #     return random.choice(self.available_items)
        # return None
        random_cdn = random.choice(self.cdn_list[0:100])
        return random_cdn.host


if __name__ == '__main__':
    # Const.IS_TEST = True
    Cdn.run()