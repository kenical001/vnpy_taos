from datetime import datetime
from collections.abc import Callable

import taos
import pandas as pd

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, MainContract
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DB_TZ,
)
from vnpy.trader.setting import SETTINGS

from .taos_script import (
    CREATE_DATABASE_SCRIPT,
    CREATE_BAR_TABLE_SCRIPT,
    CREATE_TICK_TABLE_SCRIPT,
    CREATE_MAIN_CONTRACT_TABLE_SCRIPT,
)


class TaosDatabase(BaseDatabase):
    """TDengine数据库接口"""

    def __init__(self) -> None:
        """构造函数"""
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]
        self.timezone: str = SETTINGS["database.timezone"]
        self.database: str = SETTINGS["database.database"]

        # 连接数据库
        self.conn: taos.TaosConnection = taos.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            timezone=self.timezone
        )

        self.cursor: taos.TaosCursor = self.conn.cursor()

        # 初始化创建数据库和数据表
        self.cursor.execute(CREATE_DATABASE_SCRIPT.format(self.database))
        self.cursor.execute(f"use {self.database}")
        self.cursor.execute(CREATE_BAR_TABLE_SCRIPT)
        self.cursor.execute(CREATE_TICK_TABLE_SCRIPT)
        self.cursor.execute(CREATE_MAIN_CONTRACT_TABLE_SCRIPT)

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存k线数据"""
        # 缓存字段参数
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval = bar.interval

        count: int = 0
        table_name: str = "_".join(["bar", symbol.replace("-", "_"), exchange.value, interval.value])

        # 以超级表为模版创建表
        create_table_script: str = (
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "USING s_bar(symbol, exchange, interval_, count_) "
            f"TAGS('{symbol}', '{exchange.value}', '{interval.value}', '{count}')"
        )
        self.cursor.execute(create_table_script)

        # 写入k线数据
        self.insert_in_batch(table_name, bars, 1000)

        # 查询汇总信息
        self.cursor.execute(f"SELECT start_time, end_time, count_ FROM {table_name}")
        results: list[tuple] = self.cursor.fetchall()

        overview: tuple = results[0]
        overview_start: datetime = overview[0]
        overview_end: datetime = overview[1]
        overview_count: int = int(overview[2])

        # 没有该合约
        if not overview_count:
            overview_start = bars[0].datetime
            overview_end = bars[-1].datetime
            overview_count = len(bars)
        # 已有该合约
        elif stream:
            overview_end = bars[-1].datetime
            overview_count += len(bars)
        else:
            overview_start = min(overview_start, bars[0].datetime)
            overview_end = max(overview_end, bars[-1].datetime)

            self.cursor.execute(f"select count(*) from {table_name}")
            results = self.cursor.fetchall()

            bar_count: int = int(results[0][0])
            overview_count = bar_count

        # 更新汇总信息
        self.cursor.execute(f"ALTER TABLE {table_name} SET TAG start_time='{overview_start}';")
        self.cursor.execute(f"ALTER TABLE {table_name} SET TAG end_time='{overview_end}';")
        self.cursor.execute(f"ALTER TABLE {table_name} SET TAG count_='{overview_count}';")

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存tick数据"""
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange

        count: int = 0
        table_name: str = "_".join(["tick", symbol.replace("-", "_"), exchange.value])

        # 以超级表为模版创建表
        create_table_script: str = (
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "USING s_tick(symbol, exchange, count_) "
            f"TAGS ( '{symbol}', '{exchange.value}', '{count}')"
        )
        self.cursor.execute(create_table_script)

        # 写入tick数据
        self.insert_in_batch(table_name, ticks, 1000)

        # 查询汇总信息
        self.cursor.execute(f"SELECT start_time, end_time, count_ FROM {table_name}")
        results: list[tuple] = self.cursor.fetchall()

        overview: tuple = results[0]
        overview_start: datetime = overview[0]
        overview_end: datetime = overview[1]
        overview_count: int = int(overview[2])

        # 没有该合约
        if not overview_count:
            overview_start = ticks[0].datetime
            overview_end = ticks[-1].datetime
            overview_count = len(ticks)
        # 已有该合约
        elif stream:
            overview_end = ticks[-1].datetime
            overview_count += len(ticks)
        else:
            overview_start = min(overview_start, ticks[0].datetime)
            overview_end = max(overview_end, ticks[-1].datetime)

            self.cursor.execute(f"select count(*) from {table_name}")
            results = self.cursor.fetchall()

            tick_count: int = int(results[0][0])
            overview_count = tick_count

        # 更新汇总信息
        self.cursor.execute(f"ALTER TABLE {table_name} SET TAG start_time='{overview_start}';")
        self.cursor.execute(f"ALTER TABLE {table_name} SET TAG end_time='{overview_end}';")
        self.cursor.execute(f"ALTER TABLE {table_name} SET TAG count_='{overview_count}';")

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        # 生成数据表名
        table_name: str = "_".join(["bar", symbol.replace("-", "_"), exchange.value, interval.value])

        # 从数据库读取数据
        # df: pd.DataFrame = pd.read_sql(f"select *, interval_ from {table_name} WHERE datetime BETWEEN '{start}' AND '{end}'", self.conn)

        # # 返回BarData列表
        # bars: list[BarData] = []

        # for row in df.itertuples():
        #     bar: BarData = BarData(
        #         symbol=symbol,
        #         exchange=exchange,
        #         datetime=row.datetime.astimezone(DB_TZ),
        #         interval=Interval(row.interval_),
        #         volume=row.volume,
        #         turnover=row.turnover,
        #         open_interest=row.open_interest,
        #         open_price=row.open_price,
        #         high_price=row.high_price,
        #         low_price=row.low_price,
        #         close_price=row.close_price,
        #         gateway_name="DB"
        #     )
        #     bars.append(bar)

        sql = f"""
            SELECT 
                datetime,
                volume,
                turnover,
                open_interest,
                open_price,
                high_price,
                low_price,
                close_price
            FROM {table_name}
            WHERE 
                datetime BETWEEN '{start.strftime("%Y-%m-%d %H:%M:%S")}' 
                AND '{end.strftime("%Y-%m-%d %H:%M:%S")}'
        """

         # 执行原生TDengine查询
        result = self.conn.query(sql)
        
        # 处理结果集
        bars: list[BarData] = []
        for row in result:
            # 转换时区（假设原始数据存储为UTC）
            # db_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            db_time = row[0].astimezone(DB_TZ)
            
            # 构建BarData对象
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=db_time,
                interval=interval,
                volume=row[1],
                turnover=row[2],
                open_interest=row[3],
                open_price=row[4],
                high_price=row[5],
                low_price=row[6],
                close_price=row[7],
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """读取tick数据"""
        # 生成数据表名
        table_name: str = "_".join(["tick", symbol.replace("-", "_"), exchange.value])

        # 从数据库读取数据
        df: pd.DataFrame = pd.read_sql(f"select * from {table_name} WHERE datetime BETWEEN '{start}' AND '{end}'", self.conn)

        # 返回TickData列表
        ticks: list[TickData] = []

        for row in df.itertuples():
            tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=row.datetime.astimezone(DB_TZ),
                name=row.name,
                volume=row.volume,
                turnover=row.turnover,
                open_interest=row.open_interest,
                last_price=row.last_price,
                limit_up=row.limit_up,
                limit_down=row.limit_down,
                open_price=row.open_price,
                high_price=row.high_price,
                low_price=row.last_price,
                pre_close=row.pre_close,
                bid_price_1=row.bid_price_1,
                bid_price_2=row.bid_price_2,
                bid_price_3=row.bid_price_3,
                bid_price_4=row.bid_price_4,
                bid_price_5=row.bid_price_5,
                ask_price_1=row.ask_price_1,
                ask_price_2=row.ask_price_2,
                ask_price_3=row.ask_price_3,
                ask_price_4=row.ask_price_4,
                ask_price_5=row.ask_price_5,
                bid_volume_1=row.bid_volume_1,
                bid_volume_2=row.bid_volume_2,
                bid_volume_3=row.bid_volume_3,
                bid_volume_4=row.bid_volume_4,
                bid_volume_5=row.bid_volume_5,
                ask_volume_1=row.ask_volume_1,
                ask_volume_2=row.ask_volume_2,
                ask_volume_3=row.ask_volume_3,
                ask_volume_4=row.ask_volume_4,
                ask_volume_5=row.ask_volume_5,
                localtime=row.localtime,
                gateway_name="DB"
            )
            ticks.append(tick)

        return ticks

    def load_last_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> TickData:
        """读取区间最近的tick数据"""
        # 生成数据表名
        table_name: str = "_".join(["tick", symbol.replace("-", "_"), exchange.value])

        sql = f"""
            SELECT 
                datetime,
                name,
                volume,
                turnover,
                open_interest,
                last_price,
                limit_up,
                limit_down,
                open_price,
                high_price,
                low_price,
                pre_close,
                bid_price_1,
                bid_price_2,
                bid_price_3,
                bid_price_4,
                bid_price_5,
                ask_price_1,
                ask_price_2,
                ask_price_3,
                ask_price_4,
                ask_price_5,
                bid_volume_1,
                bid_volume_2,
                bid_volume_3,
                bid_volume_4,
                bid_volume_5,
                ask_volume_1,
                ask_volume_2,
                ask_volume_3,
                ask_volume_4,
                ask_volume_5,
                localtime
            FROM {table_name}
            WHERE 
                datetime BETWEEN '{start.strftime("%Y-%m-%d %H:%M:%S")}' 
                AND '{end.strftime("%Y-%m-%d %H:%M:%S")}' 
                ORDER BY datetime DESC 
                LIMIT 1
        """

        # 执行原生TDengine查询
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
            
        if not result:
            return None

        row = result[0]

        # 转换时区（假设原始数据存储为UTC）
        tick_time = row[0].astimezone(DB_TZ)
        local_time = row[32].astimezone(DB_TZ)
            
        tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=tick_time,
                name=row[1],
                volume=row[2],
                turnover=row[3],
                open_interest=row[4],
                last_price=row[5],
                limit_up=row[6],
                limit_down=row[7],
                open_price=row[8],
                high_price=row[9],
                low_price=row[10],
                pre_close=row[11],
                bid_price_1=row[12],
                bid_price_2=row[13],
                bid_price_3=row[14],
                bid_price_4=row[15],
                bid_price_5=row[16],
                ask_price_1=row[17],
                ask_price_2=row[18],
                ask_price_3=row[19],
                ask_price_4=row[20],
                ask_price_5=row[21],
                bid_volume_1=row[22],
                bid_volume_2=row[23],
                bid_volume_3=row[24],
                bid_volume_4=row[25],
                bid_volume_5=row[26],
                ask_volume_1=row[27],
                ask_volume_2=row[28],
                ask_volume_3=row[29],
                ask_volume_4=row[30],
                ask_volume_5=row[31],
                localtime=local_time,
                gateway_name="DB"
            )
        
        return tick

    def load_last_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> BarData:
        """读取区间最近的分钟K线数据"""
        # 生成数据表名
        table_name: str = "_".join(["bar", symbol.replace("-", "_"), exchange.value, interval.value])

        sql = f"""
            SELECT 
                datetime,
                volume,
                turnover,
                open_interest,
                open_price,
                high_price,
                low_price,
                close_price
            FROM {table_name}
            WHERE 
                datetime BETWEEN '{start.strftime("%Y-%m-%d %H:%M:%S")}' 
                AND '{end.strftime("%Y-%m-%d %H:%M:%S")}' 
                ORDER BY datetime DESC 
                LIMIT 1
        """

        # 执行原生TDengine查询
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
            
        if not result:
            return None

        row = result[0]

        # 转换时区（假设原始数据存储为UTC）
        bar_time = row[0].astimezone(DB_TZ)
            
        bar: BarData = BarData(
            symbol=symbol,
            exchange=exchange,
            datetime=bar_time,
            interval=interval,
            volume=row[1],
            turnover=row[2],
            open_interest=row[3],
            open_price=row[4],
            high_price=row[5],
            low_price=row[6],
            close_price=row[7],
            gateway_name="DB"
        )
        
        return bar

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        # 生成数据表名
        table_name: str = "_".join(["bar", symbol.replace("-", "_"), exchange.value, interval.value])

        # 查询数据条数
        self.cursor.execute(f"select count(*) from {table_name}")
        result: list = self.cursor.fetchall()
        count: int = int(result[0][0])

        # 执行K线删除
        self.cursor.execute(f"DROP TABLE {table_name}")

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除tick数据"""
        # 生成数据表名
        table_name: str = "_".join(["tick", symbol.replace("-", "_"), exchange.value])

        # 查询数据条数
        self.cursor.execute(f"select count(*) from {table_name}")
        result: list = self.cursor.fetchall()
        count: int = int(result[0][0])

        # 删除tick数据
        self.cursor.execute(f"DROP TABLE {table_name}")

        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """查询K线汇总信息"""
        # 从数据库读取数据
        df: pd.DataFrame = pd.read_sql("SELECT DISTINCT symbol, exchange, interval_, start_time, end_time, count_ FROM s_bar", self.conn)

        # 返回BarOverview列表
        overviews: list[BarOverview] = []

        for row in df.itertuples():
            overview: BarOverview = BarOverview(
                symbol=row.symbol,
                exchange=Exchange(row.exchange),
                interval=Interval(row.interval_),
                start=row.start_time.astimezone(DB_TZ),
                end=row.end_time.astimezone(DB_TZ),
                count=int(row.count_),
            )
            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """查询Tick汇总信息"""
        # 从数据库读取数据
        df: pd.DataFrame = pd.read_sql("SELECT DISTINCT symbol, exchange, start_time, end_time, count_ FROM s_tick", self.conn)

        # TickOverview
        overviews: list[TickOverview] = []

        for row in df.itertuples():
            overview: TickOverview = TickOverview(
                symbol=row.symbol,
                exchange=Exchange(row.exchange),
                start=row.start_time.astimezone(DB_TZ),
                end=row.end_time.astimezone(DB_TZ),
                count=int(row.count_),
            )
            overviews.append(overview)

        return overviews

    def save_main_contract_data(self, data: list[MainContract]) -> bool:
        """保存主力合约数据"""
        if not data:
            return False
        
        # 确保所有数据都是同一个品种和交易所
        product = data[0].product
        exchange = data[0].exchange
        
        # 检查所有数据的一致性
        for item in data:
            if item.product != product or item.exchange != exchange:
                print(f"数据不一致: {item.product}/{item.exchange.value} vs {product}/{exchange.value}")
                return False
            if not item.trade_date:
                print("数据缺少交易日期")
                return False
            
        # 生成表名
        table_name: str = f"main_contract_{product}"
        
        # 以超级表为模版创建表
        create_table_script: str = (
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "USING s_main_contract(product, exchange, start_date, end_date, count_) "
            f"TAGS('{product}', '{exchange.value}', NULL, NULL, '0')"
        )
        self.cursor.execute(create_table_script)
        
        # 写入主力合约数据
        data_values = []
        for item in data:
            trade_date = item.trade_date
            symbol = item.symbol
            
            value = f"('{trade_date}', '{symbol}')"
            data_values.append(value)
        
        # 批量插入数据
        if not data_values:
            return True
            
        batch_size = 1000
        try:
            for i in range(0, len(data_values), batch_size):
                batch = data_values[i:i+batch_size]
                insert_sql = f"INSERT INTO {table_name} (trade_date, symbol) VALUES {','.join(batch)}"
                self.cursor.execute(insert_sql)
        except Exception as e:
            print(f"批量插入主力合约数据失败: {e}")
            return False
        
        # 更新汇总信息
        if data:
            # 获取当前数据的时间范围
            new_start = min(item.trade_date for item in data)
            new_end = max(item.trade_date for item in data)
            new_count = len(data)
            
            # 查询现有的汇总信息
            self.cursor.execute(f"SELECT start_date, end_date, count_ FROM {table_name} LIMIT 1")
            result = self.cursor.fetchall()
            
            if result:
                # 合并新旧数据范围
                current_start, current_end, current_count = result[0]
                start_date = min(new_start, current_start) if current_start else new_start
                end_date = max(new_end, current_end) if current_end else new_end
                count = new_count + int(current_count) if current_count else new_count
            else:
                # 没有现有数据，使用新数据范围
                start_date = new_start
                end_date = new_end
                count = new_count
            
            # 更新汇总信息
            self.cursor.execute(f"ALTER TABLE {table_name} SET TAG start_date='{start_date}';")
            self.cursor.execute(f"ALTER TABLE {table_name} SET TAG end_date='{end_date}';")
            self.cursor.execute(f"ALTER TABLE {table_name} SET TAG count_='{count}';")
        
        return True
    
    def load_main_contract_data(self, product: str, exchange: Exchange, start: datetime, end: datetime) -> list[MainContract]:
        """读取主力合约数据"""
        # 生成表名
        table_name: str = f"main_contract_{product}"
        
        try:
            
            # 构建SQL查询主力合约数据
            data_sql = f"""
                SELECT 
                    trade_date,
                    symbol
                FROM {table_name}
                WHERE 
                    trade_date BETWEEN '{start.strftime("%Y-%m-%d")}' 
                    AND '{end.strftime("%Y-%m-%d")}'
                ORDER BY trade_date
            """
            
            # 执行查询
            data_result = self.conn.query(data_sql)
            
            # 处理结果
            data = []
            for row in data_result:
                main_contract = MainContract(
                    trade_date=row[0],
                    product=product,
                    symbol=row[1],
                    exchange=exchange
                )
                data.append(main_contract)
                
            return data
        except Exception as e:
            print(f"查询主力合约数据失败: {e}")
            return []

    def insert_in_batch(self, table_name: str, data_set: list, batch_size: int) -> None:
        """数据批量插入数据库"""
        if table_name.split("_")[0] == "bar":
            generate: Callable = generate_bar
        else:
            generate = generate_tick

        data: list[str] = [f"insert into {table_name} values"]
        count: int = 0

        for d in data_set:
            data.append(generate(d))
            count += 1

            if count == batch_size:
                self.cursor.execute(" ".join(data))

                data = [f"insert into {table_name} values"]
                count = 0

        if count != 0:
            self.cursor.execute(" ".join(data))


def generate_bar(bar: BarData) -> str:
    """将BarData转换为可存储的字符串"""
    result: str = (f"('{bar.datetime}', {bar.volume}, {bar.turnover}, {bar.open_interest},"
                   + f"{bar.open_price}, {bar.high_price}, {bar.low_price}, {bar.close_price})")

    return result


def generate_tick(tick: TickData) -> str:
    """将TickData转换为可存储的字符串"""
    # tick不带localtime
    if tick.localtime:
        localtime: datetime = tick.localtime
    # tick带localtime
    else:
        localtime = tick.datetime

    result: str = (f"('{tick.datetime}', '{tick.name}', {tick.volume}, {tick.turnover}, "
                   + f"{tick.open_interest}, {tick.last_price}, {tick.last_volume}, "
                   + f"{tick.limit_up}, {tick.limit_down}, {tick.open_price}, {tick.high_price}, {tick.low_price}, {tick.pre_close}, "
                   + f"{tick.bid_price_1}, {tick.bid_price_2}, {tick.bid_price_3}, {tick.bid_price_4}, {tick.bid_price_5}, "
                   + f"{tick.ask_price_1}, {tick.ask_price_2}, {tick.ask_price_3}, {tick.ask_price_4}, {tick.ask_price_5}, "
                   + f"{tick.bid_volume_1}, {tick.bid_volume_2}, {tick.bid_volume_3}, {tick.bid_volume_4}, {tick.bid_volume_5}, "
                   + f"{tick.ask_volume_1}, {tick.ask_volume_2}, {tick.ask_volume_3}, {tick.ask_volume_4}, {tick.ask_volume_5}, "
                   + f"'{localtime}')")

    return result
