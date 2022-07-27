import io
import socket
import struct
import zlib
from enum import Enum
from typing import Any, BinaryIO, NamedTuple
import datetime

from binary_reader import BinaryReader


def _format_time(timestamp: int) -> str:
    ts = str(timestamp)
    time = ts[:-6] + ':'
    if int(ts[-6:-4]) < 60:
        time += f'{ts[-6:-4]}:{int(ts[-4:] * 60) / 10000:06.3f}'
    else:
        time += f'{int(int(ts[-6:]) * 60 / 1000000):02d}:{(int(ts[-6:]) * 60 % 1000000) * 60 / 1000000:06.3f}'
    return time


def _get_time(reader: BinaryReader) -> datetime.time:
    tminutes = reader.u16
    return datetime.time(tminutes // 60, tminutes % 60)


def _calc_price(base_value: float, offset: float) -> float:
    return (base_value + offset) / 100


def _calc_price1k(base_value: float, offset: float) -> float:
    return (base_value + offset) / 1000


class DataEntry(NamedTuple):
    date: datetime.datetime
    price_open: float
    price_high: float
    price_low: float
    price_close: float
    amount: float
    volume: float


class Api:
    class Market(Enum):
        SZ = 0
        SH = 1

    class KLineCategory(Enum):
        K5 = 0
        K15 = 1
        K30 = 2
        K60 = 3
        KDaily = 4
        KWeek = 5
        KMonth = 6
        PerMinute = 7
        K1 = 8
        KDay = 9
        KSeason = 10
        KYear = 11

    class XDXRCategory(Enum):
        ExcludeRightAndExcludeDividen = 1  # 除权除息
        AllotmentSharesListing = 2  # 送配股上市
        NonMarketableSharesListing = 3  # 非流通股上市
        UnknownChangesInShareCapital = 4  # 未知股本变动
        ChangesInShareCapital = 5  # 股本变动
        AdditionalIssuanceOfNewShares = 6  # 增发新股
        ShareRepurchase = 7  # 股份回购
        AdditionalSharesListing = 8  # 增发新股上市
        AllottedSharesListing = 9  # 转配股上市
        ConvertibleBondsListing = 10  # 可转债上市
        StockExpansionAndContraction = 11  # 扩缩股
        ReductionOfNonTradableShares = 12  # 非流通股缩股
        SendWarrant = 13  # 送认购权证
        SendPutWarrant = 14  # 送认沽权证

    _client: socket.socket | None

    RSP_HEADER_LENGTH = 0x10

    def __init__(self, server: tuple[str, int] | None = None, connection_timeout: float = 1.0) -> None:
        if server:
            host, port = server
            self._client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._client.settimeout(connection_timeout)
            self._client.connect((host, port))
            self._hello()
        else:
            self._client = None

    def _hello(self) -> None:
        self._req(b'\x0c\x02\x18\x93\x00\x01\x03\x00\x03\x00\x0d\x00\x01')
        self._req(b'\x0c\x02\x18\x94\x00\x01\x03\x00\x03\x00\x0d\x00\x02')
        self._req(b'\x0c\x03\x18\x99\x00\x01\x20\x00\x20\x00\xdb\x0f\xd5'
                  b'\xd0\xc9\xcc\xd6\xa4\xa8\xaf\x00\x00\x00\x8f\xc2\x25'
                  b'\x40\x13\x00\x00\xd5\x00\xc9\xcc\xbd\xf0\xd7\xea\x00'
                  b'\x00\x00\x02')

    def get_stocks_count(self, market: Market) -> int:
        reader = self._req(
            b'\x0c\x0c\x18\x6c\x00\x01\x08\x00\x08\x00\x4e\x04' + bytes([market.value]) + b'\x00\x75\xc7\x33\x01')
        return reader.u16

    def get_stocks_list(self, market: Market, start: int) -> list[dict[str, Any]]:
        package = b'\x0c\x01\x18\x64\x01\x01\x06\x00\x06\x00\x50\x04' + struct.pack('<HH', market.value, start)
        reader = self._req(package)
        stocks_count = reader.u16
        stocks = []
        for _ in range(stocks_count):
            stocks.append({
                '股票代码': reader.read(6).decode(),
                'volunit': reader.u16,
                '股票名称': reader.read(8).decode('gbk').rstrip('\x00'),
                'reserved_bytes1': reader.read(4),
                'decimal_point': reader.u8,
                '昨日收盘价': reader.f32,
                'reserved_bytes2': reader.read(4)
            })
        return stocks

    def get_stock_quotes(self, stocks: list[tuple[Market, str]]) -> list[dict[str, Any]]:
        stocks_count = len(stocks)
        package_size = stocks_count * 7 + 12
        package = bytearray(struct.pack('<HIHHIIHH', 0x10c, 0x02006320,
                                        package_size, package_size,
                                        0x5053e, 0, 0,
                                        stocks_count))
        for market, stock in stocks:
            package.extend(struct.pack('<B6s', market.value, stock.encode()))
        reader = self._req(package)
        reader.skip(2)
        stocks_count = reader.u16
        result = []

        for _ in range(stocks_count):
            market, stock, active1 = struct.unpack('<B6sH', reader.read(9))
            price = reader.vint
            result.append({
                '市场': self.Market(market),
                '股票代码': stock,
                'active1': active1,
                '股价': _calc_price(price, 0),
                '昨日收盘价': _calc_price(price, reader.vint),
                '今日开盘价': _calc_price(price, reader.vint),
                '最高价': _calc_price(price, reader.vint),
                '最低价': _calc_price(price, reader.vint),
                '服务器时间': _format_time(reader.vint),
                'reserved_bytes1': reader.vint,
                '成交量': reader.vint,
                '当前成交量': reader.vint,
                '成交额': reader.f32,
                '内盘': reader.vint,
                '外盘': reader.vint,
                'reserved_bytes2': reader.vint,
                'reserved_bytes3': reader.vint,
                '买1': _calc_price(price, reader.vint),
                '卖1': _calc_price(price, reader.vint),
                '买1成交量': reader.vint,
                '卖1成交量': reader.vint,
                '买2': _calc_price(price, reader.vint),
                '卖2': _calc_price(price, reader.vint),
                '买2成交量': reader.vint,
                '卖2成交量': reader.vint,
                '买3': _calc_price(price, reader.vint),
                '卖3': _calc_price(price, reader.vint),
                '买3成交量': reader.vint,
                '卖3成交量': reader.vint,
                '买4': _calc_price(price, reader.vint),
                '卖4': _calc_price(price, reader.vint),
                '买4成交量': reader.vint,
                '卖4成交量': reader.vint,
                '买5': _calc_price(price, reader.vint),
                '卖5': _calc_price(price, reader.vint),
                '买5成交量': reader.vint,
                '卖5成交量': reader.vint,
                'reserved_bytes4': reader.u16,
                'reserved_bytes5': reader.vint,
                'reserved_bytes6': reader.vint,
                'reserved_bytes7': reader.vint,
                'reserved_bytes8': reader.vint,
                '增速': reader.i16 / 100,
                'active2': reader.u16
            })
        return result

    def get_k_line(self, category: KLineCategory, market: Market, stock: str, start: int, count: int) -> list[
        dict[str, Any]]:
        reader = self._req(struct.pack('<HIHHHH6sHHHHIIH',
                                       0x10c, 0x01016408, 0x1c, 0x1c, 0x052d,
                                       market.value,
                                       stock.encode(),
                                       category.value,
                                       1,
                                       start, count,
                                       0, 0, 0))
        try:  # 尝试作为指数
            count = reader.u16
            klines = []
            pre_diff_base = 0
            for _ in range(count):
                date = self._get_datetime(category, reader)
                price_open_diff = reader.vint
                price_close_diff = reader.vint
                price_high_diff = reader.vint
                price_low_diff = reader.vint
                price_open = _calc_price1k(price_open_diff, pre_diff_base)
                price_open_diff += pre_diff_base

                pre_diff_base = price_open_diff + price_close_diff
                klines.append({
                    '时刻': date,
                    '开盘价': price_open,
                    '收盘价': _calc_price1k(price_open_diff, price_close_diff),
                    '最高价': _calc_price1k(price_open_diff, price_high_diff),
                    '最低价': _calc_price1k(price_open_diff, price_low_diff),
                    '成交量': reader.f32,
                    '成交额': reader.f32,
                    '上涨数': reader.u16,
                    '下跌数': reader.u16
                })
            return klines
        except ValueError:  # 不是指数
            reader.pos = 0
            count = reader.u16
            klines = []
            pre_diff_base = 0
            for _ in range(count):
                date = self._get_datetime(category, reader)
                price_open_diff = reader.vint
                price_close_diff = reader.vint
                price_high_diff = reader.vint
                price_low_diff = reader.vint
                price_open = _calc_price1k(price_open_diff, pre_diff_base)
                price_open_diff += pre_diff_base

                pre_diff_base = price_open_diff + price_close_diff
                klines.append({
                    '时刻': date,
                    '开盘价': price_open,
                    '收盘价': _calc_price1k(price_open_diff, price_close_diff),
                    '最高价': _calc_price1k(price_open_diff, price_high_diff),
                    '最低价': _calc_price1k(price_open_diff, price_low_diff),
                    '成交量': reader.f32,
                    '成交额': reader.f32,
                })
            return klines

    def get_minute_data(self, market: Market, stock: str) -> list[dict[str, Any]]:
        reader = self._req(
            b'\x0c\x1b\x08\x00\x01\x01\x0e\x00\x0e\x00\x1d\x05' + struct.pack('<H6sI', market.value, stock.encode(), 0))
        count = reader.u16
        last_price = 0
        reader.skip(2)
        prices = []
        for _ in range(count):
            price_raw = reader.vint
            _reserved1 = reader.vint
            last_price += price_raw
            prices.append({
                '价格': last_price / 100,
                '成交量': reader.vint
            })
        return prices

    def get_history_minute_data(self, market: Market, stock: str, date: int) -> list[dict[str, Any]]:
        reader = self._req(
            b'\x0c\x01\x30\x00\x01\x01\x0d\x00\x0d\x00\xb4\x0f' + struct.pack('<IB6s', date, market.value,
                                                                              stock.encode()))
        count = reader.u16
        last_price = 0
        reader.skip(4)
        prices = []
        for _ in range(count):
            price_raw = reader.vint
            _reserved1 = reader.vint
            last_price += price_raw
            prices.append({
                '价格': last_price / 100,
                '成交量': reader.vint
            })
        return prices

    def get_transaction_data(self, market: Market, stock: str, start: int, count: int) -> list[dict[str, Any]]:
        reader = self._req(
            b'\x0c\x17\x08\x01\x01\x01\x0e\x00\x0e\x00\xc5\x0f' + struct.pack('<H6sHH', market.value, stock.encode(),
                                                                              start, count))
        count = reader.u16
        last_price = 0
        trades = []
        for _ in range(count):
            time = _get_time(reader)
            last_price += reader.vint

            trades.append({
                '时间': time,
                '价格': last_price / 100,
                '成交量': reader.vint,
                'num': reader.vint,
                'buyorsell': reader.vint
            })
            _reserved1 = reader.vint
        return trades

    def get_history_transaction_data(self, market: Market, stock: str, start: int, count: int, date: int) -> list[
        dict[str, Any]]:
        reader = self._req(
            b'\x0c\x01\x30\x01\x00\x01\x12\x00\x12\x00\xb5\x0f' + struct.pack('<IH6sHH', date, market.value,
                                                                              stock.encode(), start, count))
        trades = []
        count = reader.u16
        reader.skip(4)
        last_price = 0
        for _ in range(count):
            time = _get_time(reader)
            last_price += reader.vint

            trades.append({
                '时间': time,
                '价格': last_price / 100,
                '成交量': reader.vint,
                'num': reader.vint,
                'buyorsell': reader.vint
            })
        return trades

    def get_company_info_entry(self, market: Market, stock: str) -> list[dict[str, Any]]:
        reader = self._req(
            b'\x0c\x0f\x10\x9b\x00\x01\x0e\x00\x0e\x00\xcf\x02' + struct.pack('<H6sI', market.value, stock.encode(), 0))
        count = reader.u16

        entries = []
        for _ in range(count):
            entries.append({
                '名称': reader.rpad_str(64),
                '文件名': reader.rpad_str(80),
                '起始': reader.u32,
                '长度': reader.u32
            })
        return entries

    def get_company_info_content(self, market: Market, stock: str, filename: str, start: int, length: int) -> str:
        reader = self._req(b'\x0c\x07\x10\x9c\x00\x01\x68\x00\x68\x00\xd0\x02' + struct.pack('<H6sH80sIII',
                                                                                             market.value,
                                                                                             stock.encode(),
                                                                                             0,
                                                                                             filename.encode().ljust(80,
                                                                                                                     b'\0'),
                                                                                             start, length, 0))
        _ = reader.read(10)
        length = reader.u16
        return reader.read(length).decode('gbk')

    def get_xdxr_info(self, market: Market, stock: str) -> list[dict[str, Any]]:
        reader = self._req(
            b'\x0c\x1f\x18\x76\x00\x01\x0b\x00\x0b\x00\x0f\x00\x01\x00' + struct.pack('<B6s', market.value,
                                                                                      stock.encode()))
        if len(reader) < 11:
            return []

        _market = self.Market(reader.u8)
        reader.skip(2)
        _code = reader.read(6).decode()

        result = []

        for _ in range(reader.u16):
            reader.skip(1 + 7)
            date = self._get_datetime(self.KLineCategory.KDay, reader)
            category = self.XDXRCategory(reader.u8)
            entry = {
                '日期': date,
                '类型': category,
            }
            match category:
                case self.XDXRCategory.ExcludeRightAndExcludeDividen:
                    entry |= {
                        '分红': reader.f32,
                        '配股价': reader.f32,
                        '送转股': reader.f32,
                        '配股': reader.f32
                    }
                case self.XDXRCategory.StockExpansionAndContraction | self.XDXRCategory.ReductionOfNonTradableShares:
                    entry |= {
                        'rev1': reader.f32,
                        'rev2': reader.f32,
                        '缩股': reader.f32,
                        'rev3': reader.f32
                    }
                case self.XDXRCategory.SendWarrant | self.XDXRCategory.SendPutWarrant:
                    entry |= {
                        '行权价': reader.f32,
                        'rev1': reader.f32,
                        '分数': reader.f32,
                        'rev2': reader.f32
                    }
                case _:
                    entry |= {
                        '盘前流通': reader.f32,
                        '前总股本': reader.f32,
                        '盘后流通': reader.f32,
                        '后总股本': reader.f32
                    }
            result.append(entry)

        return result

    def get_finance_info(self, market: Market, stock: str):
        reader = self._req(
            b'\x0c\x1f\x18\x76\x00\x01\x0b\x00\x0b\x00\x10\x00\x01\x00' + struct.pack('<B6s', market.value,
                                                                                      stock.encode()))
        reader.skip(2)
        return {
            '市场': self.Market(reader.u8),
            '股票代码': reader.read(6).decode(),
            '流动股本': reader.f32 * 10000,
            '省': reader.u16,
            '工业': reader.u16,
            '更新日期': reader.u32,
            'ipo_date': reader.u32,
            '总股本': reader.f32 * 10000,
            '国家股': reader.f32 * 10000,
            '发起人法人股': reader.f32 * 10000,
            '法人股': reader.f32 * 10000,
            'B股': reader.f32 * 10000,
            'H股': reader.f32 * 10000,
            '职工股': reader.f32 * 10000,
            '总资产': reader.f32 * 10000,
            '流动资产': reader.f32 * 10000,
            '固定资产': reader.f32 * 10000,
            '无形资产': reader.f32 * 10000,
            '股东人数': reader.f32 * 10000,
            '流动负债': reader.f32 * 10000,
            '长期负债': reader.f32 * 10000,
            '资本公积金': reader.f32 * 10000,
            '净资产': reader.f32 * 10000,
            '主营收入': reader.f32 * 10000,
            '主营利润': reader.f32 * 10000,
            '营收账款': reader.f32 * 10000,
            '营业利润': reader.f32 * 10000,
            '投资收入': reader.f32 * 10000,
            '经营现金流': reader.f32 * 10000,
            '总现金流': reader.f32 * 10000,
            '存活': reader.f32 * 10000,
            '利润总和': reader.f32 * 10000,
            '税后利润': reader.f32 * 10000,
            '净利润': reader.f32 * 10000,
            '未分配利润': reader.f32 * 10000,
            '每股净资产': reader.f32,
            'reserved2': reader.f32
        }

    def read_day_file(self, file: BinaryIO) -> list[DataEntry]:
        reader = BinaryReader(file)
        result = []
        while not reader.eof:
            result.append(DataEntry(self._get_datetime(self.KLineCategory.KDaily, reader),
                                    reader.u32,
                                    reader.u32,
                                    reader.u32,
                                    reader.u32,
                                    reader.f32,
                                    reader.u32))
            reader.skip(4)
        return result

    def read_minute_file(self, file: BinaryIO) -> list[DataEntry]:
        reader = BinaryReader(file)
        result = []
        while not reader.eof:
            result.append(DataEntry(self._get_datetime(self.KLineCategory.K1, reader),
                                    reader.u32 / 100,
                                    reader.u32 / 100,
                                    reader.u32 / 100,
                                    reader.u32 / 100,
                                    reader.f32,
                                    reader.u32))
            reader.skip(4)
        return result

    def read_minute_lc_file(self, file: BinaryIO) -> list[DataEntry]:
        reader = BinaryReader(file)
        result = []
        while not reader.eof:
            result.append(DataEntry(self._get_datetime(self.KLineCategory.K1, reader),
                                    reader.f32,
                                    reader.f32,
                                    reader.f32,
                                    reader.f32,
                                    reader.f32,
                                    reader.u32))
            reader.skip(4)
        return result

    def _get_datetime(self, category: KLineCategory, reader: BinaryReader) -> datetime.datetime:
        if category.value < self.KLineCategory.KDaily.value or category in (
                self.KLineCategory.PerMinute, self.KLineCategory.K1):
            zipday = reader.u16
            tminutes = reader.u16
            return datetime.datetime((zipday >> 11) + 2004,
                                     int((zipday % 2048) / 100),
                                     (zipday % 2048) % 100,
                                     int(tminutes / 60),
                                     tminutes % 60)
        else:
            zipday = reader.u32
            return datetime.datetime(zipday // 10000,
                                     (zipday % 10000) // 100,
                                     zipday % 100,
                                     15)

    def _req(self, data: bytes) -> BinaryReader:
        if not self._client:
            raise RuntimeError('初始化时未提供服务器地址')
        self._client.send(data)
        recv = self._client.recv(self.RSP_HEADER_LENGTH)
        _r1, _r2, _r3, zipped_size, unzipped_size = struct.unpack('<IIIHH', recv)
        data = bytearray()
        remained_size = zipped_size
        while remained_size > 0:
            tmp_data = self._client.recv(remained_size)
            remained_size -= len(tmp_data)
            data.extend(tmp_data)
        if zipped_size != unzipped_size:
            data = zlib.decompress(data)
        return BinaryReader(io.BytesIO(data))

    def heartbeat(self) -> None:
        # 发送心跳包
        # 无需理会返回结果
        self.get_stocks_count(self.Market.SH)

    def __enter__(self) -> 'Api':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


__all__ = ['Api']
