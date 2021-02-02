# coding=utf-8
import traceback
import datetime
import pandas as pd
import numpy as np
from abc import ABCMeta, abstractmethod
from decimal import Decimal
import logging
from utility.common import *
from utility.data_converter import DataConverter
from utility.file_parse_strategy_base.abstract_strategy import AbstractStrategy
from stock_settlement_parser.global_vars import *
from stock_settlement_parser.data_struct.yhzq_product15_column import YHZQ_PRODUCT15_ASSET_COLUMN_MAP, \
    YHZQ_PRODUCT15_TRADE_RECORD_COLUMN_MAP, YHZQ_PRODUCT15_POSITION_COLUMN_MAP, \
    YHZQ_PRODUCT15_POSITION_ADDITIONAL_FIELDS, YHZQ_PRODUCT15_TRADE_RECORD_ADDITIONAL_FIELDS, \
    YHZQ_PRODUCT15_ASSET_ADDITIONAL_FIELDS, OTHER_FEE_COLUMNS

logger = logging.getLogger(__name__)


class yhzq_product15(AbstractStrategy):
    def parse(self, file_name):
        np.set_printoptions(suppress=True)
        self.broker = getattr(self.settings, 'BROKER')
        self.product = getattr(self.settings, 'PRODUCT')
        self.file_name = file_name
        parse_context = YhzqContext(self.product, self.broker, self.file_name)
        return parse_context.parse()


class YhzqContext():
    def __init__(self, product, broker, file_name):
        self.file_name = file_name
        self.parser = None
        if file_name.endswith('.txt'):
            self.parser = YhzqTxtParser(product, broker, file_name)
        elif file_name.endswith('.xls') or file_name.endswith('.xlsx'):
            self.parser = YhzqExcelParser(product, broker, file_name)
        else:
            # raise ValueError('不能解析文件：%s' % self.file_name)
            logger.warning('扩展名错误, 不能解析文件：%s' % self.file_name)

    def parse(self):
        if self.parser is None:
            return {'df_amount': pd.DataFrame(), 'df_position': pd.DataFrame(), 'df_trade_record': pd.DataFrame()}
        return self.parser.parse()


class YhzqParseStrategy():
    __metaclass__ = ABCMeta

    def __init__(self, product, broker, file_name):
        self.product = product
        self.broker = broker
        self.file_name = file_name

    @abstractmethod
    def parse(self):
        pass


class YhzqExcelParser(YhzqParseStrategy):
    def parse(self):
        self.parse_base_info()
        self.get_base_information(self.df_base)
        # parse position
        self.df_position = self.parse_position_base(self.df_base)
        # market value
        self.get_market_val(self.df_position)
        # parse asset
        self.df_account_amount = self.parse_asset_base(self.df_base)
        # parse trade record
        self.df_trade_record = self.parse_trade_base(self.df_base)

        data = {'df_amount': self.df_account_amount, 'df_position': self.df_position,
                'df_trade_record': self.df_trade_record}
        return data

    def parse_base_info(self):
        self.df_base = pd.read_excel(self.file_name, dtype=str).dropna(axis=0, how='all').dropna(axis=1,how='all').reset_index(drop=True)


    @staticmethod
    def exchange_map(stock_code):
        if stock_code[:2] == '60':
            return 'SH'
        elif stock_code[:2] == '00' or stock_code[:2] == '20':
            return 'SZ'
        else:
            return 'HK'

    def get_base_information(self, df_base):
        point_x, point_y = get_element_point('资金帐号', df_base)
        point_m, point_n = get_element_point('期间', df_base)
        asset_account = df_base.iloc[point_x, point_y + 1]
        data_date = df_base.iloc[point_m, point_n + 1][:11]
        self.asset_account = asset_account
        self.data_date = data_date

    @staticmethod
    def pre_process(element, df_base):
        point_x, point_y = get_element_point(element, df_base)
        if point_x is not None and point_y is not None:
            df = df_base.iloc[point_x + 1:]
            df.columns = df.iloc[0]
            df.columns.name = None
            df = df.iloc[1:]
            df.index = range(len(df))
        else:
            df = None
        return df

    def parse_position_base(self, df_base):
        df_position = self.pre_process('证券资产', df_base)
        for s in range(len(df_position)):
            if type(df_position['证券名称'][s]) != str:
                df_position = df_position.iloc[:s].copy()
                break
        df_position.loc[:, '资金帐号'] = self.asset_account
        df_position.loc[:, '日期'] = self.data_date
        df_position.loc[:, '市场名称'] = df_position['证券代码'].apply(self.exchange_map)
        df_position.loc[:, 'FORZEN'] = 0.00
        df_position.loc[:, 'INTRANSIT'] = 0.00
        df_position.loc[:, 'CLIENT_ID_IN_BROKER'] = df_position['资金帐号']
        df_position.loc[:, 'CURRENCY'] = 'RMB'
        df_position.loc[:, 'BROKER_NAME'] = '银河证券'
        df_position.loc[:, 'CLOSE_PRICE_CURRENCY'] = 'RMB'
        df_position.loc[:, 'FROZEN'] = 0.00
        df_position.rename(columns=YHZQ_PRODUCT15_POSITION_COLUMN_MAP, inplace=True)
        df_position = df_position.copy()
        attr = ['POSITION', 'AVAILABLE', 'VAL', 'CLOSE_PRICE', 'REF_COST', 'FROZEN']
        for i in attr:
            df_position.loc[:, i] = df_position.loc[:, i].astype(np.float64)
        df_position = drop_invalid_columns(df_position)
        if df_position.empty:
            return df_position
        return df_position

    def get_market_val(self, df_pos):
        self.market_value_a = get_market_value(self.asset_account, df_pos, exchange='A')
        self.market_value_h = get_market_value(self.asset_account, df_pos, exchange='H')

    def parse_asset_base(self, df_base):
        df_asset = self.pre_process('资产信息', df_base)
        df_asset = df_asset.iloc[:3]
        df_asset.dropna(axis=1, how='all', subset=[0, 1, 2], inplace=True)
        df_asset = df_asset.copy()
        df_asset.loc[:, '资金帐号'] = self.asset_account
        df_asset.loc[:, '日期'] = self.data_date
        df_asset.loc[:, 'CLIENT_ID_IN_BROKER'] = df_asset['资金帐号']
        df_asset.loc[:, 'FINANCING_AMOUNT'] = 0.00
        df_asset.loc[:, 'MARGIN_AMOUNT'] = 0.00
        df_asset.loc[:, 'BROKER_NAME'] = '银河证券'
        df_asset.loc[0, 'A_VAL'] = self.market_value_a
        df_asset.loc[0, 'H_VAL'] = self.market_value_h
        df_asset.fillna(0.00, inplace=True)
        df_asset.rename(columns=YHZQ_PRODUCT15_ASSET_COLUMN_MAP, inplace=True)
        df_asset = df_asset.copy()
        attr = ['CUR_BALANCE', 'CUR_AVAILABLE', 'TOTAL_ASSETS', 'A_VAL']
        for i in attr:
            df_asset.loc[:, i] = df_asset.loc[:, i].astype(np.float64)
        df_asset = drop_invalid_columns(df_asset)
        if df_asset.empty:
            return df_asset
        return df_asset

    def parse_trade_base(self, df_base):
        df_trade_delivered = self.pre_process('资产交割', df_base)
        if df_trade_delivered is None or type(df_trade_delivered['银行'].iloc[0]) != str:
            df_trade_delivered = None
        else:
            for s in range(len(df_trade_delivered)):
                if type(df_trade_delivered['银行'][s]) != str:
                    df_trade_delivered = df_trade_delivered.iloc[:s]
                    break
            df_trade_delivered = df_trade_delivered.copy()
            df_trade_delivered.loc[:, '资金帐号'] = self.asset_account
            df_trade_delivered.loc[:, '市场名称'] = df_trade_delivered['证券代码'].apply(self.exchange_map)
            df_trade_delivered.loc[:, 'CLIENT_ID_IN_BROKER'] = df_trade_delivered['资金帐号']
            df_trade_delivered.loc[:, 'CURRENCY'] = 'RMB'
            df_trade_delivered.loc[:, 'KF_ABSTRACT'] = df_trade_delivered['业务标志']
            df_trade_delivered.loc[:, 'OTHER_FEE'] = 0.00
            df_trade_delivered.loc[:, 'BROKER_NAME'] = '银河证券'
            df_trade_delivered.loc[:, '日期'] = df_trade_delivered.loc[:, '日期'].apply(DataConverter.convert_str_to_date)
            for i in df_trade_delivered.index:
                df_trade_delivered.loc[i, 'OTHER_FEE'] = cal_other_fee(df_trade_delivered.loc[i],
                                                                       df_trade_delivered.columns, OTHER_FEE_COLUMNS)
            df_trade_delivered.rename(columns=YHZQ_PRODUCT15_TRADE_RECORD_COLUMN_MAP, inplace=True)
            df_trade_delivered = df_trade_delivered.copy()
            attr = ['PRICE', 'AMOUNT', 'STAMP_DUTY', 'OTHER_FEE', 'COMMISSION']
            for i in attr:
                df_trade_delivered.loc[:, i] = df_trade_delivered.loc[:, i].astype(np.float64)
            df_trade_delivered.loc[:, 'VOLUME'] = df_trade_delivered.loc[:, 'VOLUME'].astype(np.int64)

        df_trade_undelivered = self.pre_process('资产未交割', df_base)
        if df_trade_undelivered is None or type(df_trade_undelivered['银行'].iloc[0]) != str:
            df_trade_undelivered = None
        else:
            for s in range(len(df_trade_undelivered)):
                if type(df_trade_undelivered['银行'][s]) != str:
                    df_trade_undelivered = df_trade_undelivered.iloc[:s]
                    break
            df_trade_undelivered = df_trade_undelivered.copy()
            df_trade_undelivered.dropna(axis=1, how='all', subset=[0], inplace=True)
            df_trade_undelivered.loc[:, '资金帐号'] = self.asset_account
            df_trade_undelivered.loc[:, '市场名称'] = df_trade_undelivered['证券代码'].apply(self.exchange_map)
            df_trade_undelivered.loc[:, 'CLIENT_ID_IN_BROKER'] = df_trade_undelivered['资金帐号']
            df_trade_undelivered.loc[:, 'CURRENCY'] = 'RMB'
            df_trade_undelivered.loc[:, 'OTHER_FEE'] = 0.00
            df_trade_undelivered.loc[:, 'BROKER_NAME'] = '银河证券'
            df_trade_undelivered.loc[:, '委托日期'] = df_trade_undelivered.loc[:, '委托日期'].apply(
                DataConverter.convert_str_to_date)
            for i in df_trade_undelivered.index:
                df_trade_undelivered.loc[i, 'OTHER_FEE'] = cal_other_fee(df_trade_undelivered.loc[i],
                                                                         df_trade_undelivered.columns, OTHER_FEE_COLUMNS)
            df_trade_undelivered.loc[:, '业务标志'] = '/'
            df_trade_undelivered.loc[:, 'KF_ABSTRACT'] = df_trade_undelivered['业务标志']
            df_trade_undelivered.rename(columns={'委托日期': '日期', '成交数': '发生数', '成交金额': '发生金额'}, inplace=True)
            df_trade_undelivered.rename(columns=YHZQ_PRODUCT15_TRADE_RECORD_COLUMN_MAP, inplace=True)
            df_trade_undelivered = df_trade_undelivered.copy()
            attr = ['PRICE', 'AMOUNT', 'STAMP_DUTY', 'OTHER_FEE', 'COMMISSION']
            for i in attr:
                df_trade_undelivered.loc[:, i] = df_trade_undelivered.loc[:, i].astype(np.float64)
            df_trade_undelivered.loc[:, 'VOLUME'] = df_trade_undelivered.loc[:, 'VOLUME'].astype(np.int64)
        df_trade = pd.concat([df_trade_delivered, df_trade_undelivered])
        for i in df_trade.index:
            df_trade.loc[i, 'PRICE'] = float(format(df_trade.loc[i, 'PRICE'], '0.2f'))
        df_trade = drop_invalid_columns(df_trade)
        if df_trade.empty:
            return df_trade
        return df_trade


class YhzqTxtParser(YhzqParseStrategy):
    def parse(self):
        pass
