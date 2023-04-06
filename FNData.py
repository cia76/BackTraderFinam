from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass

from BackTraderFinam import FNStore


class MetaFNData(AbstractDataBase.__class__):
    def __init__(cls, name, bases, dct):
        super(MetaFNData, cls).__init__(name, bases, dct)  # Инициализируем класс данных
        FNStore.DataCls = cls  # Регистрируем класс данных в хранилище Финам


class FNData(with_metaclass(MetaFNData, AbstractDataBase)):
    """Данные Финам"""
    params = (
        ('provider_name', None),  # Название провайдера. Если не задано, то первое название по ключу name
        ('four_price_doji', False),  # False - не пропускать дожи 4-х цен, True - пропускать
        ('live_bars', False),  # False - только история, True - история и новые бары
    )

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.live_bars

    # TODO Ждем реализацию подписки на новые бары от Финама
