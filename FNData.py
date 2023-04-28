from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass

from BackTraderFinam import FNStore

from FinamPy import FinamPy


class MetaFNData(AbstractDataBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaFNData, self).__init__(name, bases, dct)  # Инициализируем класс данных
        FNStore.DataCls = self  # Регистрируем класс данных в хранилище Финам


class FNData(with_metaclass(MetaFNData, AbstractDataBase)):
    """Данные Финам"""
    params = (
        ('provider_name', None),  # Название провайдера. Если не задано, то первое название по ключу name
        ('four_price_doji', False),  # False - не пропускать дожи 4-х цен, True - пропускать
        ('schedule', None),  # Расписание работы биржи
        ('live_bars', False),  # False - только история, True - история и новые бары
    )

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.live_bars

    def __init__(self, **kwargs):
        # TODO Конвертация временнЫх интервалов Финам -> BackTrader (self.p.timeframe, self.p.compression)
        self.store = FNStore(**kwargs)  # Передаем параметры в хранилище Финам. Может работать самостоятельно, не через хранилище
        self.provider_name = self.p.provider_name if self.p.provider_name else list(self.store.providers.keys())[0]  # Название провайдера, или первое название по ключу name
        self.provider: FinamPy = self.store.providers[self.provider_name]  # Провайдер
        self.board, self.symbol = self.provider.data_name_to_board_symbol(self.p.dataname)  # По тикеру получаем биржу и код тикера
        # TODO Все остальные переменные

    def setenvironment(self, env):
        """Добавление хранилища Алор в cerebro"""
        super(FNData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища Алор в cerebro

    def start(self):
        super(FNData, self).start()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
        # TODO Ждем от Финама получение истории/истории с подпиской
        # TODO Уведомление о подключении

    def _load(self):
        """Загружаем бар из истории или новый бар в BackTrader"""
        # TODO Ждем от Финама получение истории/истории с подпиской

    def stop(self):
        super(FNData, self).stop()
        # TODO Ждем от Финама получение истории/истории с подпиской

    # Функции

    # TODO Ждем от Финама получение истории/истории с подпиской
