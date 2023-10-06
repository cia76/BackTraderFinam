import collections
from datetime import timedelta

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass
from backtrader import TimeFrame

from FinamPy import FinamPy
from FinamPy.proto.tradeapi.v1.candles_pb2 import DayCandleTimeFrame, IntradayCandleTimeFrame


class MetaSingleton(MetaParams):
    """Метакласс для создания Singleton классов"""
    def __init__(cls, *args, **kwargs):
        """Инициализация класса"""
        super(MetaSingleton, cls).__init__(*args, **kwargs)
        cls._singleton = None  # Экземпляра класса еще нет

    def __call__(cls, *args, **kwargs):
        """Вызов класса"""
        if cls._singleton is None:  # Если класса нет в экземплярах класса
            cls._singleton = super(MetaSingleton, cls).__call__(*args, **kwargs)  # то создаем зкземпляр класса
        return cls._singleton  # Возвращаем экземпляр класса


class FNStore(with_metaclass(MetaSingleton, object)):
    """Хранилище Финам. Работает с мультисчетами

    В параметр providers передавать список счетов в виде словаря с ключами:
    - provider_name - Название провайдера. Должно быть уникальным
    - client_id - Торговый счет
    - access_token - Торговый токен доступа из Config

    Пример использования:
    provider1 = dict(provider_name='finam_trade', client_id=Config.ClientIds[0], access_token=Config.AccessToken)  # Торговый счет Финам
    provider2 = dict(provider_name='finam_iia', client_id=Config.ClientIds[1], access_token=Config.AccessToken)  # ИИС Финам
    store = FNStore(providers=[provider1, provider2])  # Мультисчет
    """
    params = (
        ('providers', None),  # Список провайдеров счетов в виде словаря
    )

    BrokerCls = None  # Класс брокера будет задан из брокера
    DataCls = None  # Класс данных будет задан из данных

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Возвращает новый экземпляр класса данных с заданными параметрами"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Возвращает новый экземпляр класса брокера с заданными параметрами"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self, **kwargs):
        super(FNStore, self).__init__()
        if 'providers' in kwargs:  # Если хранилище создаем из данных/брокера (не рекомендуется)
            self.p.providers = kwargs['providers']  # то список провайдеров берем из переданного ключа providers
        self.notifs = collections.deque()  # Уведомления хранилища
        self.providers = {}  # Справочник провайдеров
        for provider in self.p.providers:  # Пробегаемся по всем провайдерам
            provider_name = provider['provider_name'] if 'provider_name' in provider else 'default'  # Название провайдера или название по умолчанию
            self.providers[provider_name] = (FinamPy(provider['access_token']), provider['client_id'])  # Работа с сервером TRANSAQ из Python через REST/gRPC https://finamweb.github.io/trade-api-docs/ с токеном по счету
        self.provider = list(self.providers.values())[0][0]  # Провайдер по умолчанию (первый) для работы со справочниками

    def start(self):
        pass  # Когда Финам сделает подписку на новые бары, то сделать по аналогии с Алором

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        for provider in self.providers.values():  # Пробегаемся по всем значениям провайдеров
            provider[0].close_channel()  # Закрываем канал перед выходом

    # Функции конвертации

    @staticmethod
    def is_intraday(timeframe) -> bool:
        """Является ли заданный временной интервал внутридневным

        :param TimeFrame timeframe: Временной интервал
        :return: Является ли заданный временной интервал внутридневным
        """
        return timeframe == TimeFrame.Minutes

    @staticmethod
    def timeframe_to_finam_timeframe(timeframe, compression):
        """Перевод временнОго интервала во временной интервал Финам

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал Финам
        """
        if timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_W1
        elif timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            if compression == 60:  # Часовой временной интервал
                return IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_H1
            elif compression == 15:  # 15-и минутный временной интервал
                return IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M15
            elif compression == 5:  # 5-и минутный временной интервал
                return IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M5
            elif compression == 1:  # 1 минутный временной интервал
                return IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M1
        else:  # В остальных случаях
            return DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1  # возвращаем значение по умолчанию

    @staticmethod
    def timeframe_to_timedelta(timeframe, compression) -> timedelta:
        """Перевод временнОго интервала в разницу во времени

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Разница во времени
        """
        if timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return timedelta(days=1)
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return timedelta(weeks=1)
        elif timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return timedelta(minutes=compression)  # 1, 5, 15 минут, 1 час
