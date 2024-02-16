import collections
import logging

from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass

from FinamPy import FinamPy


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
    logger = logging.getLogger('FNStore')  # Будем вести лог

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
            self.logger.warning('Хранилище создано из данных/брокера. Рекомендуется сначала создать хранилище, а из него создавать данные/брокера')
            self.p.providers = kwargs['providers']  # то список провайдеров берем из переданного ключа providers
        self.notifs = collections.deque()  # Уведомления хранилища
        self.providers = {}  # Справочник провайдеров
        for provider in self.p.providers:  # Пробегаемся по всем провайдерам
            provider_name = provider['provider_name'] if 'provider_name' in provider else 'default'  # Название провайдера или название по умолчанию
            self.providers[provider_name] = (FinamPy(provider['access_token']), provider['client_id'])  # Работа с сервером TRANSAQ из Python через REST/gRPC https://finamweb.github.io/trade-api-docs/ с токеном по счету
            self.logger.debug(f'Добавлен провайдер Финам {provider["client_id"]}')
        self.provider = list(self.providers.values())[0][0]  # Провайдер по умолчанию для работы со справочниками/историей. Первый счет по ключу provider_name
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Финам

    def start(self):
        pass  # TODO Ждем от Финама подписку на бары

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        for provider in self.providers.values():  # Пробегаемся по всем значениям провайдеров
            provider[0].close_channel()  # Закрываем канал перед выходом
