from collections import deque
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
    """Хранилище Финам"""
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

    def __init__(self, provider=FinamPy()):
        super(FNStore, self).__init__()
        self.notifs = deque()  # Уведомления хранилища
        self.provider = provider  # Подключаемся ко всем торговым счетам
        self.new_bars = []  # Новые бары по всем подпискам на тикеры из Финам

    def start(self):
        pass  # TODO Обработчик новых баров по подписке из Финам

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [x for x in iter(self.notifs.popleft, None)]

    def stop(self):
        # TODO Возвращаем обработчик по умолчанию
        self.provider.close_channel()  # Перед выходом закрываем канал
