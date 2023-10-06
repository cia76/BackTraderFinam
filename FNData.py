from datetime import datetime, timezone, timedelta, time
from threading import Thread, Event  # Поток и событие остановки потока получения новых бар по расписанию бир

from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass
from backtrader import TimeFrame, date2num

from BackTraderFinam import FNStore

from FinamPy import FinamPy
from FinamPy.proto.tradeapi.v1.candles_pb2 import DayCandleInterval, IntradayCandleInterval
from google.type.date_pb2 import Date
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict


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
        self.store = FNStore(**kwargs)  # Передаем параметры в хранилище Финам. Может работать самостоятельно, не через хранилище
        self.provider_name = self.p.provider_name if self.p.provider_name else list(self.store.providers.keys())[0]  # Название провайдера, или первое название по ключу name
        self.provider: FinamPy = self.store.providers[self.provider_name][0]  # Провайдер
        self.board, self.symbol = self.provider.dataname_to_board_symbol(self.p.dataname)  # По тикеру получаем код площадки и тикера
        self.timeframe = self.store.timeframe_to_finam_timeframe(self.p.timeframe, self.p.compression)  # Временной интервал
        self.intraday = self.store.is_intraday(self.p.timeframe)  # Является ли заданный временной интервал внутридневным
        self.history_bars = []  # Исторические бары после применения фильтров
        self.new_bars = []  # Новые бары
        self.exit_event = Event()  # Определяем событие выхода из потока
        self.dt_last_open = datetime.min  # Дата и время открытия последнего полученного бара
        self.last_bar_received = False  # Получен последний бар
        self.live_mode = False  # Режим получения баров. False = История, True = Новые бары

    def setenvironment(self, env):
        """Добавление хранилища Алор в cerebro"""
        super(FNData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища Алор в cerebro

    def start(self):
        super(FNData, self).start()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
        self.get_bars()  # Получаем историю
        if len(self.history_bars) > 0:  # Если был получен хотя бы 1 бар
            self.put_notification(self.CONNECTED)  # то отправляем уведомление о подключении и начале получения исторических баров
        if self.p.live_bars:  # Если получаем историю и новые бары
            Thread(target=self.stream_bars).start()  # Создаем и запускаем получение новых бар по расписанию в потоке

    def _load(self):
        """Загружаем бар из истории или новый бар в BackTrader"""
        if not self.p.live_bars:  # Если получаем только историю (self.history_bars)
            if len(self.history_bars) == 0:  # Если исторических данных нет / Все исторические данные получены
                self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических баров
                return False  # Больше сюда заходить не будем
            bar = self.history_bars[0]  # Берем первый бар из выборки, с ним будем работать
            self.history_bars.remove(bar)  # Убираем его из хранилища новых баров
        else:  # Если получаем историю и новые бары (self.new_bars)
            if len(self.new_bars) == 0:  # Если в хранилище никаких новых баров нет
                return None  # то нового бара нет, будем заходить еще
            self.last_bar_received = len(self.new_bars) == 1  # Если в хранилище остался 1 бар, то мы будем получать последний возможный бар
            bar = self.new_bars[0]  # Берем первый бар из хранилища
            self.new_bars.remove(bar)  # Убираем его из хранилища
            if not self.is_bar_valid(bar):  # Если бар не соответствует всем условиям выборки
                return None  # то пропускаем бар, будем заходить еще
            dt_open = self.get_bar_open_date_time(bar)  # Дата и время открытия бара
            if dt_open <= self.dt_last_open:  # Если пришел бар из прошлого (дата открытия меньше последней даты открытия)
                return None  # то пропускаем бар, будем заходить еще
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            if self.last_bar_received and not self.live_mode:  # Если получили последний бар и еще не находимся в режиме получения новых баров (LIVE)
                self.put_notification(self.LIVE)  # Отправляем уведомление о получении новых баров
                self.live_mode = True  # Переходим в режим получения новых баров (LIVE)
            elif self.live_mode and not self.last_bar_received:  # Если находимся в режиме получения новых баров (LIVE)
                self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) баров
                self.live_mode = False  # Переходим в режим получения истории
        # Все проверки пройдены. Записываем полученный исторический/новый бар
        self.lines.datetime[0] = date2num(self.get_bar_open_date_time(bar))  # DateTime
        self.lines.open[0] = self.provider.finam_price_to_price(self.board, self.symbol, self.get_bar_price(bar['open']))  # Open
        self.lines.high[0] = self.provider.finam_price_to_price(self.board, self.symbol, self.get_bar_price(bar['high']))  # High
        self.lines.low[0] = self.provider.finam_price_to_price(self.board, self.symbol, self.get_bar_price(bar['low']))  # Low
        self.lines.close[0] = self.provider.finam_price_to_price(self.board, self.symbol, self.get_bar_price(bar['close']))  # Close
        self.lines.volume[0] = int(bar['volume'])  # Volume
        self.lines.openinterest[0] = 0  # Открытый интерес в Финам не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        super(FNData, self).stop()
        if self.p.live_bars:  # Если получаем историю и новые бары
            self.exit_event.set()  # то отменяем расписание
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых баров
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    # Получение истории

    def get_bars(self) -> None:
        """Получение истории группами по 500 бар / 365 дней"""
        interval = IntradayCandleInterval(count=500) if self.intraday else DayCandleInterval(count=500)  # Максимальное кол-во баров для истории 500
        if self.p.fromdate:  # Если задана дата и время начала интервала
            fromdate_utc = self.provider.msk_to_utc_datetime(self.p.fromdate, True) if self.intraday else self.p.fromdate.replace(tzinfo=timezone.utc)  # то для интрадея переводим ее в UTC, иначе, берем дату без изменения
        else:  # Если дата и время начала интервала не задана
            fromdate_utc = datetime(1990, 1, 1, tzinfo=timezone.utc)  # то берем дату, когда никакой тикер еще не торговался
        if self.p.todate:  # Если задана дата и время окончания интервала
            todate_utc = self.provider.msk_to_utc_datetime(self.p.todate, True) if self.intraday else self.p.todate.replace(tzinfo=timezone.utc)  # то для интрадея переводим ее в UTC, иначе, берем дату без изменения
        else:  # Если дата и время окончания интервала не задана
            todate_utc = datetime.utcnow().replace(tzinfo=timezone.utc)  # то берем текущую дату и время UTC
        while True:  # Будем получать бары пока не получим все
            td = timedelta(days=30) if self.intraday else timedelta(days=365)  # Внутри дня максимальный запрос за 30 дней. Для дней и выше - 365 дней
            todate_min_utc = min(todate_utc, fromdate_utc + td)  # Максимум, можем делать запросы за 365 дней
            from_ = getattr(interval, 'from')  # Т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
            to_ = getattr(interval, 'to')  # Аналогично будем работать с атрибутом to для единообразия
            if self.intraday:  # Для интрадея datetime -> Timestamp
                from_.seconds = Timestamp(seconds=int(fromdate_utc.timestamp())).seconds
                to_.seconds = Timestamp(seconds=int(todate_min_utc.timestamp())).seconds
            else:  # Для дневных интервалов и выше datetime -> Date
                date_from = Date(year=fromdate_utc.year, month=fromdate_utc.month, day=fromdate_utc.day)
                from_.year = date_from.year
                from_.month = date_from.month
                from_.day = date_from.day
                date_to = Date(year=todate_min_utc.year, month=todate_min_utc.month, day=todate_min_utc.day)
                to_.year = date_to.year
                to_.month = date_to.month
                to_.day = date_to.day
            print(fromdate_utc, todate_min_utc)  # Для отладки
            history_bars = MessageToDict(self.provider.get_intraday_candles(self.board, self.symbol, self.timeframe, interval) if self.intraday else
                                         self.provider.get_day_candles(self.board, self.symbol, self.timeframe, interval),
                                         including_default_value_fields=True)['candles']  # Получаем бары, переводим в словарь/список
            if len(history_bars) == 0:  # Если новых бар нет
                fromdate_utc = todate_min_utc + timedelta(minutes=1) if self.intraday else todate_min_utc + timedelta(days=1)  # то начало интервала смещаем на возможный следующий бар по UTC
            else:  # Если получили новые бары
                last_bar_utc = self.provider.msk_to_utc_datetime(self.get_bar_open_date_time(history_bars[-1]), True)  # Дата и время открытия последнего бара по UTC
                fromdate_utc = last_bar_utc + timedelta(minutes=1) if self.intraday else last_bar_utc + timedelta(days=1)  # Смещаем время на возможный следующий бар по UTC
                for bar in history_bars:  # Пробегаемся по всем полученным барам
                    if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                        self.history_bars.append(bar)  # то добавляем бар
            if fromdate_utc >= todate_utc:  # Если задана дата окончания интервала, и она не позже даты начала
                break  # то выходим из цикла получения баров

    # Расписание

    def stream_bars(self) -> None:
        """Поток получения новых бар по расписанию биржи"""
        time_frame = self.store.timeframe_to_timedelta(self.p.timeframe, self.p.compression)  # Разница во времени между барами
        interval = IntradayCandleInterval(count=1) if self.intraday else DayCandleInterval(count=1)  # Принимаем последний завершенный бар
        while True:
            market_datetime_now = self.p.schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
            trade_bar_open_datetime = self.p.schedule.get_trade_bar_open_datetime(market_datetime_now, time_frame)  # Дата и время бара, который будем получать
            trade_bar_request_datetime = self.p.schedule.get_trade_bar_request_datetime(trade_bar_open_datetime, time_frame)  # Дата и время запроса бара на бирже
            sleep_time_secs = (trade_bar_request_datetime - market_datetime_now + self.p.schedule.delta).total_seconds()  # Время ожидания в секундах
            exit_event_set = self.exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
            if exit_event_set:  # Если произошло событие выхода из потока
                self.provider.close_channel()  # Закрываем канал перед выходом
                return  # Выходим из потока, дальше не продолжаем
            trade_bar_open_datetime_utc = self.p.schedule.msk_to_utc_datetime(trade_bar_open_datetime)  # Дата и время бара в UTC
            from_ = getattr(interval, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
            if self.intraday:  # Для интрадея datetime -> Timestamp
                seconds_from = self.p.schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время бара в timestamp UTC
                date_from = Timestamp(seconds=seconds_from)  # Дата и время бара в Google Timestamp UTC
                from_.seconds = date_from.seconds
            else:  # Для дневных интервалов и выше datetime -> Date
                date_from = Date(year=trade_bar_open_datetime_utc.year, month=trade_bar_open_datetime_utc.month, day=trade_bar_open_datetime_utc.day)
                from_.year = date_from.year
                from_.month = date_from.month
                from_.day = date_from.day
            bars = MessageToDict(self.provider.get_intraday_candles(self.board, self.symbol, self.timeframe, interval) if self.intraday else
                                 self.provider.get_day_candles(self.board, self.symbol, self.timeframe, interval),
                                 including_default_value_fields=True)['candles']  # Получаем бары, переводим в словарь/список
            if len(bars) == 0:  # Если новых бар нет
                continue  # Будем получать следующий бар
            self.new_bars.append(bars[0])  # Получаем первый (завершенный) бар

    # Функции

    def is_bar_valid(self, bar) -> bool:
        """Проверка бара на соответствие условиям выборки"""
        dt_open = self.get_bar_open_date_time(bar)  # Дата и время открытия бара
        if self.p.sessionstart != time.min and dt_open.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            return False  # то бар не соответствует условиям выборки
        dt_close = self.get_bar_close_date_time(dt_open)  # Дата и время закрытия бара
        if self.p.sessionend != time(23, 59, 59, 999990) and dt_close.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            return False  # то бар не соответствует условиям выборки
        high = self.provider.finam_price_to_price(self.board, self.symbol, self.get_bar_price(bar['high']))  # High
        low = self.provider.finam_price_to_price(self.board, self.symbol, self.get_bar_price(bar['low']))  # Low
        if not self.p.four_price_doji and high == low:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            return False  # то бар не соответствует условиям выборки
        time_market_now = self.get_finam_date_time_now()  # Текущее биржевое время
        if dt_close > time_market_now and time_market_now.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
            return False  # то бар не соответствует условиям выборки
        return True  # В остальных случаях бар соответствуем условиям выборки

    @staticmethod
    def get_bar_price(bar_price) -> float:
        """Цена бара"""
        return round(int(bar_price['num']) * 10 ** -int(bar_price['scale']), int(bar_price['scale']))

    def get_bar_open_date_time(self, bar) -> datetime:
        """Дата и время открытия бара. Переводим из GMT в MSK для интрадея. Оставляем в GMT для дневок и выше."""
        # Дату/время UTC получаем в формате ISO 8601. Пример: 2023-06-16T20:01:00Z
        # В статье https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date описывается проблема, что Z на конце нужно убирать
        return self.provider.utc_to_msk_datetime(datetime.fromisoformat(bar['timestamp'][:-1])) if self.intraday else \
            datetime(bar['date']['year'], bar['date']['month'], bar['date']['day'])  # Дату/время переводим из UTC в МСК

    def get_bar_close_date_time(self, dt_open, period=1) -> datetime:
        """Дата и время закрытия бара"""
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return dt_open + timedelta(days=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return dt_open + timedelta(weeks=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            year = dt_open.year + (dt_open.month + period - 1) // 12  # Год
            month = (dt_open.month + period - 1) % 12 + 1  # Месяц
            return datetime(year, month, 1)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Years:  # Годовой временной интервал
            return dt_open.replace(year=dt_open.year + period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return dt_open + timedelta(minutes=self.p.compression * period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Seconds:  # Секундный временной интервал
            return dt_open + timedelta(seconds=self.p.compression * period)  # Время закрытия бара

    def get_finam_date_time_now(self) -> datetime:
        """Текущая дата и время МСК"""
        return datetime.now(self.provider.tz_msk).replace(tzinfo=None)  # TODO Нужно получить текущее дату и время с Финама, когда появится в API
