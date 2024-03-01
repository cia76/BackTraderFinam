from datetime import datetime, timezone, timedelta, time
from uuid import uuid4  # Номера расписаний должны быть уникальными во времени и пространстве
from threading import Thread, Event  # Поток и событие остановки потока получения новых бар по расписанию биржи
import os.path
import csv
import logging

from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass
from backtrader import TimeFrame, date2num

from BackTraderFinam import FNStore

from FinamPy.proto.tradeapi.v1.candles_pb2 import DayCandleTimeFrame, DayCandleInterval, IntradayCandleTimeFrame, IntradayCandleInterval
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
        ('account_id', 0),  # Порядковый номер счета
        ('four_price_doji', False),  # False - не пропускать дожи 4-х цен, True - пропускать
        ('schedule', None),  # Расписание работы биржи
        ('live_bars', False),  # False - только история, True - история и новые бары
    )
    datapath = os.path.join('..', '..', 'Data', 'Finam', '')  # Путь сохранения файла истории
    delimiter = '\t'  # Разделитель значений в файле истории. По умолчанию табуляция
    dt_format = '%d.%m.%Y %H:%M'  # Формат представления даты и времени в файле истории. По умолчанию русский формат

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.live_bars

    def __init__(self):
        self.store = FNStore()  # Хранилище Финам
        self.intraday = self.p.timeframe == TimeFrame.Minutes  # Внутридневной временной интервал
        self.board, self.symbol = self.store.provider.dataname_to_board_symbol(self.p.dataname)  # По тикеру получаем код режима торгов и тикера
        self.client_id = self.store.provider.client_ids[self.p.account_id]  # Счет тикера
        self.finam_timeframe = self.bt_timeframe_to_finam_timeframe(self.p.timeframe, self.p.compression)  # Конвертируем временной интервал из BackTrader в Финам
        self.tf = self.bt_timeframe_to_tf(self.p.timeframe, self.p.compression)  # Конвертируем временной интервал из BackTrader для имени файла истории и расписания
        self.file = f'{self.board}.{self.symbol}_{self.tf}'  # Имя файла истории
        self.logger = logging.getLogger(f'FNData.{self.file}')  # Будем вести лог
        self.file_name = f'{self.datapath}{self.file}.txt'  # Полное имя файла истории
        self.history_bars = []  # Исторические бары из файла и истории после проверки на соответствие условиям выборки
        self.guid = None  # Идентификатор подписки/расписания на историю цен
        self.exit_event = Event()  # Определяем событие выхода из потока
        self.dt_last_open = datetime.min  # Дата и время открытия последнего полученного бара
        self.last_bar_received = False  # Получен последний бар
        self.live_mode = False  # Режим получения бар. False = История, True = Новые бары

    def setenvironment(self, env):
        """Добавление хранилища Финам в cerebro"""
        super(FNData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища Финам в cerebro

    def start(self):
        super(FNData, self).start()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) бар
        self.get_bars_from_file()  # Получаем бары из файла
        self.get_bars_from_history()  # Получаем бары из истории
        if len(self.history_bars) > 0:  # Если был получен хотя бы 1 бар
            self.put_notification(self.CONNECTED)  # то отправляем уведомление о подключении и начале получения исторических бар
        if self.p.live_bars:  # Если получаем историю и новые бары
            if self.p.schedule:  # Если получаем новые бары по расписанию
                self.guid = uuid4().hex  # guid расписания
                Thread(target=self.stream_bars).start()  # Создаем и запускаем получение новых бар по расписанию в потоке
            else:  # Если получаем новые бары по подписке
                raise NotImplementedError  # TODO Ждем от Финама подписку на бары

    def _load(self):
        """Загрузка бара из истории или нового бара"""
        if len(self.history_bars) > 0:  # Если есть исторические данные
            bar = self.history_bars.pop(0)  # Берем и удаляем первый бар из хранилища. С ним будем работать
        elif not self.p.live_bars:  # Если получаем только историю (self.history_bars) и исторических данных нет / все исторические данные получены
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических бар
            self.logger.debug('Бары из файла/истории отправлены в ТС. Новые бары получать не нужно. Выход')
            return False  # Больше сюда заходить не будем
        else:  # Если получаем историю и новые бары (self.store.new_bars)
            if len(self.store.new_bars) == 0:  # Если в хранилище никаких новых бар нет
                return None  # то нового бара нет, будем заходить еще
            new_bars = [new_bar for new_bar in self.store.new_bars if new_bar['guid'] == self.guid]  # Смотрим в хранилище новых бар бары с guid подписки
            if len(new_bars) == 0:  # Если новый бар еще не появился
                self.logger.debug('Новых бар нет')
                return None  # то нового бара нет, будем заходить еще
            self.last_bar_received = len(new_bars) == 1  # Если в хранилище остался 1 бар, то мы будем получать последний возможный бар
            if self.last_bar_received:  # Получаем последний возможный бар
                self.logger.debug('Получение последнего возможного на данный момент бара')
            new_bar = new_bars[0]  # Берем первый бар из хранилища
            self.store.new_bars.remove(new_bar)  # Убираем его из хранилища
            new_bar = new_bar['data']  # С данными этого бара будем работать
            dt_open = self.get_bar_open_date_time(new_bar)  # Дата и время открытия бара
            bar = dict(datetime=dt_open,
                       open=self.store.provider.dict_decimal_to_float(new_bar['open']),
                       high=self.store.provider.dict_decimal_to_float(new_bar['high']),
                       low=self.store.provider.dict_decimal_to_float(new_bar['low']),
                       close=self.store.provider.dict_decimal_to_float(new_bar['close']),
                       volume=new_bar['volume'])  # Бар из хранилища новых бар
            if not self.is_bar_valid(bar):  # Если бар не соответствует всем условиям выборки
                return None  # то пропускаем бар, будем заходить еще
            self.logger.debug(f'Сохранение нового бара с {bar["datetime"].strftime(self.dt_format)} в файл')
            self.save_bar_to_file(bar)  # Сохраняем бар в конец файла
            if self.last_bar_received and not self.live_mode:  # Если получили последний бар и еще не находимся в режиме получения новых бар (LIVE)
                self.put_notification(self.LIVE)  # Отправляем уведомление о получении новых бар
                self.live_mode = True  # Переходим в режим получения новых бар (LIVE)
            elif self.live_mode and not self.last_bar_received:  # Если находимся в режиме получения новых бар (LIVE)
                self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) бар
                self.live_mode = False  # Переходим в режим получения истории
        # Все проверки пройдены. Записываем полученный исторический/новый бар
        self.lines.datetime[0] = date2num(bar['datetime'])  # DateTime
        self.lines.open[0] = self.store.provider.finam_price_to_price(self.board, self.symbol, bar['open'])  # Open
        self.lines.high[0] = self.store.provider.finam_price_to_price(self.board, self.symbol, bar['high'])  # High
        self.lines.low[0] = self.store.provider.finam_price_to_price(self.board, self.symbol, bar['low'])  # Low
        self.lines.close[0] = self.store.provider.finam_price_to_price(self.board, self.symbol, bar['close'])  # Close
        self.lines.volume[0] = int(bar['volume'])  # Volume подается как строка. Его обязательно нужно привести к целому
        self.lines.openinterest[0] = 0  # Открытый интерес в Финам не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        super(FNData, self).stop()
        if self.p.live_bars:  # Если была подписка/расписание
            if self.p.schedule:  # Если получаем новые бары по расписанию
                self.exit_event.set()  # то отменяем расписание
            else:  # Если получаем новые бары по подписке
                raise NotImplementedError  # TODO Ждем от Финама подписку на бары
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых бар
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    # Получение бар

    def get_bars_from_file(self) -> None:
        """Получение бар из файла"""
        if not os.path.isfile(self.file_name):  # Если файл не существует
            return  # то выходим, дальше не продолжаем
        self.logger.debug(f'Получение бар из файла {self.file_name}')
        with open(self.file_name) as file:  # Открываем файл на последовательное чтение
            reader = csv.reader(file, delimiter=self.delimiter)  # Данные в строке разделены табуляцией
            next(reader, None)  # Пропускаем первую строку с заголовками
            for csv_row in reader:  # Последовательно получаем все строки файла
                bar = dict(datetime=datetime.strptime(csv_row[0], self.dt_format),
                           open=float(csv_row[1]), high=float(csv_row[2]), low=float(csv_row[3]),
                           close=float(csv_row[4]),
                           volume=int(csv_row[5]))  # Бар из файла
                if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                    self.history_bars.append(bar)  # то добавляем бар
        if len(self.history_bars) > 0:  # Если были получены бары из файла
            self.logger.debug(f'Получено бар из файла: {len(self.history_bars)} с {self.history_bars[0]["datetime"].strftime(self.dt_format)} по {self.history_bars[-1]["datetime"].strftime(self.dt_format)}')

    def get_bars_from_history(self) -> None:
        """Получение бар из истории группами по кол-ву бар и дней"""
        file_history_bars_len = len(self.history_bars)  # Кол-во полученных бар из файла для лога
        interval = IntradayCandleInterval(count=500) if self.intraday else DayCandleInterval(count=500)  # Максимальное кол-во бар для истории 500
        td = timedelta(days=(30 if self.intraday else 365))  # Максимальный запрос за 30 дней для внутридневных интервалов и 1 год (365 дней) для дневных и выше
        todate_utc = datetime.utcnow().replace(tzinfo=timezone.utc)  # Будем получать бары до текущей даты и времени UTC
        if len(self.history_bars) > 0:  # Если были получены бары из файла
            last_date: datetime = self.history_bars[-1]['datetime']  # Дата и время последнего бара по МСК
            next_bar_open_utc = self.store.provider.msk_to_utc_datetime(last_date + timedelta(minutes=1), True) if self.intraday else \
                last_date.replace(tzinfo=timezone.utc) + timedelta(days=1)  # Смещаем время на возможный следующий бар по UTC
        else:  # Если не были получены бары из файла
            next_bar_open_utc = datetime(1990, 1, 1, tzinfo=timezone.utc)  # Берем дату, когда никакой тикер еще не торговался
        from_ = getattr(interval, 'from')  # Т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        to_ = getattr(interval, 'to')  # Аналогично будем работать с атрибутом to для единообразия
        first_request = not os.path.isfile(self.file_name)  # Если файл не существует, то первый запрос будем формировать без даты окончания. Так мы в первом запросе получим первые бары истории
        while True:  # Будем получать бары пока не получим все
            todate_min_utc = min(todate_utc, next_bar_open_utc + td)  # До какой даты можем делать запрос
            if self.intraday:  # Для интрадея datetime -> Timestamp
                from_.seconds = Timestamp(seconds=int(next_bar_open_utc.timestamp())).seconds  # Дата и время начала интервала UTC
                if not first_request:  # Для всех запросов, кроме первого
                    to_.seconds = Timestamp(seconds=int(todate_min_utc.timestamp())).seconds  # Дата и время окончания интервала UTC
                    if from_.seconds == to_.seconds:  # Если дата и время окончания интервала совпадает с датой и временем начала
                        break  # то выходим из цикла получения бар
            else:  # Для дневных интервалов и выше datetime -> Date
                date_from = Date(year=next_bar_open_utc.year, month=next_bar_open_utc.month, day=next_bar_open_utc.day)  # Дата начала интервала UTC
                from_.year = date_from.year
                from_.month = date_from.month
                from_.day = date_from.day
                if not first_request:  # Для всех запросов, кроме первого
                    date_to = Date(year=todate_min_utc.year, month=todate_min_utc.month, day=todate_min_utc.day)  # Дата окончания интервала UTC
                    if date_to == date_from:  # Если дата окончания интервала совпадает с датой начала
                        break  # то выходим из цикла получения бар
                    to_.year = date_to.year
                    to_.month = date_to.month
                    to_.day = date_to.day
            if first_request:  # Для первого запроса
                first_request = False  # далее будем ставить в запросы дату окончания интервала
            self.logger.debug(f'Запрос с {next_bar_open_utc} по {todate_min_utc}')
            response = (self.store.provider.get_intraday_candles(self.board, self.symbol, self.finam_timeframe, interval) if self.intraday else
                        self.store.provider.get_day_candles(self.board, self.symbol, self.finam_timeframe, interval))  # Получаем ответ на запрос бар
            if not response:  # Если в ответ ничего не получили
                self.logger.warning('Ошибка запроса бар из истории')
                return  # то выходим, дальше не продолжаем
            else:  # Если в ответ пришли бары
                response_dict = MessageToDict(response, including_default_value_fields=True)  # Переводим в словарь из JSON
                if 'candles' not in response_dict:  # Если бар нет в словаре
                    self.logger.error(f'Бар (candles) нет в словаре {response_dict}')
                    return  # то выходим, дальше не продолжаем
            new_bars_dict = response_dict['candles']  # Получаем все бары из Finam
            if len(new_bars_dict) > 0:  # Если пришли новые бары
                first_bar_open_dt = self.get_bar_open_date_time(new_bars_dict[0])  # Дату и время первого полученного бара переводим из UTC в МСК
                last_bar_open_dt = self.get_bar_open_date_time(new_bars_dict[-1])  # Дату и время последнего полученного бара переводим из UTC в МСК
                self.logger.debug(f'Получены бары с {first_bar_open_dt} по {last_bar_open_dt}')
                for new_bar in new_bars_dict:  # Пробегаемся по всем полученным барам
                    bar = dict(datetime=self.get_bar_open_date_time(new_bar),
                               open=self.store.provider.dict_decimal_to_float(new_bar['open']),
                               high=self.store.provider.dict_decimal_to_float(new_bar['high']),
                               low=self.store.provider.dict_decimal_to_float(new_bar['low']),
                               close=self.store.provider.dict_decimal_to_float(new_bar['close']),
                               volume=new_bar['volume'])
                    if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                        self.history_bars.append(bar)  # то добавляем бар
                        self.save_bar_to_file(bar)  # и сохраняем его в файл
                last_bar_open_utc = self.store.provider.msk_to_utc_datetime(last_bar_open_dt, True) if self.intraday else \
                    last_bar_open_dt.replace(tzinfo=timezone.utc)  # Дата и время открытия последнего бара UTC
                next_bar_open_utc = last_bar_open_utc + timedelta(minutes=1) if self.intraday else last_bar_open_utc + timedelta(days=1)  # Смещаем время на возможный следующий бар UTC
            else:  # Если новых бар нет
                next_bar_open_utc = todate_min_utc + timedelta(minutes=1) if self.intraday else todate_min_utc + timedelta(days=1)  # то смещаем время на возможный следующий бар UTC
            if next_bar_open_utc > todate_utc:  # Если пройден весь интервал
                break  # то выходим из цикла получения бар
        if len(self.history_bars) - file_history_bars_len > 0:  # Если получены бары из истории
            self.logger.debug(f'Получено бар из истории: {len(self.history_bars) - file_history_bars_len} с {self.history_bars[file_history_bars_len]["datetime"].strftime(self.dt_format)} по {self.history_bars[-1]["datetime"].strftime(self.dt_format)}')
        else:  # Бары из истории не получены
            self.logger.debug('Из истории новых бар не получено')

    def stream_bars(self) -> None:
        """Поток получения новых бар по расписанию биржи"""
        self.logger.debug('Запуск получения новых бар по расписанию')
        interval = IntradayCandleInterval(count=1) if self.intraday else DayCandleInterval(count=1)  # Принимаем последний завершенный бар
        from_ = getattr(interval, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        while True:
            market_datetime_now = self.p.schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
            trade_bar_open_datetime = self.p.schedule.trade_bar_open_datetime(market_datetime_now, self.tf)  # Дата и время открытия бара, который будем получать
            trade_bar_request_datetime = self.p.schedule.trade_bar_request_datetime(market_datetime_now, self.tf)  # Дата и время запроса бара на бирже
            sleep_time_secs = (trade_bar_request_datetime - market_datetime_now).total_seconds()  # Время ожидания в секундах
            self.logger.debug(f'Получение новых бар с {trade_bar_open_datetime.strftime(self.dt_format)} по расписанию в {trade_bar_request_datetime.strftime(self.dt_format)}. Ожидание {sleep_time_secs} с')
            exit_event_set = self.exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
            if exit_event_set:  # Если произошло событие выхода из потока
                self.logger.warning('Отмена получения новых бар по расписанию')
                return  # то выходим из потока, дальше не продолжаем
            if self.intraday:  # Для интрадея datetime -> Timestamp
                seconds_from = self.p.schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время бара в timestamp UTC
                date_from = Timestamp(seconds=seconds_from)  # Дата и время начала интервала UTC
                from_.seconds = date_from.seconds
            else:  # Для дневных интервалов и выше datetime -> Date
                trade_bar_open_dt_utc = trade_bar_open_datetime  # Дата бара передается с нулевым временем. Поэтому, конвертировать в UTC не нужно
                date_from = Date(year=trade_bar_open_dt_utc.year, month=trade_bar_open_dt_utc.month, day=trade_bar_open_dt_utc.day)  # Дата начала интервала UTC
                from_.year = date_from.year
                from_.month = date_from.month
                from_.day = date_from.day
            response = self.store.provider.get_intraday_candles(self.board, self.symbol, self.finam_timeframe, interval) if self.intraday else \
                self.store.provider.get_day_candles(self.board, self.symbol, self.finam_timeframe, interval)  # Получаем внутридневные или дневные бары
            if not response:  # Если в ответ ничего не получили
                self.logger.warning('Ошибка запроса бар из истории по расписанию')
                continue  # то будем получать следующий бар
            response_dict = MessageToDict(response, including_default_value_fields=True)  # Получаем бары, переводим в словарь/список
            if 'candles' not in response_dict:  # Если бар нет в словаре
                self.logger.warning(f'Бар (candles) нет в истории по расписанию {response_dict}')
                continue  # то будем получать следующий бар
            bars = response_dict['candles']  # Последний сформированный и текущий несформированный (если имеется) бары
            if len(bars) == 0:  # Если бары не получены
                self.logger.warning('Новые бары по расписанию не получены')
                continue  # то будем получать следующий бар
            bar = bars[0]  # Получаем первый (завершенный) бар
            self.logger.debug('Получен бар по расписанию')
            self.store.new_bars.append(dict(guid=self.guid, data=bar))  # Добавляем в хранилище новых бар

    # Функции

    @staticmethod
    def bt_timeframe_to_finam_timeframe(timeframe, compression=1):
        """Перевод временнОго интервала из BackTrader в Финам

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
    def bt_timeframe_to_tf(timeframe, compression=1) -> str:
        """Перевод временнОго интервала из BackTrader для имени файла истории и расписания https://ru.wikipedia.org/wiki/Таймфрейм

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал для имени файла истории и расписания
        """
        if timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return f'M{compression}'
        # Часовой график f'H{compression}' заменяем минутным. Пример: H1 = M60
        elif timeframe == TimeFrame.Days:  # Дневной временной интервал
            return f'D1'
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return f'W1'
        elif timeframe == TimeFrame.Months:  # Месячный временной интервал
            return f'MN1'
        elif timeframe == TimeFrame.Years:  # Годовой временной интервал
            return f'Y1'
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

    def get_bar_open_date_time(self, bar) -> datetime:
        """Дата и время открытия бара. Переводим из GMT в MSK для интрадея. Оставляем в GMT для дневок и выше."""
        # Дату/время UTC получаем в формате ISO 8601. Пример: 2023-06-16T20:01:00Z
        # В статье https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date описывается проблема, что Z на конце нужно убирать
        return self.store.provider.utc_to_msk_datetime(
            datetime.fromisoformat(bar['timestamp'][:-1])) if self.intraday else \
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

    def is_bar_valid(self, bar) -> bool:
        """Проверка бара на соответствие условиям выборки"""
        dt_open = bar['datetime']  # Дата и время открытия бара МСК
        if dt_open <= self.dt_last_open:  # Если пришел бар из прошлого (дата открытия меньше последней даты открытия)
            self.logger.debug(f'Дата/время открытия бара {dt_open} <= последней даты/времени открытия {self.dt_last_open}')
            return False  # то бар не соответствует условиям выборки
        if self.p.fromdate and dt_open < self.p.fromdate or self.p.todate and dt_open > self.p.todate:  # Если задан диапазон, а бар за его границами
            # self.logger.debug(f'Дата/время открытия бара {dt_open} за границами диапазона {self.p.fromdate} - {self.p.todate}')
            return False  # то бар не соответствует условиям выборки
        if self.p.sessionstart != time.min and dt_open.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            self.logger.debug(f'Дата/время открытия бара {dt_open} до начала торговой сессии {self.p.sessionstart}')
            return False  # то бар не соответствует условиям выборки
        dt_close = self.get_bar_close_date_time(dt_open)  # Дата и время закрытия бара
        if self.p.sessionend != time(23, 59, 59, 999990) and dt_close.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            self.logger.debug(f'Дата/время открытия бара {dt_open} после окончания торговой сессии {self.p.sessionend}')
            return False  # то бар не соответствует условиям выборки
        if not self.p.four_price_doji and bar['high'] == bar['low']:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            self.logger.debug(f'Бар {dt_open} - дожи 4-х цен')
            return False  # то бар не соответствует условиям выборки
        time_market_now = self.get_finam_date_time_now()  # Текущее биржевое время
        if dt_close > time_market_now and time_market_now.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
            self.logger.debug(f'Дата/время {dt_close} закрытия бара еще не наступило')
            return False  # то бар не соответствует условиям выборки
        self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
        return True  # В остальных случаях бар соответствуем условиям выборки

    def save_bar_to_file(self, bar) -> None:
        """Сохранение бара в конец файла"""
        if not os.path.isfile(self.file_name):  # Существует ли файл
            self.logger.warning(f'Файл {self.file_name} не найден и будет создан')
            with open(self.file_name, 'w', newline='') as file:  # Создаем файл
                writer = csv.writer(file, delimiter=self.delimiter)  # Данные в строке разделены табуляцией
                writer.writerow(bar.keys())  # Записываем заголовок в файл
        with open(self.file_name, 'a', newline='') as file:  # Открываем файл на добавление в конец. Ставим newline, чтобы в Windows не создавались пустые строки в файле
            writer = csv.writer(file, delimiter=self.delimiter)  # Данные в строке разделены табуляцией
            csv_row = bar.copy()  # Копируем бар для того, чтобы изменить формат даты
            csv_row['datetime'] = csv_row['datetime'].strftime(self.dt_format)  # Приводим дату к формату файла
            writer.writerow(csv_row.values())  # Записываем бар в конец файла
            self.logger.debug(f'В файл {self.file_name} записан бар на {csv_row["datetime"]}')

    def get_finam_date_time_now(self) -> datetime:
        """Текущая дата и время МСК"""
        # TODO Получить текущее дату и время с Финама, когда появится в API
        return datetime.now(self.store.provider.tz_msk).replace(tzinfo=None)
