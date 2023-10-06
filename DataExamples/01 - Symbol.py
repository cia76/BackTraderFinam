from datetime import datetime, date, timedelta, time
from backtrader import Cerebro, TimeFrame
from BackTraderFinam import FNStore  # Хранилище Finam
from FinamPy.Config import Config  # Файл конфигурации
from MarketPy.Schedule import MOEXStocks, MOEXFutures  # Расписания торгов фондового/срочного рынков
import Strategy  # Торговые системы

# Исторические/новые бары тикера
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер в формате <Код биржи MOEX/SPBX>.<Код тикера>
    schedule = MOEXStocks()  # Расписание торгов фондового рынка
    # symbol = 'FUT.SiZ3'  # Для фьючерсов: <Код тикера><Месяц экспирации: 3-H, 6-M, 9-U, 12-Z><Последняя цифра года>
    # schedule = MOEXFutures()  # Расписание торгов срочного рынка
    store = FNStore(providers=[dict(dict(provider_name='finam_trade', client_id=Config.ClientIds[0], access_token=Config.AccessToken))])  # Хранилище Alor
    cerebro = Cerebro(stdstats=False)  # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    today = date.today()  # Сегодняшняя дата без времени
    week_ago = today - timedelta(days=7)  # Дата неделю назад без времени

    # 1. Все исторические недельные бары (eod)
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Weeks)

    # 2. Исторические дневные бары с начала года (eod, fromdate)
    # data = store.getdata(dataname=symbol, fromdate=datetime(today.year, 1, 1))

    # 3. Исторические дневные бары до начала месяца (eod, todate)
    # data = store.getdata(dataname=symbol, todate=datetime(today.year, today.month, 1))

    # 4. Исторические дневные бары за первую половину прошлого года (eod, fromdate, todate)
    # data = store.getdata(dataname=symbol, fromdate=datetime(today.year - 1, 1, 1), todate=datetime(today.year - 1, 7, 1))

    # 5. Исторические часовые бары за последнюю неделю (intraday, fromdate)
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=60, fromdate=week_ago)

    # 6. Исторические 5-и минутные бары первого часа сегодняшней сессии без первой 5-и минутки (intraday, fromdate, todate)
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=datetime(today.year, today.month, today.day, 10, 5), todate=datetime(today.year, today.month, today.day, 10, 55))

    # 7. Исторические 5-и минутные бары первого часа сессиЙ за неделю без первой 5-и минутки (intraday, sessionstart, sessionend)
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=5, fromdate=week_ago, todate=today, sessionstart=time(10, 5), sessionend=time(11, 0))

    # 8. Исторические минутные бары с дожи 4-х цен за неделю (intraday, four_price_doji)
    # data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=week_ago, todate=today, four_price_doji=True)

    # 9. Исторические и новые минутные бары с начала сегодняшней сессии (intraday, schedule, live_bars)
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Minutes, compression=1, fromdate=today, schedule=schedule, live_bars=True)

    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(Strategy.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    # cerebro.plot()  # Рисуем график
