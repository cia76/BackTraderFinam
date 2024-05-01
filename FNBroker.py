from typing import Union  # Объединение типов
import collections
from datetime import datetime
import logging

from backtrader import BrokerBase, Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.utils.py3 import with_metaclass

from BackTraderFinam import FNStore, FNData

from FinamPy.proto.common_pb2 import BUY_SELL_BUY, BUY_SELL_SELL, OrderValidBefore, OrderValidBeforeType
from FinamPy.proto.orders_pb2 import OrderStatus
from FinamPy.proto.stops_pb2 import StopLoss, StopQuantity, StopQuantityUnits
from FinamPy.proto.events_pb2 import OrderEvent, PortfolioEvent


# noinspection PyArgumentList
class MetaFNBroker(BrokerBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaFNBroker, self).__init__(name, bases, dct)  # Инициализируем класс брокера
        FNStore.BrokerCls = self  # Регистрируем класс брокера в хранилище Финам


# noinspection PyProtectedMember,PyArgumentList,PyUnusedLocal
class FNBroker(with_metaclass(MetaFNBroker, BrokerBase)):
    """Брокер Финам"""
    logger = logging.getLogger('FNBroker')  # Будем вести лог

    def __init__(self):
        super(FNBroker, self).__init__()
        self.store = FNStore()  # Хранилище Финам
        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках
        self.startingcash = self.cash = 0  # Стартовые и текущие свободные средства по счету
        self.startingvalue = self.value = 0  # Стартовая и текущая стоимость позиций
        self.positions = collections.defaultdict(Position)  # Список позиций
        self.orders = collections.OrderedDict()  # Список заявок, отправленных на биржу
        self.ocos = {}  # Список связанных заявок (One Cancel Others)
        self.pcs = collections.defaultdict(collections.deque)  # Очередь всех родительских/дочерних заявок (Parent - Children)

        self.store.provider.on_order = self.on_order  # Обработка заявок
        # self.store.provider.on_portfolio = self.on_portfolio  # Обработка портфеля
        self.order_trade_request_id = self.store.provider.subscribe_order_trade(list(self.store.provider.client_ids))  # Подписываемся на заявки/сделки по счету
        # TODO Ждем подписку на портфель

    def start(self):
        super(FNBroker, self).start()
        self.get_all_active_positions()  # Получаем все активные позиции

    def getcash(self, client_id=None):
        """Свободные средства по счету, по всем счетам"""
        cash = 0  # Будем набирать свободные средства
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            if client_id:  # Если считаем свободные средства по счету
                cash = next((position.price for key, position in self.positions.items() if key[0] == client_id and not key[1]), None)  # Денежная позиция по портфелю/рынку
            else:  # Если считаем свободные средства по всем счетам
                cash = sum([position.price for key, position in self.positions.items() if not key[1]])  # Сумма всех денежных позиций
                self.cash = cash  # Сохраняем текущие свободные средства
        return cash

    def getvalue(self, datas=None, client_id=None):
        """Стоимость позиции, позиций по счету, всех позиций"""
        value = 0  # Будем набирать стоимость позиций
        if self.store.BrokerCls:  # Если брокер есть в хранилище
            if datas:  # Если считаем стоимость позиции/позиций
                data: FNData  # Данные Финам
                for data in datas:  # Пробегаемся по всем тикерам
                    position = self.positions[(data.client_id, data.board, data.symbol)]  # Позиция по тикеру
                    value += position.price * position.size  # Добавляем стоимость позиции по тикеру
            elif client_id:  # Если считаем свободные средства по счету
                value = sum([position.price * position.size for key, position in self.positions.items() if key[0] == client_id and key[1]])  # Стоимость позиций по портфелю/бирже
            else:  # Если считаем стоимость всех позиций
                value = sum([position.price * position.size for key, position in self.positions.items() if key[1]])  # Стоимость всех позиций
                self.value = value  # Сохраняем текущую стоимость позиций
        return value

    def getposition(self, data: FNData):
        """Позиция по тикеру
        Используется в strategy.py для закрытия (close) и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        return self.positions[(data.client_id, data.board, data.symbol)]  # Получаем позицию по тикеру или нулевую позицию, если тикера в списке позиций нет

    def buy(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, True, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера о принятии/отклонении заявки на бирже
        return order

    def sell(self, owner, data, size, price=None, plimit=None, exectype=None, valid=None, tradeid=0, oco=None, trailamount=None, trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.create_order(owner, data, size, price, plimit, exectype, valid, oco, parent, transmit, False, **kwargs)
        self.notifs.append(order.clone())  # Уведомляем брокера о принятии/отклонении заявки на бирже
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.cancel_order(order)

    def get_notification(self):
        return self.notifs.popleft() if self.notifs else None  # Удаляем и возвращаем крайний левый элемент списка уведомлений или ничего

    def next(self):
        self.notifs.append(None)  # Добавляем в список уведомлений пустой элемент

    def stop(self):
        super(FNBroker, self).stop()
        self.store.provider.on_order = self.store.provider.default_handler  # Обработка заявок
        # self.store.provider.on_portfolio = self.store.provider.default_handler  # Обработка портфеля
        self.store.provider.unsubscribe_order_trade(self.order_trade_request_id)  # Отменяем подписки на заявки/сделки
        # TODO Ждем отмену подписки на портфель
        self.store.BrokerCls = None  # Удаляем класс брокера из хранилища

    # Функции

    def get_all_active_positions(self):
        """Все активные позиции по всем счетам"""
        cash = 0  # Будем набирать свободные средства
        value = 0  # Будем набирать стоимость позиций
        for client_id in self.store.provider.client_ids:  # Пробегаемся по всем счетам
            response = self.store.provider.get_portfolio(client_id)  # Портфель по счету
            for money in response.money:  # Пробегаемся по всем свободным средствам
                cross_rate = next(item.cross_rate for item in response.currencies if item.name == money.currency)  # Кол-во рублей за единицу валюты
                price = money.balance * cross_rate  # Сумма в рублях
                cash += price  # Увеличиваем общий размер свободных средств
                self.positions[(client_id, None, money.currency)] = Position(1, price)  # Сохраняем в списке открытых позиций
            for position in response.positions:  # Пробегаемся по всем активным позициям счета
                si = next(security for security in self.store.provider.symbols.securities if security.market == position.market and security.code == position.security_code)  # Поиск тикера по рынку
                cross_rate = next(item.cross_rate for item in response.currencies if item.name == position.currency)  # Кол-во рублей за единицу валюты
                size = position.balance  # Кол-во в штуках
                price = position.average_price * cross_rate  # Цена входа
                value += price * size  # Увеличиваем общий размер стоимости позиций
                self.positions[(client_id, si.board, si.code)] = Position(size, price)  # Сохраняем в списке открытых позиций
        self.cash = cash  # Сохраняем текущие свободные средства
        self.value = value  # Сохраняем текущую стоимость позиций

    def get_order(self, transaction_id) -> Union[Order, None]:
        """Заявка BackTrader по номеру транзакции
        Пробегаемся по всем заявкам на бирже. Если нашли совпадение по номеру транзакции, то возвращаем заявку BackTrader. Иначе, ничего не найдено

        :param int transaction_id: Номер транзакции
        :return: Заявка BackTrader или None
        """
        return next((order for order in self.orders.values() if order.info['transaction_id'] == transaction_id), None)

    def create_order(self, owner, data: FNData, size, price=None, plimit=None, exectype=None, valid=None, oco=None, parent=None, transmit=True, simulated=False, is_buy=True, **kwargs):
        """Создание заявки. Привязка параметров счета и тикера. Обработка связанных и родительской/дочерних заявок
        Даполнительные параметры передаются через **kwargs:
        - account_id - Порядковый номер счета
        """
        order = BuyOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, simulated=simulated, transmit=transmit) if is_buy \
            else SellOrder(owner=owner, data=data, size=size, price=price, pricelimit=plimit, exectype=exectype, valid=valid, oco=oco, parent=parent, simulated=simulated, transmit=transmit)  # Заявка на покупку/продажу
        order.addcomminfo(self.getcommissioninfo(data))  # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addinfo(**kwargs)  # Передаем в заявку все дополнительные параметры, в т.ч. account_id
        if order.exectype in (Order.Close, Order.StopTrail, Order.StopTrailLimit, Order.Historical):  # Эти типы заявок не реализованы
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.board}.{data.symbol} отклонена. Работа с заявками {order.exectype} не реализована')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        client_id = self.store.provider.client_ids[order.info['account_id']] if 'account_id' in order.info else data.client_id  # Торговый счет из заявки/тикера
        order.addinfo(client_id=client_id)  # Сохраняем в заявке
        if order.exectype != Order.Market and not order.price:  # Если цена заявки не указана для всех заявок, кроме рыночной
            price_type = 'Лимитная' if order.exectype == Order.Limit else 'Стоп'  # Для стоп заявок это будет триггерная (стоп) цена
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.board}.{data.symbol} отклонена. {price_type} цена (price) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if order.exectype == Order.StopLimit and not order.pricelimit:  # Если лимитная цена не указана для стоп-лимитной заявки
            self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.board}.{data.symbol} отклонена. Лимитная цена (pricelimit) не указана для заявки типа {order.exectype}')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if oco:  # Если есть связанная заявка
            self.ocos[order.ref] = oco.ref  # то заносим в список связанных заявок
        if not transmit or parent:  # Для родительской/дочерних заявок
            parent_ref = getattr(order.parent, 'ref', order.ref)  # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            if order.ref != parent_ref and parent_ref not in self.pcs:  # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
                self.logger.warning(f'Постановка заявки {order.ref} по тикеру {data.board}.{data.symbol} отклонена. Родительская заявка не найдена')
                order.reject(self)  # то отклоняем заявку
                self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
                return order  # Возвращаем отклоненную заявку
            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)
        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            if not parent:  # Для обычных заявок
                return self.place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                self.notifs.append(order.clone())  # Удедомляем брокера о создании новой заявки
                return self.place_order(order.parent)  # Отправляем родительскую заявку на биржу
        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False)
        return order  # то возвращаем созданную заявку со статусом Created. На биржу ее пока не ставим

    def place_order(self, order: Order):
        """Отправка заявки на биржу"""
        client_id = order.info['client_id']  # Торговый счет
        board = order.data.board  # Код режима торгов
        symbol = order.data.symbol  # Тикер
        buy_sell = BUY_SELL_BUY if order.isbuy() else BUY_SELL_SELL  # Покупка/продажа
        si = self.store.provider.get_symbol_info(board, symbol)  # Информация о тикере
        quantity = abs(order.size // si.lot_size)  # Размер позиции в лотах. В Финам всегда передается положительный размер лота
        self.logger.debug(f'order.size={order.size}, si.lot_size={si.lot_size}, quantity={quantity}')  # Для отладки правильно установленного лота
        response = None  # Результат запроса
        if order.exectype == Order.Market:  # Рыночная заявка
            response = self.store.provider.new_order(client_id, board, symbol, buy_sell, quantity)
        elif order.exectype == Order.Limit:  # Лимитная заявка
            price = self.store.provider.price_to_finam_price(board, symbol, order.price)  # Лимитная цена
            response = self.store.provider.new_order(client_id, board, symbol, buy_sell, quantity, price=price)
        elif order.exectype == Order.Stop:  # Стоп заявка
            price = self.store.provider.price_to_finam_price(board, symbol, order.price)  # Стоп цена
            response = self.store.provider.new_stop(
                client_id, board, symbol, buy_sell,
                StopLoss(activation_price=price, market_price=True, quantity=StopQuantity(units=StopQuantityUnits.STOP_QUANTITY_UNITS_LOTS, value=quantity), use_credit=False),
                valid_before=OrderValidBefore(type=OrderValidBeforeType.ORDER_VALID_BEFORE_TYPE_TILL_CANCELLED))
        elif order.exectype == Order.StopLimit:  # Стоп-лимитная заявка
            price = self.store.provider.price_to_finam_price(board, symbol, order.price)  # Стоп цена
            pricelimit = self.store.provider.price_to_finam_price(board, symbol, order.pricelimit)  # Лимитная цена
            response = self.store.provider.new_stop(
                client_id, board, symbol, buy_sell, None,
                StopLoss(activation_price=price, market_price=False, price=pricelimit, quantity=StopQuantity(units=StopQuantityUnits.STOP_QUANTITY_UNITS_LOTS, value=quantity), use_credit=False),
                valid_before=OrderValidBefore(type=OrderValidBeforeType.ORDER_VALID_BEFORE_TYPE_TILL_CANCELLED))
        order.submit(self)  # Отправляем заявку на биржу (Order.Submitted)
        self.notifs.append(order.clone())  # Уведомляем брокера об отправке заявки на биржу
        if not response:  # Если при отправке заявки на биржу произошла веб ошибка
            self.logger.warning(f'Постановка заявки по тикеру {board}.{symbol} отклонена. Ошибка веб сервиса')
            order.reject(self)  # то отклоняем заявку
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки
            return order  # Возвращаем отклоненную заявку
        if order.exectype in (Order.Market, Order.Limit):  # Для рыночной и лимитной заявки
            order.addinfo(transaction_id=response.transaction_id)  # Идентификатор транзакции добавляем в заявку
        elif order.exectype in (Order.Stop, Order.StopLimit):  # Для стоп и стоп-лимитной заявки
            order.addinfo(stop_id=response.stop_id)  # Идентификатор стоп заявки добавляем в заявку
        order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        self.orders[order.ref] = order  # Сохраняем заявку в списке заявок, отправленных на биржу
        return order  # Возвращаем заявку

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return  # то выходим, дальше не продолжаем
        client_id = order.info['client_id']  # Торговый счет
        if order.exectype in (Order.Market, Order.Limit):  # Для рыночной и лимитной заявки
            self.store.provider.cancel_order(client_id, order.info['transaction_id'])  # Отмена активной заявки
        elif order.exectype in (Order.Stop, Order.StopLimit):  # Для стоп и стоп-лимитной заявки
            self.store.provider.cancel_stop(client_id, order.info['stop_id'])  # Отмена активной стоп заявки
        return order  # В список уведомлений ничего не добавляем. Ждем события on_order

    def oco_pc_check(self, order):
        """
        Проверка связанных заявок
        Проверка родительской/дочерних заявок
        """
        ocos = self.ocos.copy()  # Пока ищем связанные заявки, они могут измениться. Поэтому, работаем с копией
        for order_ref, oco_ref in ocos.items():  # Пробегаемся по списку связанных заявок
            if oco_ref == order.ref:  # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in ocos.keys():  # Если у этой заявки указана связанная заявка
            oco_ref = ocos[order.ref]  # то получаем номер транзакции связанной заявки
            self.cancel_order(self.orders[oco_ref])  # отменяем связанную заявку

        if not order.parent and not order.transmit and order.status == Order.Completed:  # Если исполнена родительская заявка
            pcs = self.pcs[order.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent:  # Пропускаем первую (родительскую) заявку
                    self.place_order(child)  # Отправляем дочернюю заявку на биржу
        elif order.parent:  # Если исполнена/отменена дочерняя заявка
            pcs = self.pcs[order.parent.ref]  # Получаем очередь родительской/дочерних заявок
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent and child.ref != order.ref:  # Пропускаем первую (родительскую) заявку и исполненную заявку
                    self.cancel_order(child)  # Отменяем дочернюю заявку

    def on_portfolio(self, event: PortfolioEvent):
        """Обработка портфеля"""
        for money in event.money:  # Пробегаемся по всем свободным средствам
            cross_rate = next(currency.cross_rate for currency in event.currencies if currency.name == money.currency)  # Кол-во рублей за единицу валюты
            price = money.balance * cross_rate  # Сумма в рублях
            self.positions[(event.client_id, None, money.currency)] = Position(1, price)  # Сохраняем в списке открытых позиций
        for position in event.positions:  # Пробегаемся по всем активным позициям счета
            si = next(item for item in self.store.provider.symbols.securities if item.market == position.market and item.code == position.security_code)  # Поиск тикера по рынку
            cross_rate = next(currency.cross_rate for currency in event.currencies if currency.name == position.currency)  # Кол-во рублей за единицу валюты
            size = position.balance  # Кол-во в штуках
            price = position.average_price * cross_rate  # Цена входа
            self.positions[(event.client_id, si.board, si.code)] = Position(size, price)  # Сохраняем в списке открытых позиций

    def on_order(self, event: OrderEvent):
        """Обработка заявок"""
        order: Order = self.get_order(event.transaction_id)  # Пытаемся получить заявку по номеру транзакции
        if not order:  # Если заявки нет в BackTrader (не из автоторговли)
            return  # то выходим, дальше не продолжаем
        if event.status == OrderStatus.ORDER_STATUS_NONE:  # Если заявка не выставлена
            pass  # то ничего не делаем, т.к. это просто уведомление о том, что заявка принята
        elif event.status == OrderStatus.ORDER_STATUS_ACTIVE:  # Если заявка выставлена
            pass  # то ничего не делаем, т.к. уведомили о выставлении заявки на этапе ее постановки
        elif event.status == OrderStatus.ORDER_STATUS_CANCELLED:  # Если заявка отменена
            if order.status == order.Canceled:  # Бывает, что Финам дублируем события отмены заявок. Если заявка уже была удалена
                return  # то выходим, дальше не продолжаем
            order.cancel()  # Отменяем существующую заявку
            self.notifs.append(order.clone())  # Уведомляем брокера об отмене заявки
            self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Canceled)
        elif event.status == OrderStatus.ORDER_STATUS_MATCHED:  # Если заявка исполнена
            dt = self.store.provider.utc_to_msk_datetime(datetime.now())  # Перевод текущего времени (другого нет) в московское
            pos = self.getposition(order.data)  # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
            board = order.data.board  # Код режима торгов
            symbol = order.data.symbol  # Тикер
            si = self.store.provider.get_symbol_info(board, symbol)  # Информация о тикере
            size = event.quantity * si.lot_size  # Кол-во в штуках
            if event.buy_sell == BUY_SELL_SELL:  # Если сделка на продажу
                size *= -1  # то кол-во ставим отрицательным
            price = event.price  # Цена исполнения за штуку
            psize, pprice, opened, closed = pos.update(size, price)  # Обновляем размер/цену позиции на размер/цену сделки
            order.execute(dt, size, price, closed, 0, 0, opened, 0, 0, 0, 0, psize, pprice)  # Исполняем заявку в BackTrader
            if order.executed.remsize:  # Если осталось что-то к исполнению
                if order.status != order.Partial:  # Если заявка переходит в статус частичного исполнения (может исполняться несколькими частями)
                    order.partial()  # то заявка частично исполнена
                    self.notifs.append(order.clone())  # Уведомляем брокера о частичном исполнении заявки
            else:  # Если ничего нет к исполнению
                order.completed()  # то заявка полностью исполнена
                self.notifs.append(order.clone())  # Уведомляем брокера о полном исполнении заявки
                # Снимаем oco-заявку только после полного исполнения заявки
                # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
                self.oco_pc_check(order)  # Проверяем связанные и родительскую/дочерние заявки (Completed)
