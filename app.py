#app.py
import sys
from collections import defaultdict
from datetime import datetime, timezone
from json import JSONDecodeError

from flask import Flask, render_template, redirect, url_for, request, flash, jsonify
from flask_login import LoginManager, login_user, login_required, logout_user, current_user

from config import Config
from forms import LoginForm, RegistrationForm, WarehouseSettingsForm
from database import db
from models import User
from flask_migrate import Migrate
from kafka import KafkaConsumer, KafkaProducer
import json
import threading


app = Flask(__name__)
app.config.from_object('config.Config')

# Инициализируем продюсер
producer2 = KafkaProducer(
    bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Инициализация расширений
db.init_app(app)
migrate = Migrate(app, db)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

class WarehouseOnlineManager:
    def __init__(self):
        self.active_warehouses = {}
        self.lock = threading.Lock()
        self.data_ready = threading.Event()
        self.consumer = KafkaConsumer(
            Config.KAFKA_WAREHOUSES_ONLINE_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-warehouse-online-v3',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            session_timeout_ms=60000,
            max_poll_interval_ms=300000
        )
        self.thread = threading.Thread(
            target=self.update_warehouses,
            daemon=True
        )
        self.thread.start()

    def update_warehouses(self):
        for message in self.consumer:
            try:
                data = message.value
                print(f" === DEBUF WOM RAW DATA##: {data}")

                if not data:
                    self.consumer.commit()
                    continue

                data = data.copy()

                wh_id = data.get('wh_id')
                metadata = data.get('metadata', {})

                if not wh_id:
                    app.logger.warning("WOM received message without wh_id")
                    self.consumer.commit()
                    continue

                #self.act_wh.add(str(wh_id))

                # Добавляем обработку отсутствующего timestamp
                timestamp_str = data.get('timestamp')
                timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else datetime.utcnow()

                if not wh_id:
                    raise ValueError("Missing warehouse ID in message")

                with self.lock:
                    print(f" === DEBUG LOCK WarehouseOnlineManager")

                    temp = self.active_warehouses.get(wh_id)
                    if temp is not None:
                        current_entry = temp.copy()
                    else:
                        current_entry = None
                    print(f"  *  DEBUG current entry: {current_entry}")

                    new_entry = {
                        'wh_id': wh_id,
                        'last_seen': datetime.now(timezone.utc),
                        'timestamp': timestamp,
                        'metadata': metadata
                    }
                    print(f"  *  DEBUG new entry: {new_entry}")
                    # Rule 3: Если записи нет в кэше - добавляем её
                    if current_entry is None:
                        self.active_warehouses[wh_id] = new_entry.copy()
                        print(f"  >  DEBUG None to full: {self.active_warehouses}")
                        if not self.data_ready.is_set():  # Сигнализируем при первом успешном обновлении
                            self.data_ready.set()
                    else:
                        # Rule 5: Обновляем только при изменении данных
                        if (current_entry['timestamp'] != new_entry['timestamp'] or
                                current_entry['metadata'] != new_entry['metadata'] or
                            current_entry['wh_id'] != new_entry['wh_id']):
                            self.active_warehouses[wh_id] = new_entry.copy()
                            print(f"  >  DEBUG was change: {self.active_warehouses}")
                            if not self.data_ready.is_set():  # Сигнализируем при первом успешном обновлении
                                self.data_ready.set()
                        else:
                            # Обновляем только last_seen если данные не изменились
                            print(f"  *  DEBUG last_seen c/n: {current_entry['last_seen']} / {new_entry['last_seen']}")
                            current_entry['last_seen'] = new_entry['last_seen']
                            self.active_warehouses = current_entry

                    # self.active_warehouses[wh_id] = {
                    #     'wh_id': wh_id,
                    #     'last_seen': datetime.now(timezone.utc),
                    #     'timestamp': timestamp,
                    #     'metadata': metadata  # Сохраняем metadata
                    # }
                    #print(f"we know: {wh_id}")  # Для быстрой отладки

                self.consumer.commit()

            except Exception as e:
                app.logger.error(f"Warehouse online error: {str(e)}")
                app.logger.debug(f"Problematic message: {message.value}")
                try:
                    self.consumer.commit()
                except Exception as commit_err:
                    app.logger.error(f"WOM failed to commit offset after error: {commit_err}")



    def get_warehouses(self):
        with self.lock:
            now = datetime.now(timezone.utc)
            result = [
                {
                    'wh_id': wh_id,
                    'metadata': data.get('metadata', {})  # Передаем metadata
                }.copy()  # Возвращаем словари вместо строк
                for wh_id, data in self.active_warehouses.items()
                if (now - data['last_seen']).total_seconds() < 200
            ]
            print(f"  @  DEBUG WarehouseOnlineManager GET {result}")
            return result.copy()

class GoodsResponseHandler:
    """Обработчик ответов с товарами склада"""

    def __init__(self):
        self.goods_cache = {}
        self.lock = threading.Lock()
        self.data_ready = threading.Event()
        self.consumer = KafkaConsumer(
            Config.KAFKA_GOODS_RESPONSE_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-goods-response-v2',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            session_timeout_ms=120000,
            max_poll_interval_ms=3000000
        )
        self.thread = threading.Thread(
            target=self.process_responses,
            daemon=True
        )
        self.thread.start()

    def process_responses(self):
        for message in self.consumer:
            try:
                data = message.value

                print(f" === DEBUF GRH RAW DATA##: {data}")

                if not data:
                    self.consumer.commit()
                    continue

                data = data.copy()

                print(f" === DEBUF GRH RAW DATA##: {data}")
                if not isinstance(data, dict):
                    print(f"DEBUF LOG Получено некорректное сообщение: {data}")
                    self.consumer.commit()
                    continue

                wh_id = data.get('wh_id')
                goods = data.get('goods')

                if not wh_id:
                    print(f"DEBUF LOG Отсутствует ID склада в сообщении")
                    self.consumer.commit()
                    continue

                if not goods:
                    print(f"DEBUF LOG Отсутствуют товары для склада {wh_id}")
                    self.consumer.commit()
                    continue

                # Валидация структуры данных
                valid_goods = []
                for item in goods:
                    if 'pgd_id' in item and 'quantity' in item:
                        valid_goods.append({
                            'pgd_id': str(item['pgd_id']),
                            'quantity': int(item['quantity'])
                        })

                with self.lock:
                    print(f" === DEBUG LOCK GoodsResponseHandler")

                    temp = self.goods_cache.get(wh_id)
                    if temp is not None:
                        state_goods = temp.copy()
                    else:
                        state_goods = None


                    print(f"  *  DEBUG current entry: {state_goods}")
                    print(f"  *  DEBUG new entry: {valid_goods}")

                    # Rule 3: Если данных нет в кэше - добавляем
                    if state_goods is None:
                        self.goods_cache[wh_id] = valid_goods.copy()
                        print(f"  >  DEBUG None to full: {self.goods_cache}")
                        if not self.data_ready.is_set():  # Сигнализируем при первом успешном обновлении
                            self.data_ready.set()
                    else:
                        # Rule 5: Обновляем только при изменении данных
                        if state_goods != valid_goods:
                            self.goods_cache[wh_id] = valid_goods.copy()
                            print(f"  >  DEBUG was change: {self.goods_cache}")
                            if not self.data_ready.is_set():  # Сигнализируем при первом успешном обновлении
                                self.data_ready.set()

                    #self.goods_cache[wh_id] = valid_goods
                    #print(f"DEBUF-S: goods_cache[{wh_id}] = {valid_goods}")

                self.consumer.commit()

            except StopIteration:
                pass
            except Exception as e:
                app.logger.error(f"Goods response error: {str(e)}")
                try:
                    self.consumer.commit()
                except Exception as commit_err:
                    app.logger.error(f"GRH failed to commit offset after error: {commit_err}")


    def get_goods(self, wh_id):
        with self.lock:
            # Возвращаем копию данных или пустой список, если ключ не найден
            # (self.goods_cache.get(wh_id, [])).copy()
            goods = self.goods_cache.get(wh_id, []).copy()
            print(f"  @  DEBUG GoodsResponseHandler GET {goods}")
            return goods

    def is_ready(self):
        """Проверяет наличие товаров для хотя бы одного склада"""
        with self.lock:
            return any(self.goods_cache.values())

# TODO: Улучшить отрисовку
class WarehouseStateInvoice:
    def __init__(self):
        self.state_invoices = {}
        self.lock = threading.Lock()
        self.data_ready = threading.Event()
        self.consumer = KafkaConsumer(
            Config.KAFKA_STATE_INVOICE_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-warehouse-state-invoice-v1-abc',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=30000  # Увеличьте таймаут, если нужно
        )
        self.thread = threading.Thread(
            target=self.update_state_invoice,
            daemon=True
        )
        self.thread.start()

    def _deserialize_message(self, raw_message):
        """Кастомный десериализатор с обработкой ошибок"""
        try:
            data = json.loads(raw_message.decode('utf-8'))
            # Конвертируем строковый timestamp в datetime
            if 'timestamp' in data:
                ts = data['timestamp'].rstrip('Z')
                data['timestamp'] = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
            return data
        except (UnicodeDecodeError, JSONDecodeError) as e:
            app.logger.error(f"Deserialization error: {str(e)}")
            return {}

    def update_state_invoice(self):
        for message in self.consumer:
            try:
                data = message.value
                print(f" === DEBUF WSI RAW DATA##: {data}")

                if not data:
                    continue

                data = data.copy()

                wh_id = data.get('wh_id')
                invoices = data.get('invoices', [])
                timestamp = data.get('timestamp') or datetime.utcnow().isoformat() #datetime.now(timezone.utc) !!!!!

                if not wh_id:
                    raise ValueError("Missing warehouse ID in message")

                with self.lock:
                    print(f" === DEBUG LOCK WarehouseStateInvoice")

                    temp = self.state_invoices.get(wh_id)
                    if temp is not None:
                        current_state = temp.copy()
                    else:
                        current_state = None

                    print(f"  *  DEBUG current entry: {current_state}")

                    # Формируем новое состояние
                    new_state = {
                        'wh_id': wh_id,
                        'last_seen': datetime.now(timezone.utc),
                        'timestamp': timestamp,
                        'invoices': self._process_invoices(invoices)
                    }
                    print(f"  *  DEBUG new entry: {new_state}")

                    # Rule 3: Если данных нет в кэше - добавляем
                    if current_state is None:
                        self.state_invoices[wh_id] = new_state.copy()
                        print(f"  >  DEBUG None to full: {self.state_invoices}")
                        if not self.data_ready.is_set():  # Сигнализируем при первом успешном обновлении
                            self.data_ready.set()
                    else:
                        # Rule 5: Обновляем только при изменении данных (без учета last_seen)
                        if (current_state['timestamp'] != new_state['timestamp'] or
                                current_state['invoices'] != new_state['invoices']):
                            self.state_invoices[wh_id] = new_state.copy()
                            print(f"  >  DEBUG was change: {self.state_invoices}")
                            if not self.data_ready.is_set():  # Сигнализируем при первом успешном обновлении
                                self.data_ready.set()

                    # self.state_invoices[wh_id] = {
                    #     'wh_id': wh_id,
                    #     'last_seen': datetime.now(timezone.utc),
                    #     'timestamp': timestamp,
                    #     'invoices': self._process_invoices(invoices)
                    # }
                    #print(f"we know: {self.state_invoices}")  # Для быстрой отладки

                self.consumer.commit()

            except Exception as e:
                app.logger.error(f"Warehouse update error: {str(e)}")
                app.logger.debug(f"Problematic message: {message.value}")

    def _process_invoices(self, raw_invoices):
        """Валидация и нормализация структуры накладных"""
        processed = []
        for inv in raw_invoices:
            if not all(key in inv for key in ('invoice_id', 'invoice_type', 'status')):
                continue

            processed.append({
                'invoice_id': inv['invoice_id'],
                'type': inv['invoice_type'],
                'status': inv['status'],
                'created_at': datetime.fromisoformat(inv['created_at']),
                'sender': inv['sender_warehouse'],
                'receiver': inv['receiver_warehouse'],
                'items': [
                    {
                        'pgd_id': item['pgd_id'],
                        'quantity': item['quantity'],
                        'batch': item['batch_number']
                    }
                    for item in inv.get('items', [])
                    if all(k in item for k in ('pgd_id', 'quantity'))
                ]
            })
        return processed

    def get_state_invoice(self):
        with self.lock:
            now = datetime.now(timezone.utc)
            result = [
                {
                    'wh_id': wh_data['wh_id'],
                    'timestamp': wh_data['timestamp'],
                    'invoices': wh_data['invoices']
                }.copy()
                for wh_data in self.state_invoices.values()
                if (now - wh_data['last_seen']).total_seconds() < 200
            ].copy()
            print(f"  @  DEBUG WarehouseStateInvoice GET {result}")
            return result

    def get_invoices_by_status(self, status):
        with self.lock:
            result = [
                invoice
                for wh_data in self.state_invoices.values()
                for invoice in wh_data['invoices']
                if invoice['status'] == status
            ].copy()
            print(f"  @  DEBUG WarehouseStateInvoice GET byS {result}")
            return result

    def is_ready(self):
        with self.lock:
            return bool(self.state_invoices)

# TODO: При заполнении products_cache перестать слушать кафку здесь и выключить поток, не сбросив кэш
class ProductsResponseHandler:
    """Обработчик ответов с товарами """

    def __init__(self):
        self.products_cache = [] #{}
        self.lock = threading.Lock()
        self.data_ready = threading.Event()
        self.consumer = KafkaConsumer(
            app.config['KAFKA_PRODUCT_TOPIC'],
            bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-consumer-group-v1',
            enable_auto_commit=False,
            session_timeout_ms=60000,
            max_poll_interval_ms=300000,
            consumer_timeout_ms=-1
        )
        self.thread = threading.Thread(
            target=self.process_responses,
            daemon=True
        )
        self.thread.start()
        app.logger.info("Kafka consumer connected. Waiting for messages...")

    def process_responses(self):
        for message in self.consumer:
            try:
                products = message.value
                print(f" === DEBUF RAW DATA##: {products}")
                if not isinstance(products, list):
                    app.logger.warning(f"PRH received non-list message: {products}")
                    self.consumer.commit()
                    continue

                if not products:  # Skip empty lists
                    print("Received empty products list, ignoring.")
                    self.consumer.commit()
                    continue

                # Валидация структуры продуктов (простая)
                valid_products = []
                for item in products:
                    if isinstance(item, dict) and 'id' in item and 'name' in item:
                        item['id'] = str(item['id'])  # Приводим ID к строке для унификации
                        valid_products.append(item)
                    else:
                        app.logger.warning(f"PRH invalid product structure in message: {item}")

                products = products.copy()

                with self.lock:
                    print(f" === DEBUG LOCK ProductsResponseHandler")

                    temp = self.products_cache
                    if temp is not None:
                        current_cache = temp.copy()
                    else:
                        current_cache = None

                    print(f"  *  DEBUG current entry: {current_cache}")
                    print(f"  *  DEBUG new entry: {products}")

                    if current_cache is None:  # Rule 3: Empty cache can accept non-empty
                        self.products_cache = products.copy()
                        print(f"  >  DEBUG None to full: {self.products_cache}")
                        if not self.data_ready.is_set():  # Сигнализируем при первом успешном обновлении
                            self.data_ready.set()
                    else:
                        if products != current_cache:  # Rule 5: Update only if different
                            self.products_cache = products.copy()
                            print(f"  >  DEBUG was change: {self.products_cache}")
                            if not self.data_ready.is_set():  # Сигнализируем при первом успешном обновлении
                                self.data_ready.set()
                    #self.products_cache = products.copy()
                    #print(f"DEBUF view self.products: {products}")

                self.consumer.commit()

            except Exception as e:
                app.logger.error(f"Error processing message : {str(e)}")
                try:
                    self.consumer.commit()
                except Exception as commit_err:
                    app.logger.error(f"PRH failed to commit offset after error: {commit_err}")

    def get_products(self):
        with self.lock:
            result = self.products_cache.copy()
            print(f"  @  DEBUG WarehouseOnlineManager GET {result}")
            return result


@app.template_filter('datetimeformat')
def datetimeformat_filter(value, format='%d.%m.%Y %H:%M'):
    try:
        if not value:
            return "Дата не указана"

        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return "Неверный формат даты"

        return value.strftime(format)

    except Exception as e:
        app.logger.error(f"Ошибка форматирования даты: {str(e)}")
        return "Ошибка даты"

def wh_formatter(wh_id):
    """Форматирует WH ID в читаемый вид"""
    if not wh_id or len(wh_id) != 24:
        return wh_id
    try:
        # Пример: WHAAAAAARUS060ru00000002 -> WH-AAAA-AA-RUS-060-RU-0000-0002
        parts = [
            wh_id[0:2],
            wh_id[2:6],
            wh_id[6:8],
            wh_id[8:11],
            wh_id[11:14],
            wh_id[14:16],
            wh_id[16:20],
            wh_id[20:]
        ]
        return '-'.join(parts)
    except:
        return wh_id

# Регистрация фильтра в Jinja
app.jinja_env.filters['wh_formatter'] = wh_formatter

def check_kafka_connection():
    try:
        consumer = KafkaConsumer(bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'])
        consumer.topics()
        consumer.close()
        app.logger.info("Успешное подключение к Kafka")
        return True
    except Exception as e:
        app.logger.error(f"Ошибка подключения к Kafka: {str(e)}")
        return False

#TODO здесь читаются картинки
#def start_kafka_consumer():
    # while True:
    #     consumer = None
    #     try:
    #         consumer = KafkaConsumer(
    #             app.config['KAFKA_PRODUCT_TOPIC'],
    #             bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
    #             auto_offset_reset='latest',
    #             value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    #             group_id='wi-consumer-group',
    #             enable_auto_commit=True,
    #             session_timeout_ms=30000,
    #             max_poll_interval_ms=300000,
    #             consumer_timeout_ms=10000
    #         )
    #
    #         app.logger.info("Kafka consumer connected. Waiting for messages...")
    #
    #         for message in consumer:
    #             try:
    #                 products = message.value
    #                 if not isinstance(products, list):
    #                     raise ValueError("Invalid message format")
    #
    #                 app.logger.info(f"Received {len(products)} products")
    #
    #                 with products_cache_lock:
    #                     products_cache.clear()
    #                     products_cache.extend(products)
    #
    #             except Exception as e:
    #                 app.logger.error(f"Error processing message: {str(e)}")
    #
    #     except (KafkaError, NoBrokersAvailable) as e:
    #         app.logger.error(f"Kafka connection error: {str(e)}. Retrying in 5 seconds...")
    #         time.sleep(5)
    #     except Exception as e:
    #         app.logger.error(f"Unexpected error: {str(e)}")
    #         time.sleep(5)
    #     finally:
    #         if consumer:
    #             try:
    #                 consumer.close()
    #             except:
    #                 pass

# MAy Be DELETE
# def start_stock_consumer():
#     app.logger.info("Starting stock consumer")
#     while True:
#         try:
#             consumer = KafkaConsumer(
#                 app.config['KAFKA_STOCK_TOPIC'],
#                 bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
#                 value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#                 group_id='wi-stock-group-hz',
#                 auto_offset_reset='earliest',
#                 enable_auto_commit=False
#             )
#
#             app.logger.info("Connected to Kafka stock topic")
#
#             for message in consumer:
#                 try:
#                     stock_data = message.value
#                     if not isinstance(stock_data, dict):
#                         raise ValueError(f"Invalid stock data format: {type(stock_data)}")
#
#                     with stock_lock:
#                         for warehouse_id, items in stock_data.items():
#                             warehouse_stock[warehouse_id] = {
#                                 str(item.get('pgd_id')): item.get('quantity', 0)
#                                 for item in items if 'pgd_id' in item and 'quantity' in item
#                             }
#                         print("DEBUG SUKA (17/04/2025) [MAy Be DELETE] CALL def start_stock_consumer()")
#                 except Exception as e:
#                     print(f'loger_start_stock_cons' +  str(e))
#                 with stock_lock:
#                     for warehouse_id, items in message.value.items():
#                         # Преобразовываем ID товаров к строке
#                         warehouse_stock[warehouse_id] = {
#                             str(item['pgd_id']): item['quantity']
#                             for item in items
#                         }
#                         app.logger.info(f"Updated stock for warehouse: {warehouse_id}")
#
#         except Exception as e:
#             app.logger.error(f"Stock consumer error: {str(e)}")
#             time.sleep(5)


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


# @app.route('/')
# @login_required
# def index():
#     try:
#         section = request.args.get('section', 'products')
#         selected_wh = request.args.get('warehouse', 'all')
#         with products_cache_lock:
#             products = [p.copy() for p in products_cache]
#
#         with stock_lock:
#             if selected_wh != 'all':
#                 stock = warehouse_stock.get(selected_wh, {})
#                 filtered_products = [
#                     p for p in products
#                     if str(p.get('id', '')) in stock  # Двойное преобразование
#                 ]
#             else:
#                 filtered_products = products
#
#         return render_template(
#             'index.html',
#             section=section,
#             products=filtered_products,
#             warehouses=warehouse_manager.get_active_warehouses(),
#             selected_wh=selected_wh,
#             invoices=get_filtered_invoices(selected_wh)  # Передаем правильные данные
#         )
#     except Exception as e:
#         app.logger.error(f"Index page error: {str(e)}")
#         return render_template('error.html'), 500


# def greet_warehouse(wh_id):
#     """Функция приветствия склада с запросом товаров"""
#     print(f"Привет, склад {wh_id}! Запрашиваем список товаров...")
#
#     # Отправка запроса в Kafka
#     try:
#         producer2.send(
#             app.config['KAFKA_GOODS_REQUEST_TOPIC'],
#             {
#                 'wh_id': wh_id,
#                 'command': 'get_all_goods',
#                 'timestamp': datetime.now(timezone.utc).isoformat()
#             }
#         )
#         producer2.flush()
#     except Exception as e:
#         app.logger.error(f"Ошибка отправки запроса товаров: {str(e)}")


@app.route('/')
@login_required
def index():
    try:
        section = request.args.get('section', 'products')
        selected_wh = request.args.get('warehouse', 'all')

        all_products = app.products_response.get_products()

        filtered_products = []
        if selected_wh != 'all':

            # Получаем товары склада из кэша
            warehouse_goods = app.goods_handler.get_goods(selected_wh)
            print(f"Goods for {selected_wh}: {warehouse_goods}")

            # Создаем словарь для быстрого поиска товаров
            goods_dict = {
                str(item['pgd_id']): item  # Преобразуем pgd_id в строку
                for item in warehouse_goods
            }

            # Формируем список товаров с информацией о количестве
            for product in all_products:
                product_id = str(product.get('id'))
                if product_id in goods_dict:
                    product_copy = product.copy()
                    product_copy['quantity'] = goods_dict[product_id]['quantity']
                    filtered_products.append(product_copy)
        else:
            # Для "Всех складов" показываем все товары без количества
            filtered_products = all_products

        warehouses_data = app.warehouse_online_mgr.get_warehouses()
        invoices = app.warehouse_state_invoice.get_state_invoice()

        print(f"% defub | section: {section}")
        print(f"% defub | selected_wh: {selected_wh}")
        print(f"% defub | all Warehouses: {warehouses_data}")
        print(f"% defub | all products: {all_products}")
        print(f"% defub | filtered products (all or goods by WH): {filtered_products}")
        print(f"% defub | invoices: {invoices}")

        with app.goods_handler.lock:
            print(f"% defub | full goods: {app.goods_handler.goods_cache}")

        print(f"% defub | full2 goods: {app.goods_handler.get_goods('WHAAAAAARUS060ru00000002')}")

        return render_template(
            'index.html',
            section=section,
            selected_wh=selected_wh,
            warehouses=warehouses_data,
            allProducts=all_products,
            products=filtered_products,
            invoices=invoices
        )
    except Exception as e:
        app.logger.error(f"Index error: {str(e)}")
        return "Произошла ошибка сервера. Попробуйте позже.", 500

# DELETE !!!
# def get_filtered_invoices(selected_wh):
#     with invoices_cache_lock:
#         return [
#             {
#                 'id': inv.get('id', 'N/A'),  # Используем get с значением по умолчанию
#                 'type': inv.get('type'),
#                 'sender': inv.get('sender'),
#                 'receiver': inv.get('receiver'),
#                 'status': inv.get('status'),
#                 'items': [
#                     {
#                         'name': f"Товар {item.get('id', '?')}",  # Защита от отсутствия id
#                         'quantity': item.get('quantity', 0)
#                     } for item in inv.get('items', [])
#                 ],
#                 'timestamp': inv.get('timestamp')
#             }
#             for inv in invoices_cache
#             if selected_wh == 'all'
#             or inv.get('sender') == selected_wh
#             or inv.get('receiver') == selected_wh
#         ]

@app.route('/get_invoices')
def get_invoices():
    warehouse_id = request.args.get('warehouse_id')
    if not warehouse_id:
        return jsonify({'error': 'Склад не выбран'}), 400

    warehouse_data = app.warehouse_state_invoice.get_state_invoice() #state_invoices.get(warehouse_id, {}) # ВАЖНО
    return jsonify({
        'invoices': [invoice for warehouse4 in warehouse_data for invoice in warehouse4['invoices']] #warehouse_data.get('invoices', [])
    })


@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user and user.check_password(form.password.data):
            login_user(user)
            return redirect(url_for('index'))
        flash('Invalid username or password')
    return render_template('login.html', form=form)


@app.route('/register', methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User(username=form.username.data, email=form.email.data)
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()
        flash('Registration successful!')
        return redirect(url_for('login'))
    return render_template('register.html', form=form)


# WI/app.py
@app.route('/create_invoice', methods=['POST'])
@login_required
def create_invoice():
    try:
        # Извлечение базовых данных
        invoice_type = request.form.get('type')
        operated_wh = request.form.get('operated')

        # Валидация основных полей
        if not all([invoice_type, operated_wh]):
            flash('Заполните все обязательные поля', 'danger')
            return redirect(url_for('index', section='tasks'))

        if len(operated_wh) != 24:
            flash('Некорректный формат идентификатора склада', 'danger')
            return redirect(url_for('index', section='tasks'))

        # Обработка товаров
        items = []
        item_indices = {k.split('[')[1].split(']')[0]
                        for k in request.form.keys()
                        if k.startswith('items[') and '][id]' in k}

        for idx in item_indices:
            try:
                item_id = int(request.form.get(f'items[{idx}][id]'))
                quantity = int(request.form.get(f'items[{idx}][quantity]', 0))

                if quantity <= 0:
                    raise ValueError("Некорректное количество")

                items.append({'id': item_id, 'quantity': quantity})

            except (ValueError, TypeError) as e:
                app.logger.error(f"Ошибка обработки товара: {str(e)}")
                flash('Проверьте правильность данных товаров', 'danger')
                return redirect(url_for('index', section='tasks'))

        if not items:
            flash('Добавьте хотя бы один товар', 'danger')
            return redirect(url_for('index', section='tasks'))

        # Формирование сообщения
        invoice_data = {
            'type': invoice_type,
            'operated': operated_wh,
            'items': items,
            'user_id': current_user.id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        print(f'< ><> < > <>< > DEBUG SUKA [ invoice_data ] = {invoice_data}')

        # Отправка в Kafka
        # producer.send(
        #     app.config.get('KAFKA_INVOICE_TOPIC', 'invoice_requests'),
        #     invoice_data
        # )
        # producer.flush()

        flash('Накладная успешно создана!', 'success')
        return redirect(url_for('index', section='tasks'))

    except Exception as e:
        app.logger.error(f"Ошибка создания накладной: {str(e)}")
        flash('Внутренняя ошибка сервера', 'danger')
        return redirect(url_for('index', section='tasks'))


@app.route('/settings', methods=['GET', 'POST'])
@login_required
def warehouse_settings():
    if not current_user.is_admin:
        flash('Access denied')
        return redirect(url_for('index'))

    form = WarehouseSettingsForm()
    # if form.validate_on_submit():
    #     warehouse['connected'] = True
    #     warehouse['auto_balance_params'] = {
    #         'threshold': form.threshold.data,
    #         'interval': form.interval.data
    #     }
    #     flash('Settings saved!')
    #     return redirect(url_for('index'))
    return render_template('settings.html', form=form)


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.template_filter('wh_formatter')
def wh_formatter(wh):
    """Улучшенный фильтр форматирования"""
    if isinstance(wh, dict):
        name = wh.get('metadata', {}).get('name', wh['wh_id'])
        return f"{name} ({wh['wh_id'][:4]}...{wh['wh_id'][-4:]})"

    if isinstance(wh, str):
        return f"{wh[:4]}...{wh[-4:]}"

    return str(wh)

@app.route('/get_warehouse_goods', methods=['POST'])
@login_required
def get_warehouse_goods():
    wh_id = request.json.get('wh_id')
    if wh_id:
        # Отправляем запрос в Kafka
        producer2.send(Config.KAFKA_GOODS_REQUEST_TOPIC, {
            'wh_id': wh_id,
            'command': 'get_all_goods'
        })
        producer2.flush()
        return {'status': 'request_sent'}
    return {'status': 'error'}

# @app.route('/current_goods')
# @login_required
# def current_goods():
#     wh_id = request.args.get('wh_id')
#     return {'goods': app.goods_handler.get_goods(wh_id)}


@app.route('/current_goods')
def current_goods():
    wh_id = request.args.get('wh_id')
    if not wh_id:
        return jsonify({"error": "Не указан ID склада"}), 400

    goods = app.goods_handler.get_goods(wh_id)

    # Логируем для отладки
    app.logger.info(f"Запрос товаров для склада {wh_id}, получено {len(goods)} позиций")

    # Обогащаем данные о товарах информацией из кэша продуктов, если она доступна
    enriched_goods = []
    products = app.products_handler.get_products() if hasattr(app, 'products_handler') else []

    for item in goods:
        pgd_id = item['pgd_id']
        product_info = next((p for p in products if str(p.get('pgd_id')) == pgd_id), {})

        enriched_goods.append({
            'pgd_id': pgd_id,
            'quantity': item['quantity'],
            'name': product_info.get('name', f"Товар {pgd_id}"),
            'description': product_info.get('description', '')
        })

    return jsonify({'goods': enriched_goods})


def create_admin():
    with app.app_context():
        if not User.query.filter_by(username='adm').first():
            admin = User(username='adm', email='leo@gmail.com', is_admin=True)
            admin.set_password('1')
            db.session.add(admin)
            db.session.commit()
            print("Admin user created!")


def initialize_handlers(time = 120):
    if check_kafka_connection():
        # ProductsResponseHandler
        print(f"Create ProductsResponseHandler")
        app.products_response = ProductsResponseHandler()
        try:
            print(f"Wait ProductsResponseHandler")
            app.products_response.data_ready.wait(time)
        except TimeoutError:
            print(f"Timeout of data for ProductsResponseHandler")

        # WarehouseOnlineManager
        print(f"Create WarehouseOnlineManager")
        app.warehouse_online_mgr = WarehouseOnlineManager()
        try:
            print(f"Wait WarehouseOnlineManager")
            app.warehouse_online_mgr.data_ready.wait(time)
        except TimeoutError:
            print(f"Timeout of data for WarehouseOnlineManager")

        # WarehouseStateInvoice
        print(f"Create WarehouseStateInvoice")
        app.warehouse_state_invoice = WarehouseStateInvoice()
        try:
            print(f"Wait WarehouseStateInvoice")
            app.warehouse_state_invoice.data_ready.wait(time)
        except TimeoutError:
            print(f"Timeout of data for WarehouseStateInvoice")

        # GoodsResponseHandler
        print(f"Create GoodsResponseHandler")
        app.goods_handler = GoodsResponseHandler()
        try:
            print(f"Wait GoodsResponseHandler")
            app.goods_handler.data_ready.wait(time)
        except TimeoutError:
            print(f"Timeout of data for GoodsResponseHandler")

    else:
        app.logger.error("Failed to connect to Kafka")
        sys.exit(1)

if __name__ == '__main__':

    with app.app_context():
        db.create_all()
        initialize_handlers()
    create_admin()
    app.run(debug=False, port=5010)
