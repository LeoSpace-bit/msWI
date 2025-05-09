#app.py
import sys
import uuid
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
                print(f" === DEBUF WOM RAW DATA##: {data}") # Оставляем для отладки если нужно

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

                timestamp_str = data.get('timestamp')
                try:
                    # Улучшаем парсинг времени, обрабатывая разные форматы и Z
                    if timestamp_str:
                        timestamp_str = timestamp_str.rstrip('Z')
                        if '.' in timestamp_str:
                             timestamp = datetime.fromisoformat(timestamp_str)
                        else:
                             timestamp = datetime.fromisoformat(timestamp_str + '.000000') # Добавляем микросекунды если их нет
                    else:
                         timestamp = datetime.utcnow()

                    # Устанавливаем таймзону UTC если ее нет
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=timezone.utc)

                except ValueError as e:
                    app.logger.warning(f"WOM could not parse timestamp '{timestamp_str}': {e}. Using current time.")
                    timestamp = datetime.now(timezone.utc)


                with self.lock:
                    print(f" === DEBUG LOCK WarehouseOnlineManager")
                    current_entry = self.active_warehouses.get(wh_id)
                    print(f"  *  DEBUG current entry: {current_entry}")

                    new_entry = {
                        'wh_id': wh_id,
                        'last_seen': datetime.now(timezone.utc),
                        'timestamp': timestamp,
                        'metadata': metadata
                    }
                    print(f"  *  DEBUG new entry: {new_entry}")

                    if current_entry is None:
                        self.active_warehouses[wh_id] = new_entry.copy()
                        # print(f"  >  DEBUG None to full: {self.active_warehouses}")
                        if not self.data_ready.is_set():
                            self.data_ready.set()
                    else:
                         # Сравниваем только основные данные, last_seen обновляем всегда
                        should_update = (
                            current_entry.get('timestamp') != new_entry['timestamp'] or
                            current_entry.get('metadata') != new_entry['metadata'] or
                            current_entry.get('wh_id') != new_entry['wh_id']
                        )

                        if should_update:
                            # Обновляем запись полностью, включая last_seen
                            self.active_warehouses[wh_id] = new_entry.copy()
                            print(f"  >  DEBUG was change: {self.active_warehouses}")
                            if not self.data_ready.is_set():
                                self.data_ready.set()
                        else:
                            # Данные не изменились, обновляем только last_seen
                            print(f"  *  DEBUG last_seen update only")
                            self.active_warehouses[wh_id]['last_seen'] = new_entry['last_seen']


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
            timeout_seconds = 200 # Вынести в конфиг
            result = [
                {
                    'wh_id': wh_id,
                    'metadata': data.get('metadata', {})
                }
                for wh_id, data in self.active_warehouses.items()
                # Используем .get для last_seen на случай если запись неполная
                if data.get('last_seen') and (now - data['last_seen']).total_seconds() < timeout_seconds
            ]
            print(f"  @  DEBUG WarehouseOnlineManager GET {result}")
            return result.copy()

class GoodsResponseHandler:
    """Обработчик ответов с товарами склада"""

    def __init__(self):
        self.goods_cache = {} # { wh_id: [ {pgd_id: str, quantity: int}, ... ] }
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

                if not data or not isinstance(data, dict):
                    app.logger.warning(f"GRH received invalid message: {data}")
                    self.consumer.commit()
                    continue

                data = data.copy()
                wh_id = data.get('wh_id')
                goods_raw = data.get('goods') # Это должен быть список

                if not wh_id:
                    app.logger.warning(f"GRH message missing wh_id: {data}")
                    self.consumer.commit()
                    continue

                # Обрабатываем случай, когда goods = None или не список
                if not isinstance(goods_raw, list):
                     app.logger.warning(f"GRH goods data is not a list for wh_id {wh_id}: {goods_raw}. Treating as empty.")
                     valid_goods = []
                else:
                     # Валидация структуры данных внутри списка
                    valid_goods = []
                    for item in goods_raw:
                        # Проверяем, что item это словарь и содержит нужные ключи
                        if isinstance(item, dict) and 'pgd_id' in item and 'quantity' in item:
                             try:
                                 # Приводим к нужным типам и добавляем
                                 valid_goods.append({
                                     'pgd_id': str(item['pgd_id']),
                                     'quantity': int(item['quantity'])
                                 })
                             except (ValueError, TypeError) as e:
                                 app.logger.warning(f"GRH invalid data types in goods item for {wh_id}: {item}. Error: {e}")
                        else:
                             app.logger.warning(f"GRH invalid goods item structure for {wh_id}: {item}")


                with self.lock:
                    print(f" === DEBUG LOCK GoodsResponseHandler")
                    current_goods = self.goods_cache.get(wh_id) # Может быть None
                    print(f"  *  DEBUG current entry for {wh_id}: {current_goods}")
                    print(f"  *  DEBUG new entry for {wh_id}: {valid_goods}")

                    # Обновляем только если данные изменились или их не было
                    # Сравнение списков словарей может быть неточным если порядок разный.
                    if current_goods != valid_goods:
                        self.goods_cache[wh_id] = valid_goods
                        print(f"  >  DEBUG updated goods for {wh_id}: {self.goods_cache[wh_id]}")
                        if not self.data_ready.is_set() and valid_goods: # Сигнализируем если добавили непустые данные
                            self.data_ready.set()

                self.consumer.commit()

            except Exception as e:
                app.logger.error(f"Goods response error: {str(e)}")
                app.logger.debug(f"Problematic message: {message.value}")
                try:
                    self.consumer.commit()
                except Exception as commit_err:
                    app.logger.error(f"GRH failed to commit offset after error: {commit_err}")


    def get_goods(self, wh_id):
        """Возвращает список товаров для указанного склада."""
        with self.lock:
            # Возвращаем копию списка или пустой список, если ключ не найден
            goods = self.goods_cache.get(str(wh_id), [])
            print(f"  @  DEBUG GoodsResponseHandler GET for {wh_id}: {goods}")
            return goods.copy()

# TODO: Улучшить отрисовку
class WarehouseStateInvoice:
    def __init__(self):
        self.state_invoices = {} # { wh_id: {'wh_id': ..., 'last_seen': ..., 'timestamp': ..., 'invoices': [...]}, ... }
        self.lock = threading.Lock()
        self.data_ready = threading.Event()
        self.consumer = KafkaConsumer(
            Config.KAFKA_STATE_INVOICE_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=self._deserialize_message, # Используем кастомный десериализатор
            group_id='wi-warehouse-state-invoice-v1-abc', # Группу лучше сделать уникальной или новой версией
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            # consumer_timeout_ms=30000 # Убрал таймаут, чтобы слушал постоянно
        )
        self.thread = threading.Thread(
            target=self.update_state_invoice,
            daemon=True
        )
        self.thread.start()

    def _deserialize_message(self, raw_message):
        """Кастомный десериализатор с обработкой ошибок JSON и времени."""
        try:
            data = json.loads(raw_message.decode('utf-8'))
            if not isinstance(data, dict):
                app.logger.error(f"WSI Deserialization error: message is not a dict: {data}")
                return None

            return data
        except (UnicodeDecodeError, JSONDecodeError) as e:
            app.logger.error(f"WSI Deserialization error: {str(e)}")
            app.logger.debug(f"Problematic raw message: {raw_message}")
            return None
        except Exception as e:
            app.logger.error(f"WSI Unexpected deserialization error: {str(e)}")
            app.logger.debug(f"Problematic raw message: {raw_message}")
            return None

    def update_state_invoice(self):
        for message in self.consumer:
            try:
                data = message.value # Уже десериализовано или None
                print(f" === DEBUF WSI RAW DATA##: {data}")

                if not data: # Пропускаем если десериализация не удалась
                    self.consumer.commit() # Коммитим чтобы не читать битое сообщение снова
                    continue

                data = data.copy() # Копировать не нужно, т.к. десериализатор создает новый объект

                wh_id = data.get('wh_id')
                invoices_raw = data.get('invoices', [])
                timestamp_str = data.get('timestamp') # Получаем как строку или None

                if not wh_id:
                    app.logger.warning(f"WSI message missing wh_id: {data}")
                    self.consumer.commit()
                    continue

                timestamp_dt = None
                if isinstance(timestamp_str, str):
                    try:
                        ts_str = timestamp_str.rstrip('Z')
                        if '.' not in ts_str:
                            ts_str += '.000000'
                        timestamp_dt = datetime.fromisoformat(ts_str)
                        if timestamp_dt.tzinfo is None:
                            timestamp_dt = timestamp_dt.replace(tzinfo=timezone.utc)
                    except ValueError:
                        app.logger.warning(f"WSI Could not parse timestamp: {timestamp_str}. Using current time.")
                        timestamp_dt = datetime.now(timezone.utc)
                else:
                    app.logger.warning(f"WSI Missing or invalid timestamp in message for {wh_id}. Using current time.")
                    timestamp_dt = datetime.now(timezone.utc)


                with self.lock:
                    print(f" === DEBUG LOCK WarehouseStateInvoice")
                    current_state = self.state_invoices.get(wh_id)
                    print(f"  *  DEBUG current entry for {wh_id}: {current_state}")

                    # Валидируем и обрабатываем invoices
                    processed_invoices = self._process_invoices(invoices_raw, wh_id)

                    # Формируем новое состояние
                    new_state = {
                        'wh_id': wh_id,
                        'last_seen': datetime.now(timezone.utc),
                        'timestamp': timestamp_dt, # Используем datetime объект
                        'invoices': processed_invoices
                    }
                    print(f"  *  DEBUG new entry for {wh_id}: {new_state}")

                    # Сравниваем содержимое (без last_seen)
                    should_update = True # По умолчанию обновляем
                    if current_state:
                         # Сравниваем timestamp и invoices
                         # Сравнение списков словарей может быть неточным из-за порядка,
                         # но для начала оставим так.
                         if (current_state.get('timestamp') == new_state['timestamp'] and
                             current_state.get('invoices') == new_state['invoices']):
                             should_update = False

                    if should_update:
                        self.state_invoices[wh_id] = new_state # Сохраняем новый стейт
                        print(f"  >  DEBUG state updated for {wh_id}")
                        if not self.data_ready.is_set() and new_state['invoices']:
                            self.data_ready.set()
                    else:
                        # Данные не изменились, обновим только last_seen
                        self.state_invoices[wh_id]['last_seen'] = new_state['last_seen']
                        print(f"  *  DEBUG last_seen updated for {wh_id}")


                self.consumer.commit()

            except Exception as e:
                # Логируем исключение с трассировкой
                app.logger.exception(f"WSI error processing message: {e}")
                app.logger.debug(f"Problematic message value: {message.value}")
                try:
                    self.consumer.commit()
                except Exception as commit_err:
                    app.logger.error(f"WSI failed to commit offset after error: {commit_err}")

    def _process_invoices(self, raw_invoices, wh_id_context):
        """Валидация и нормализация структуры накладных. Возвращает список валидных накладных."""
        processed = []
        if not isinstance(raw_invoices, list):
            app.logger.warning(f"WSI invoices data is not a list for wh_id {wh_id_context}. Treating as empty.")
            return []

        for inv in raw_invoices:
             # Проверяем базовую структуру и типы
             if not isinstance(inv, dict) or not all(key in inv for key in ('invoice_id', 'invoice_type', 'status', 'created_at', 'sender_warehouse', 'receiver_warehouse')):
                 app.logger.warning(f"WSI invalid invoice structure or missing keys for {wh_id_context}: {inv}")
                 continue

             # Парсим дату создания
             created_at_dt = None
             if isinstance(inv.get('created_at'), str):
                 try:
                     ts_str = inv['created_at'].rstrip('Z')
                     if '.' not in ts_str:
                         ts_str += '.000000'
                     created_at_dt = datetime.fromisoformat(ts_str)
                     if created_at_dt.tzinfo is None:
                         created_at_dt = created_at_dt.replace(tzinfo=timezone.utc)
                 except ValueError:
                    app.logger.warning(f"WSI could not parse created_at for invoice {inv.get('invoice_id')} in {wh_id_context}: {inv.get('created_at')}")
                    # Решаем, что делать: пропустить инвойс или оставить дату как None/строку? Пропустим.
                    continue
             else:
                 app.logger.warning(f"WSI missing or invalid created_at for invoice {inv.get('invoice_id')} in {wh_id_context}")
                 continue # Пропускаем инвойс без валидной даты создания

             # Обрабатываем товары (items)
             processed_items = []
             raw_items = inv.get('items', [])
             if isinstance(raw_items, list):
                 for item in raw_items:
                      # Проверяем структуру товара
                      if isinstance(item, dict) and all(k in item for k in ('pgd_id', 'quantity')):
                          try:
                             processed_items.append({
                                 'pgd_id': str(item['pgd_id']), # Приводим к строке
                                 'quantity': int(item['quantity']), # Приводим к int
                                 'batch': item.get('batch_number', None) # Используем .get для опционального поля
                             })
                          except (ValueError, TypeError):
                              app.logger.warning(f"WSI invalid data types in invoice item for {inv.get('invoice_id')} in {wh_id_context}: {item}")
                      else:
                          app.logger.warning(f"WSI invalid invoice item structure for {inv.get('invoice_id')} in {wh_id_context}: {item}")
             else:
                 app.logger.warning(f"WSI items data is not a list for invoice {inv.get('invoice_id')} in {wh_id_context}")


             processed.append({
                'invoice_id': inv['invoice_id'], # Тип может быть не строкой, оставляем как есть
                'type': str(inv['invoice_type']),
                'status': str(inv['status']),
                'created_at': created_at_dt, # Используем datetime объект
                'sender': str(inv['sender_warehouse']),
                'receiver': str(inv['receiver_warehouse']),
                'items': processed_items
             })
        return processed

    def get_state_invoice(self):
        """Возвращает список состояний всех активных складов."""
        with self.lock:
            now = datetime.now(timezone.utc)
            timeout_seconds = 200
            # Возвращаем список словарей, содержащих данные по каждому складу
            result = [
                {
                    'wh_id': wh_data['wh_id'],
                    'timestamp': wh_data['timestamp'], # Это datetime объект
                    'invoices': wh_data['invoices'] # Это список обработанных инвойсов
                }
                for wh_data in self.state_invoices.values()
                if wh_data.get('last_seen') and (now - wh_data['last_seen']).total_seconds() < timeout_seconds
            ]
            print(f"  @  DEBUG WarehouseStateInvoice GET {result}")
            return result.copy() # Копировать не обязательно, генерируется заново

    def get_invoices_by_status(self, status):
        """Возвращает плоский список всех инвойсов с заданным статусом со всех активных складов."""
        with self.lock:
            all_active_states = self.get_state_invoice() # Получаем активные состояния
            result = []
            for wh_data in all_active_states:
                for invoice in wh_data.get('invoices', []):
                    if invoice.get('status') == status:
                        result.append(invoice)
            print(f"  @  DEBUG WarehouseStateInvoice GET by Status '{status}': {result}")
            return result # Возвращаем сам список

# TODO: При заполнении products_cache перестать слушать кафку здесь и выключить поток, не сбросив кэш
class ProductsResponseHandler:
    """Обработчик ответов с товарами """

    def __init__(self):
        self.products_cache = [] # [{id: str, name: str, ...}, ...]
        self.lock = threading.Lock()
        self.data_ready = threading.Event()
        self.consumer = KafkaConsumer(
            app.config['KAFKA_PRODUCT_TOPIC'],
            bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-consumer-group-v1', # Возможно, стоит обновить версию группы при изменениях
            enable_auto_commit=False,
            session_timeout_ms=60000,
            max_poll_interval_ms=300000,
            # consumer_timeout_ms=-1 # Слушаем постоянно
        )
        self.thread = threading.Thread(
            target=self.process_responses,
            daemon=True
        )
        self.thread.start()
        app.logger.info("PRH: Kafka consumer connected. Waiting for messages...")

    def process_responses(self):
        for message in self.consumer:
            try:
                products_raw = message.value
                print(f" === DEBUF PRH RAW DATA##: {products_raw}")

                if not isinstance(products_raw, list):
                    app.logger.warning(f"PRH received non-list message: {products_raw}")
                    self.consumer.commit()
                    continue

                # Пропускаем пустые списки, но не None или другие типы
                if not products_raw: # Это условие пропустит и [], и None, и 0 итд
                #if products_raw == []: # Явно проверяем на пустой список
                    print("PRH Received empty products list, ignoring.")
                    self.consumer.commit()
                    continue

                # Валидация структуры продуктов
                valid_products = []
                for item in products_raw:
                    if isinstance(item, dict) and 'id' in item and 'name' in item:
                        try:
                            # Создаем новый словарь только с нужными и валидными данными
                            valid_item = {
                                'id': str(item['id']), # Приводим ID к строке
                                'name': str(item['name']),
                                # Добавляем остальные поля, приводя к ожидаемым типам или с .get()
                                'price': float(item.get('price', 0.0)),
                                'photo_url': item.get('photo_url'), # Оставляем как есть (строка или None)
                                'weight': item.get('weight') # Оставляем как есть
                            }
                            valid_products.append(valid_item)
                        except (ValueError, TypeError) as e:
                             app.logger.warning(f"PRH invalid data types in product item: {item}. Error: {e}")
                    else:
                        app.logger.warning(f"PRH invalid product structure in message: {item}")

                # Сравниваем с кэшем после валидации
                with self.lock:
                    print(f" === DEBUG LOCK ProductsResponseHandler")
                    # Сравнение списков словарей может быть неточным из-за порядка
                    # Для надежности можно использовать множества frozenset(item.items())
                    # Но пока оставим прямое сравнение
                    if self.products_cache != valid_products:
                        self.products_cache = valid_products # Заменяем кэш валидными данными
                        print(f"  >  DEBUG PRH cache updated. New count: {len(self.products_cache)}")
                        if not self.data_ready.is_set() and self.products_cache:
                            self.data_ready.set() # Сигнализируем если кэш стал непустым

                self.consumer.commit()

            except Exception as e:
                app.logger.exception(f"PRH Error processing message: {e}")
                app.logger.debug(f"Problematic message value: {message.value}")
                try:
                    self.consumer.commit()
                except Exception as commit_err:
                    app.logger.error(f"PRH failed to commit offset after error: {commit_err}")

    def get_products(self):
        """Возвращает копию кэша всех товаров."""
        with self.lock:
            print(f"  @  DEBUG ProductsResponseHandler GET. Count: {len(self.products_cache)}")
            return self.products_cache.copy()


@app.template_filter('datetimeformat')
def datetimeformat_filter(value, format='%d.%m.%Y %H:%M'):
    """Форматирует объект datetime или ISO строку в нужный формат."""
    if not value:
        return "Дата не указана"

    dt = None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            # Пытаемся распарсить ISO строку, учитывая Z
            value = value.rstrip('Z')
            if '.' not in value: # Добавляем микросекунды если нет для fromisoformat
                 value += ".000000"
            dt = datetime.fromisoformat(value)
        except ValueError:
            return "Неверный формат строки даты"

    if dt:
        # Если объект aware, конвертируем в локальное время (или оставляем UTC?)
        # Пока просто форматируем как есть
        try:
            return dt.strftime(format)
        except ValueError:
            return "Ошибка форматирования даты" # Например, год вне диапазона
    else:
        return "Неверный тип даты"

def wh_formatter(wh_id):
    """Форматирует WH ID в читаемый вид WH-AAAA-AA-RUS-060-RU-0000-0002"""
    if not wh_id or not isinstance(wh_id, str) or len(wh_id) != 24:
        return wh_id # Возвращаем как есть, если формат некорректный
    try:
        # WHAAAAAARUS060ru00000002
        # 012345678901234567890123
        parts = [
            wh_id[0:2],   # WH
            wh_id[2:6],   # AAAA
            wh_id[6:8],   # AA
            wh_id[8:11],  # RUS
            wh_id[11:14], # 060
            wh_id[14:16], # ru
            wh_id[16:20], # 0000
            wh_id[20:24]  # 0002
        ]
        return '-'.join(parts)
    except Exception:
        # На случай непредвиденных ошибок при срезах или join
        app.logger.warning(f"Error formatting wh_id: {wh_id}", exc_info=True)
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


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


@app.route('/')
@login_required
def index():
    """Главная страница с отображением товаров и заявок."""
    try:
        section = request.args.get('section', 'products')
        # Получаем ID склада для фильтрации товаров, 'all' по умолчанию
        selected_wh_products = request.args.get('warehouse', 'all')

        # Получаем данные от обработчиков
        all_products_list = app.products_response.get_products()
        warehouses_list = app.warehouse_online_mgr.get_warehouses()
        # Получаем все состояния инвойсов (список словарей)
        all_invoice_states = app.warehouse_state_invoice.get_state_invoice()

        # --- Логика для секции Товаров ---
        filtered_products = []
        if selected_wh_products != 'all':
            # Получаем товары только для выбранного склада
            warehouse_goods = app.goods_handler.get_goods(selected_wh_products) # [{'pgd_id': '3', 'quantity': 20}, ...]

            # Создаем словарь для быстрого поиска количества по pgd_id
            goods_quantity_map = {
                str(item['pgd_id']): item['quantity']
                for item in warehouse_goods
            }

            # Фильтруем общий список товаров
            for product in all_products_list:
                product_id = str(product.get('id'))
                if product_id in goods_quantity_map:
                    product_copy = product.copy() # Копируем, чтобы не изменять исходный кэш
                    product_copy['quantity'] = goods_quantity_map[product_id] # Добавляем количество
                    filtered_products.append(product_copy)
        else:
            # Показываем все товары из каталога, количество не добавляем
            # Копируем, чтобы избежать передачи изменяемого объекта в шаблон
            filtered_products = [p.copy() for p in all_products_list]

        # --- Логика для секции Заявок ---
        # Данные по заявкам (all_invoice_states) передаются как есть.
        # Фильтрация будет на стороне клиента (JavaScript).

        # Отладочный вывод
        print(f"% defub | section: {section}")
        print(f"% defub | selected_wh_products: {selected_wh_products}")
        print(f"% defub | Warehouses List: {warehouses_list}")
        print(f"% defub | All Products List Count: {len(all_products_list)}")
        print(f"% defub | Filtered Products Count: {len(filtered_products)}")
        print(f"% defub | All Invoice States Count: {len(all_invoice_states)}")

        return render_template(
            'index.html',
            section=section,
            # Для секции продуктов
            selected_wh=selected_wh_products, # Передаем выбранный склад для <select>
            warehouses=warehouses_list,        # Список складов для <select>
            products=filtered_products,        # Отфильтрованный список товаров для отображения
            allProducts=all_products_list,     # Полный список товаров для модального окна
            # Для секции заявок
            invoice_states=all_invoice_states  # Полный список состояний заявок для JS
        )
    except Exception as e:
        app.logger.exception(f"Index error: {str(e)}") # Используем logger.exception для трассировки
        flash("Произошла ошибка при загрузке страницы. Попробуйте позже.", "error") # Можно использовать flash
        return "Произошла ошибка сервера. Пожалуйста, проверьте логи.", 500

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


@app.route('/create_invoice', methods=['POST'])
@login_required
def create_invoice():
    """Обрабатывает форму создания накладной."""
    try:
        invoice_type = request.form.get('type')
        operated_wh_id = request.form.get('operated')
        items = []
        # Парсинг items[index][key]
        item_keys = [key for key in request.form if key.startswith('items[')]
        item_indices = sorted(list(set(int(key.split('[')[1].split(']')[0]) for key in item_keys)))

        for index in item_indices:
            item_id = request.form.get(f'items[{index}][id]')
            item_quantity = request.form.get(f'items[{index}][quantity]')
            if item_id and item_quantity:
                try:
                    items.append({
                        'pgd_id': str(item_id),
                        'quantity': int(item_quantity)
                        # 'batch_number': выдаётся на стороне WH (LogisticServices)
                    })
                except ValueError:
                    flash(f"Некорректное количество для товара с ID {item_id}.", "warning")
                    # Прервать обработку или пропустить товар? Пропустим пока.
                    continue

        if not invoice_type or not operated_wh_id or not items:
            #flash('Не все поля заполнены корректно или не добавлены товары.', 'danger')
            return redirect(url_for('index', section='tasks')) # Возвращаемся на страницу с задачами

        # 'arrival' ? 'Приемка' : invoice.type === 'departure'
        if invoice_type == 'arrival':
            sender_wh = 'any'
            receiver_wh = str(operated_wh_id)
        elif invoice_type == 'departure':
            sender_wh = str(operated_wh_id)
            receiver_wh = 'any'
        else:
            raise NameError('Goodbye dude')

        # Пример сообщения (структура зависит от вашего контракта)
        kafka_message = {
            'tag': str(uuid.uuid4()),
            'invoice_type': invoice_type,
            'sender_warehouse': sender_wh, # Определить логикой
            'receiver_warehouse': receiver_wh, # Определить логикой
            'items': items,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        print(f"+& DEBUG KAFKA {kafka_message}")

        try:
            producer2.send(Config.KAFKA_LOGISTIC_INVOICE_TOPIC, value=kafka_message)
            producer2.flush()
            print(f"DEBUG KAFKA: Запрос на создание накладной отправлен: {kafka_message}")
            flash('Запрос на создание накладной отправлен.', 'success')
        except Exception as e:
            app.logger.exception(f"Ошибка отправки запроса на создание накладной в Kafka: {e}")
            flash('Ошибка при отправке запроса на создание накладной.', 'danger')

        # ПОКА ЧТО просто выводим сообщение об успехе (ЗАМЕНИТЬ НА ОТПРАВКУ В KAFKA)
        # flash(f'Запрос на {invoice_type} для склада {operated_wh_id} с {len(items)} товарами получен (требуется отправка в Kafka).', 'info')

    except Exception as e:
        app.logger.exception(f"Ошибка при обработке формы создания накладной: {e}")
        flash('Произошла внутренняя ошибка при создании накладной.', 'danger')

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

# НЕ ИСПОЛЬЗУЕТСЯ
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
        create_admin()
        initialize_handlers()
    app.run(debug=False, port=5010)
