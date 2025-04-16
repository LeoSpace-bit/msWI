#app.py
import time
from collections import defaultdict
from datetime import datetime, timezone
from datetime import timedelta
from json import JSONDecodeError

from flask import Flask, render_template, redirect, url_for, request, flash, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from kafka.errors import KafkaError, NoBrokersAvailable

from config import Config
from forms import LoginForm, RegistrationForm, WarehouseSettingsForm
from database import db
from models import User
from flask_migrate import Migrate
from kafka import KafkaConsumer, KafkaProducer, producer
import json
import threading


#warehouse_state_invoice = None

app = Flask(__name__)
app.config.from_object('config.Config')

#kafka
#products_cache = []
#products_cache_lock = threading.Lock()  # Добавляем блокировку
warehouse_stock = defaultdict(dict)
stock_lock = threading.Lock()
#active_warehouses = set()
invoices_cache = []
invoices_cache_lock = threading.Lock()

# Инициализируем продюсер
producer = KafkaProducer(
    bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)




# Инициализация расширений
db.init_app(app)
migrate = Migrate(app, db)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

# Mock warehouse data (in-memory storage)
warehouse = {
    'connected': False,
    'products': defaultdict(int),
    'tasks': [],
    'auto_balance_params': {'threshold': 100, 'interval': 60}
}



class WarehouseOnlineManager:
    def __init__(self):
        self.active_warehouses = {}
        self.lock = threading.Lock()
        self.consumer = KafkaConsumer(
            Config.KAFKA_WAREHOUSES_ONLINE_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-warehouse-online-v3',
            auto_offset_reset='earliest'
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
                wh_id = data.get('wh_id')
                metadata = data.get('metadata', {})

                # Добавляем обработку отсутствующего timestamp
                timestamp_str = data.get('timestamp')
                timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else datetime.utcnow()

                if not wh_id:
                    raise ValueError("Missing warehouse ID in message")

                with self.lock:
                    self.active_warehouses[wh_id] = {
                        'wh_id': wh_id,
                        'last_seen': datetime.now(timezone.utc),
                        'timestamp': timestamp,
                        'metadata': metadata  # Сохраняем metadata
                    }
                    app.logger.debug(f"Updated warehouse: {wh_id}")
                    print(f"we know: {wh_id}")  # Для быстрой отладки

            except Exception as e:
                app.logger.error(f"Warehouse online error: {str(e)}")
                app.logger.debug(f"Problematic message: {message.value}")

    def get_warehouses(self):
        with self.lock:
            now = datetime.now(timezone.utc)
            return [
                {
                    'wh_id': wh_id,
                    'metadata': data.get('metadata', {})  # Передаем metadata
                }  # Возвращаем словари вместо строк
                for wh_id, data in self.active_warehouses.items()
                if (now - data['last_seen']).total_seconds() < 20
            ]


class GoodsResponseHandler:
    """Обработчик ответов с товарами склада"""

    def __init__(self):
        self.goods_cache = {}
        self.consumer = KafkaConsumer(
            Config.KAFKA_GOODS_RESPONSE_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-goods-response',
            auto_offset_reset='earliest'
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
                if not isinstance(data, dict):
                    raise ValueError("Invalid response format")

                wh_id = data.get('wh_id')
                goods = data.get('goods', [])

                # Валидация структуры данных
                valid_goods = []
                for item in goods:
                    if 'pgd_id' in item and 'quantity' in item:
                        valid_goods.append({
                            'pgd_id': str(item['pgd_id']),
                            'quantity': int(item['quantity'])
                        })

                self.goods_cache[wh_id] = valid_goods
                app.logger.info(f"Updated goods cache for {wh_id}")

            except Exception as e:
                app.logger.error(f"Goods response error: {str(e)}")

    def get_goods(self, wh_id):
        return self.goods_cache.get(wh_id, [])


# TODO: Улучшить отрисовку
class WarehouseStateInvoice:
    def __init__(self):
        self.state_invoices = {}
        self.lock = threading.Lock()
        self.consumer = KafkaConsumer(
            Config.KAFKA_STATE_INVOICE_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-warehouse-state-invoice-v1',
            auto_offset_reset='latest'
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
                if not data:
                    continue

                wh_id = data.get('wh_id')
                invoices = data.get('invoices', [])
                timestamp = data.get('timestamp') or datetime.now(timezone.utc)

                if not wh_id:
                    raise ValueError("Missing warehouse ID in message")

                with self.lock:
                    self.state_invoices[wh_id] = {
                        'wh_id': wh_id,
                        'last_seen': datetime.now(timezone.utc),
                        'timestamp': timestamp,
                        'invoices': self._process_invoices(invoices)
                    }
                    print(f"we know: {self.state_invoices}")  # Для быстрой отладки

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
            return [
                {
                    'wh_id': wh_data['wh_id'],
                    'timestamp': wh_data['timestamp'],
                    'invoices': wh_data['invoices']
                }
                for wh_data in self.state_invoices.values()
                if (now - wh_data['last_seen']).total_seconds() < 20
            ]

    def get_invoices_by_status(self, status):
        with self.lock:
            return [
                invoice
                for wh_data in self.state_invoices.values()
                for invoice in wh_data['invoices']
                if invoice['status'] == status
            ]





class ProductsResponseHandler:
    """Обработчик ответов с товарами """

    def __init__(self):
        self.products_cache = {}
        self.lock = threading.Lock()
        self.consumer = KafkaConsumer(
            app.config['KAFKA_PRODUCT_TOPIC'],
            bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='wi-consumer-group',
            enable_auto_commit=True,
            session_timeout_ms=30000,
            max_poll_interval_ms=300000,
            consumer_timeout_ms=10000
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
                if not isinstance(products, list):
                    raise ValueError("Invalid message format")

                app.logger.info(f"Received {len(products)} products")

                with self.lock:
                    self.products_cache.clear()
                    self.products_cache = products

            except Exception as e:
                app.logger.error(f"Error processing message: {str(e)}")





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


def start_stock_consumer():
    app.logger.info("Starting stock consumer")
    while True:
        try:
            consumer = KafkaConsumer(
                app.config['KAFKA_STOCK_TOPIC'],
                bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='wi-stock-group',
                auto_offset_reset='earliest',
                enable_auto_commit=False
            )

            app.logger.info("Connected to Kafka stock topic")

            for message in consumer:
                try:
                    stock_data = message.value
                    if not isinstance(stock_data, dict):
                        raise ValueError(f"Invalid stock data format: {type(stock_data)}")

                    with stock_lock:
                        for warehouse_id, items in stock_data.items():
                            warehouse_stock[warehouse_id] = {
                                str(item.get('pgd_id')): item.get('quantity', 0)
                                for item in items if 'pgd_id' in item and 'quantity' in item
                            }
                except Exception as e:
                    print(f'loger_start_stock_cons' +  str(e))
                with stock_lock:
                    for warehouse_id, items in message.value.items():
                        # Преобразовываем ID товаров к строке
                        warehouse_stock[warehouse_id] = {
                            str(item['pgd_id']): item['quantity']
                            for item in items
                        }
                        app.logger.info(f"Updated stock for warehouse: {warehouse_id}")

        except Exception as e:
            app.logger.error(f"Stock consumer error: {str(e)}")
            time.sleep(5)


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


def greet_warehouse(wh_id):
    """Функция приветствия склада с запросом товаров"""
    print(f"Привет, склад {wh_id}! Запрашиваем список товаров...")

    # Отправка запроса в Kafka
    try:
        producer.send(
            app.config['KAFKA_GOODS_REQUEST_TOPIC'],
            {
                'wh_id': wh_id,
                'command': 'get_all_goods',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        )
        producer.flush()
    except Exception as e:
        app.logger.error(f"Ошибка отправки запроса товаров: {str(e)}")


@app.route('/')
@login_required
def index():
    try:
        section = request.args.get('section', 'products')
        selected_wh = request.args.get('warehouse', 'all')

        # Добавляем логирование
        app.logger.info(f"Available warehouses: {warehouse_online_mgr.get_warehouses()}")

        all_products = [p.copy() for p in products_response.products_cache] # May be without copy

        print(f"defub | selected_wh: {selected_wh}")
        print(f"defub | state_invoices: {warehouse_state_invoice.state_invoices}")
        print(f"defub | all_products: {all_products}")
        print(f"defub | get_goods: {goods_handler.get_goods(selected_wh)}")
        filtered_products = []
        if selected_wh != 'all':
            greet_warehouse(selected_wh)

            # Получаем товары склада из кэша
            warehouse_goods = goods_handler.get_goods(selected_wh)
            print(f"Goods for {selected_wh}: {warehouse_goods}")
            app.logger.info(f"Goods for {selected_wh}: {warehouse_goods}")

            # Создаем словарь для быстрого поиска товаров
            goods_dict = {
                str(item['pgd_id']): item  # Преобразуем pgd_id в строку
                for item in warehouse_goods
            }

            # Формируем список товаров с информацией о количестве
            for product in all_products:
                product_id = str(product.get('id', ''))
                if product_id in goods_dict:
                    product_copy = product.copy()
                    product_copy['quantity'] = goods_dict[product_id]['quantity']
                    filtered_products.append(product_copy)
        else:
            # Для "Всех складов" показываем все товары без количества
            filtered_products = all_products

        warehouses_data = warehouse_online_mgr.get_warehouses()
        print(f"defub | Warehouses data for template: {warehouses_data}")
        app.logger.info(f"Warehouses data for template: {warehouses_data}")

        return render_template(
            'index.html',
            section=section,
            products=filtered_products,
            warehouses=warehouses_data,
            selected_wh=selected_wh,
            invoices=warehouse_state_invoice.state_invoices
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

    warehouse_data = warehouse_state_invoice.state_invoices.get(warehouse_id, {})
    return jsonify({
        'invoices': warehouse_data.get('invoices', [])
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
    if form.validate_on_submit():
        warehouse['connected'] = True
        warehouse['auto_balance_params'] = {
            'threshold': form.threshold.data,
            'interval': form.interval.data
        }
        flash('Settings saved!')
        return redirect(url_for('index'))
    return render_template('settings.html', form=form)


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


class WarehouseManager:
    def __init__(self):
        self.warehouses = {}
        self.lock = threading.Lock()
        self.consumer_thread = threading.Thread(target=self._consume_updates, daemon=True)
        self.consumer_thread.start()

    def _consume_updates(self):
        while True:
            consumer = None
            try:
                consumer = KafkaConsumer(
                    app.config['KAFKA_WH_REGISTRY_TOPIC'],
                    bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    group_id='wi-warehouse-group-v2'
                )
                app.logger.info(f"Подписан на топик: {app.config['KAFKA_WH_REGISTRY_TOPIC']}")

                for message in consumer:
                    with stock_lock:
                        for warehouse_id, items in message.value.items():
                            # Преобразование pgd_id в строку и логирование
                            app.logger.info(f"Processing stock items: {items}")
                            warehouse_stock[warehouse_id] = {
                                str(item['pgd_id']): item['quantity']  # Гарантированное преобразование в строку
                                for item in items
                            }
                    self._cleanup_inactive()

                    # with stock_lock:
                    #     for warehouse_id, items in message.value.items():
                    #         # Преобразование pgd_id в строку и логирование
                    #         app.logger.info(f"Processing stock items: {items}")
                    #         warehouse_stock[warehouse_id] = {
                    #             str(item['pgd_id']): item['quantity']  # Гарантированное преобразование в строку
                    #             for item in items
                    #         }
                        #self._cleanup_inactive()

            except Exception as e:
                app.logger.error(f"Ошибка потребителя складов: {str(e)}")
                time.sleep(5)
            finally:
                if consumer:
                    consumer.close()

    def _cleanup_inactive(self):
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)  # Aware datetime
        inactive = [wh_id for wh_id, wh in self.warehouses.items()
                    if wh['last_seen'] < cutoff]
        #for wh_id in inactive:
        #    del self.warehouses[wh_id]

    def get_active_warehouses(self):
        with self.lock:
            return [
                {'wh_id': wh_id, **wh}
                for wh_id, wh in self.warehouses.items()
                if wh['status'] == 'active'
            ]

warehouse_manager = WarehouseManager()

@app.template_filter('wh_formatter')
def wh_formatter(wh):
    """Улучшенный фильтр форматирования"""
    if isinstance(wh, dict):
        name = wh.get('metadata', {}).get('name', wh['wh_id'])
        return f"{name} ({wh['wh_id'][:4]}...{wh['wh_id'][-4:]})"

    if isinstance(wh, str):
        return f"{wh[:4]}...{wh[-4:]}"

    return str(wh)


# Добавляем новый consumer для обновлений накладных
def start_invoice_updates_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                'invoice_updates',
                bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='wi-invoice-group-v2'
            )
            for message in consumer:
                try:
                    invoice = message.value
                    if not isinstance(invoice, dict):
                        raise ValueError("Invoice is not a dictionary")

                    if 'id' not in invoice:
                        app.logger.warning("Invoice missing ID: %s", invoice)
                        continue

                    #with invoices_cache_lock:
                # Логика обновления кэша
                except Exception as e:
                    app.logger.error(f"Invoice processing failed: {str(e)}")
        except Exception as e:
            app.logger.error(f"Invoice consumer error: {str(e)}")
            time.sleep(5)
# def start_invoice_updates_consumer():
#     while True:
#         try:
#             consumer = KafkaConsumer(
#                 app.config['KAFKA_STOCK_TOPIC'],
#                 bootstrap_servers=app.config['KAFKA_BOOTSTRAP_SERVERS'],
#                 value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#                 group_id='wi-stock-group-v2',  # Новый group_id для сброса офсетов
#                 auto_offset_reset='earliest'
#             )
#
#             for message in consumer:
#                 with invoices_cache_lock:
#                     invoice = message.value
#                     # Обновляем или добавляем накладную
#                     existing = next((i for i in invoices_cache if i['id'] == invoice['id']), None)
#                     if existing:
#                         existing.update(invoice)
#                     else:
#                         invoices_cache.append(invoice)
#
#         except Exception as e:
#             app.logger.error(f"Invoice consumer error: {str(e)}")
#             time.sleep(5)



@app.route('/get_warehouse_goods', methods=['POST'])
@login_required
def get_warehouse_goods():
    wh_id = request.json.get('wh_id')
    if wh_id:
        # Отправляем запрос в Kafka
        producer.send(Config.KAFKA_GOODS_REQUEST_TOPIC, {
            'wh_id': wh_id,
            'command': 'get_all_goods'
        })
        producer.flush()
        return {'status': 'request_sent'}
    return {'status': 'error'}

@app.route('/current_goods')
@login_required
def current_goods():
    wh_id = request.args.get('wh_id')
    return {'goods': goods_handler.get_goods(wh_id)}


def create_admin():
    with app.app_context():
        if not User.query.filter_by(username='adm').first():
            admin = User(username='adm', email='leo@gmail.com', is_admin=True)
            admin.set_password('1')
            db.session.add(admin)
            db.session.commit()
            print("Admin user created!")


if __name__ == '__main__':
    if check_kafka_connection():
        threading.Thread(target=start_stock_consumer, daemon=True).start()
        threading.Thread(target=start_invoice_updates_consumer, daemon=True).start()

    else:
        app.logger.error("Failed to connect to Kafka")

    warehouse_online_mgr = WarehouseOnlineManager()
    goods_handler = GoodsResponseHandler()
    warehouse_state_invoice = WarehouseStateInvoice()
    products_response = ProductsResponseHandler()

    with app.app_context():
        db.create_all()
    create_admin()
    app.run(debug=True, port=5010)
