#app.py
import time
from collections import defaultdict
from flask import Flask, render_template, redirect, url_for, request, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from kafka.errors import KafkaError, NoBrokersAvailable

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

#kafka
products_cache = []
products_cache_lock = threading.Lock()  # Добавляем блокировку
warehouse_stock = defaultdict(dict)
stock_lock = threading.Lock()


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
        return True
    except Exception as e:
        app.logger.error(f"Kafka connection error: {str(e)}")
        return False


def start_kafka_consumer():
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
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

            app.logger.info("Kafka consumer connected. Waiting for messages...")

            for message in consumer:
                try:
                    products = message.value
                    if not isinstance(products, list):
                        raise ValueError("Invalid message format")

                    app.logger.info(f"Received {len(products)} products")

                    with products_cache_lock:
                        products_cache.clear()
                        products_cache.extend(products)

                except Exception as e:
                    app.logger.error(f"Error processing message: {str(e)}")

        except (KafkaError, NoBrokersAvailable) as e:
            app.logger.error(f"Kafka connection error: {str(e)}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            app.logger.error(f"Unexpected error: {str(e)}")
            time.sleep(5)
        finally:
            if consumer:
                try:
                    consumer.close()
                except:
                    pass


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
                app.logger.debug(f"Received stock message: {message.value}")
                with stock_lock:
                    for warehouse, items in message.value.items():
                        warehouse_stock[warehouse] = {
                            item['pgd_id']: item['quantity']
                            for item in items
                        }
                        app.logger.info(f"Updated stock for warehouse: {warehouse}")

        except Exception as e:
            app.logger.error(f"Stock consumer error: {str(e)}")
            time.sleep(5)


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


@app.route('/')
@login_required
def index():
    section = request.args.get('section', 'products')
    selected_wh = request.args.get('warehouse', 'all')

    with products_cache_lock:
        products = products_cache.copy()

    with stock_lock:
        if selected_wh != 'all':
            stock = warehouse_stock.get(selected_wh, {})
            products = [p for p in products if stock.get(p['id'], 0) > 0]

    return render_template(
        'index.html',
        section=section,
        products=products,
        warehouses=list(warehouse_stock.keys()),
        selected_wh=selected_wh
    )


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
    try:
        invoice_type = request.form.get('type')
        sender_wh = request.form.get('warehouse')

        # Для departure указываем получателя
        receiver_wh = "WHAAAAAARUS060ru00000002" if invoice_type == "departure" else sender_wh

        # Проверка совпадения складов
        if invoice_type == "arrival" and sender_wh == Config.RECIPIENT_WAREHOUSE:
            raise ValueError("Невозможно принять товар на тот же склад")

        # Формируем сообщение с метаданными
        message = {
            'headers': {
                'sender': sender_wh,
                'receiver': receiver_wh,
                'type': invoice_type
            },
            'body': {
                'items': [
                    {'pgd_id': int(request.form[f'items[{i}][id]']),
                     'quantity': int(request.form[f'items[{i}][quantity]'])}
                    for i in range(len(request.form) // 2)
                ]
            }
        }

        # Отправка в Kafka
        producer.send(
            app.config['KAFKA_INVOICE_TOPIC'],
            value=json.dumps(message).encode('utf-8'),
            headers=[
                ('sender', sender_wh.encode()),
                ('receiver', receiver_wh.encode())
            ]
        )
        producer.flush()

        flash('Накладная создана', 'success')
    except Exception as e:
        app.logger.error(f"Ошибка: {str(e)}")
        flash(f'Ошибка: {str(e)}', 'danger')

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
        threading.Thread(target=start_kafka_consumer, daemon=True).start()
        threading.Thread(target=start_stock_consumer, daemon=True).start()
    else:
        app.logger.error("Failed to connect to Kafka")

    with app.app_context():
        db.create_all()
    create_admin()
    app.run(debug=True, port=5010)
