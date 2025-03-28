from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, IntegerField
from wtforms.validators import DataRequired, Email, EqualTo, ValidationError, Length
from models import User
import re

class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Login')


def validate_password(password):
    errors = []

    # Проверка на латинские буквы и разрешенные символы
    if not re.fullmatch(r'^[A-Za-z0-9~!?@#$%^&*_\-+()\[\]{}><]+$', password):
        errors.append("Contains invalid characters. Only Latin letters, numbers and ~!?@#$%^&*_-+()[]{}>< allowed")

    # Проверка на наличие заглавных и строчных букв
    if not re.search(r'[A-Z]', password):
        errors.append("Must contain at least one uppercase letter")
    if not re.search(r'[a-z]', password):
        errors.append("Must contain at least one lowercase letter")

    # Проверка на наличие цифр
    if not re.search(r'\d', password):
        errors.append("Must contain at least one digit")

    # Проверка на пробелы
    if ' ' in password:
        errors.append("Spaces are not allowed")

    if errors:
        raise ValidationError('; '.join(errors))


class RegistrationForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[
        DataRequired(),
        Length(min=8, max=128, message='Password must be 8-128 characters'),
        EqualTo('password2', message='Passwords must match'),
        lambda form, field: validate_password(field.data)
    ])
    password2 = PasswordField('Repeat Password', validators=[
            DataRequired(),
            Length(min=8, max=128, message='Password must be 8-128 characters'),
            EqualTo('password2', message='Passwords must match'),
            lambda form, field: validate_password(field.data)
            , EqualTo('password')])
    submit = SubmitField('Register')

    def validate_username(self, username):
        user = User.query.filter_by(username=username.data).first()
        if user is not None:
            raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        user = User.query.filter_by(email=email.data).first()
        if user is not None:
            raise ValidationError('Email already registered. Please use a different email.')


class WarehouseSettingsForm(FlaskForm):
    threshold = IntegerField('Balance Threshold', validators=[DataRequired()])
    interval = IntegerField('Check Interval (minutes)', validators=[DataRequired()])
    submit = SubmitField('Save Settings')


