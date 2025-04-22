import pandas as pd
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler, CallbackContext, CallbackQueryHandler
import sqlite3
import pytz
from datetime import time, datetime
from buttons_handler import handle_resignation, get_vacation_conversation_handler, handle_vacation_start
import os
import logging
import glob
import io

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
ENTER_TAB_NUMBER, ENTER_READINGS, SELECT_EQUIPMENT, ENTER_VALUE, CONFIRM_READINGS = range(5)

conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
cursor = conn.cursor()

# Создание таблиц, если они не существуют
cursor.execute('''
CREATE TABLE IF NOT EXISTS Users_admin_bot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tab_number INTEGER UNIQUE,
    name TEXT,
    role TEXT,
    t_number INTEGER,
    location TEXT,
    division TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Users_user_bot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tab_number INTEGER UNIQUE,
    name TEXT,
    role TEXT,
    t_number INTEGER,
    location TEXT,
    division TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Users_dir_bot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tab_number INTEGER UNIQUE,
    name TEXT,
    t_number INTEGER,
    role TEXT,
    location TEXT,
    division TEXT
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS shifts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    tab_number INTEGER UNIQUE,
    is_on_shift BOOLEAN,
    FOREIGN KEY (tab_number) REFERENCES Users_user_bot(tab_number)
)''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS vacations (
    tab_number INTEGER PRIMARY KEY,
    start_date TEXT,
    end_date TEXT,
    FOREIGN KEY (tab_number) REFERENCES Users_user_bot(tab_number)
)''')

conn.commit()

# Загрузка таблицы пользователей
def load_users_table():
    try:
        df = pd.read_excel('Users.xlsx')
        return df
    except Exception as e:
        print(f"Ошибка при загрузке файла Users.xlsx: {e}")
        return pd.DataFrame()

# Загрузка таблицы смен
def load_shifts_table():
    try:
        df = pd.read_excel('shifts.xlsx')
        return df
    except Exception as e:
        print(f"Ошибка при загрузке файла shifts.xlsx: {e}")
        return pd.DataFrame()

# Обработка команды /start
def start(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("Привет! Введите ваш табельный номер:")
    return ENTER_TAB_NUMBER

# Обработка введенного табельного номера
def handle_tab_number(update: Update, context: CallbackContext) -> int:
    try:
        tab_number = update.message.text
        if not tab_number.isdigit():
            update.message.reply_text("Табельный номер должен состоять только из цифр. Пожалуйста, введите корректный номер:")
            return ENTER_TAB_NUMBER
            
        df_users = load_users_table()
        if df_users.empty:
            update.message.reply_text("База данных пользователей недоступна. Попробуйте позже.")
            return ConversationHandler.END
            
        user = df_users[df_users['Табельный номер'] == int(tab_number)]
        
        if not user.empty:
            name = user['ФИО'].values[0]
            role = determine_role(user)
            t_number = user['Номер телефона'].values[0]
            location = user['Локация'].values[0]
            division = user['Подразделение'].values[0] if 'Подразделение' in user.columns else ""
            context.user_data['role'] = role
            context.user_data['tab_number'] = int(tab_number)
            
            if not is_user_in_db(int(tab_number), role):
                add_user_to_db(int(tab_number), name, role, t_number, location, division)
                update.message.reply_text(f"Здравствуйте, {name}!\n Ваша роль: {role}.\nЛокация: {location}.\nПодразделение: {division}")
            else:
                update.message.reply_text(f"Здравствуйте, {name}! Вы уже зарегистрированы в системе.")
            
            # Разные сообщения для разных ролей
            if role in ['Администратор', 'Руководитель']:
                update.message.reply_text("✅ Вы имеете постоянный доступ к боту.")
            else:
                if check_shift_status(int(tab_number)):
                    update.message.reply_text("✅ Вы на вахте. Бот доступен для работы.")
                else:
                    update.message.reply_text("⛔ В настоящее время вы не на вахте. Бот недоступен.")
            
            show_role_specific_menu(update, role)
            return ConversationHandler.END
        else:
            update.message.reply_text("Пользователь с таким табельным номером не найден.")
            return ENTER_TAB_NUMBER
        
    except Exception as e:
        update.message.reply_text(f"Произошла ошибка: {str(e)}")
        return ConversationHandler.END

# Проверка статуса вахты
def check_shift_status(tab_number):
    try:
        cursor.execute('SELECT is_on_shift FROM shifts WHERE tab_number = ?', (tab_number,))
        result = cursor.fetchone()
        
        if result is None:
            return False
            
        # Обрабатываем разные возможные форматы данных
        status = str(result[0]).upper().strip()
        return status in ["ДА", "YES", "TRUE", "1", "1.0"]
    except Exception as e:
        print(f"Ошибка при проверке статуса вахты: {e}")
        return False

def is_user_available(tab_number: int, role: str) -> bool:
    """Проверяет, доступен ли пользователь.
    Для администраторов и руководителей - всегда доступен.
    Для обычных пользователей - только на вахте и не в отпуске."""
    try:
        # Администраторы и руководители всегда имеют доступ
        if role in ['Администратор', 'Руководитель']:
            return True
        # 1. Проверяем отпуск (для всех ролей)
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('''
            SELECT 1 FROM vacations 
            WHERE tab_number = ? AND start_date <= ? AND end_date >= ?
        ''', (tab_number, today, today))
        if cursor.fetchone():
            return False
        # 2. Для обычных пользователей проверяем статус смены
        return check_shift_status(tab_number)
    except Exception as e:
        print(f"Ошибка проверки доступности: {e}")
        return False  # В случае ошибки считаем, что пользователь недоступен

def check_access(update: Update, context: CallbackContext) -> bool:
    # Проверка доступа перед выполнением команд
    if 'tab_number' not in context.user_data or 'role' not in context.user_data:
        update.message.reply_text("Пожалуйста, сначала введите ваш табельный номер через /start")
        return False
    
    tab_number = context.user_data['tab_number']
    role = context.user_data['role']
    
    if not is_user_available(tab_number, role):
        update.message.reply_text("⛔ В настоящее время бот недоступен для вас (вы не на смене или в отпуске)")
        return False
    return True

# Определение роли пользователя
def determine_role(user):
    role = user['Роль'].values[0] if 'Роль' in user.columns else "Пользователь"
    
    if 'Администратор' in str(role):
        return 'Администратор'
    elif 'Руководитель' in str(role):
        return 'Руководитель'
    else:
        return 'Пользователь'

# Показ меню в зависимости от роли
def show_role_specific_menu(update: Update, role: str):
    keyboard = [['Я уволился', 'Я в отпуске', 'В начало']]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    
    if role == 'Администратор':
        update.message.reply_text("Доступные команды для администратора: /admin_command", reply_markup=reply_markup)
    elif role == 'Руководитель':
        update.message.reply_text("Доступные команды для руководителя: /manager_command", reply_markup=reply_markup)
    else:
        update.message.reply_text("Доступные команды для пользователя: /user_command", reply_markup=reply_markup)

def handle_button(update: Update, context: CallbackContext):
    text = update.message.text
    if text == 'Я уволился':
        handle_resignation(update, context)
    elif text == 'Я в отпуске':
        # Запускаем обработчик отпуска из buttons_handler
        return handle_vacation_start(update, context)
    elif text == 'В начало':
        return return_to_start(update, context)

# Удаление пользователя из базы данных
def delete_user(tab_number, role):
    try:
        if role == 'Администратор':
            cursor.execute('DELETE FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
        elif role == 'Руководитель':
            cursor.execute('DELETE FROM Users_dir_bot WHERE tab_number = ?', (tab_number,))
        else:
            cursor.execute('DELETE FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
        
        # Также удаляем из таблицы смен
        cursor.execute('DELETE FROM shifts WHERE tab_number = ?', (tab_number,))
        conn.commit()
        return True
    except Exception as e:
        print(f"Ошибка при удалении пользователя: {e}")
        return False

# Проверка, существует ли пользователь в базе данных
def is_user_in_db(tab_number, role):
    try:
        if role == 'Администратор':
            cursor.execute('SELECT * FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
        elif role == 'Руководитель':
            cursor.execute('SELECT * FROM Users_dir_bot WHERE tab_number = ?', (tab_number,))
        else:
            cursor.execute('SELECT * FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
        
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"Ошибка при проверке пользователя в БД: {e}")
        return False

# Добавление пользователя в соответствующую таблицу базы данных
def add_user_to_db(tab_number, name, role, t_number, location, division):
    try:
        if role == 'Администратор':
            cursor.execute('INSERT INTO Users_admin_bot (tab_number, name, role, t_number, location, division) VALUES (?, ?, ?, ?, ?, ?)', 
                         (tab_number, name, role, t_number, location, division))
        elif role == 'Руководитель':
            cursor.execute('INSERT INTO Users_dir_bot (tab_number, name, role, t_number, location, division) VALUES (?, ?, ?, ?, ?, ?)', 
                         (tab_number, name, role, t_number, location, division))
        else:
            cursor.execute('INSERT INTO Users_user_bot (tab_number, name, role, t_number, location, division) VALUES (?, ?, ?, ?, ?, ?)', 
                         (tab_number, name, role, t_number, location, division))
        conn.commit()
        return True
    except Exception as e:
        print(f"Ошибка при добавлении пользователя в БД: {e}")
        return False

def update_shifts_from_excel():
    try:
        df = load_shifts_table()
        if not df.empty:
            # Очистка таблицы перед обновлением
            cursor.execute('DELETE FROM shifts')
            
            # Вставка новых данных
            for _, row in df.iterrows():
                tab_number = row['tab_number']
                name = row['name']
                shift_status = str(row['is_on_shift']).upper().strip() if pd.notna(row['is_on_shift']) else "НЕТ"
                is_on_shift = shift_status in ["ДА", "YES", "TRUE", "1", "1.0"]
                
                cursor.execute('''
                INSERT INTO shifts (name, tab_number, is_on_shift)
                VALUES (?, ?, ?)
                ON CONFLICT(tab_number) DO UPDATE SET
                    name = excluded.name,
                    is_on_shift = excluded.is_on_shift
                ''', (name, tab_number, is_on_shift))
            
            conn.commit()
            print("Данные о сменах в БД обновлены.")
    except FileNotFoundError:
        print("Файл shifts.xlsx не найден.")
    except Exception as e:
        print(f"Ошибка при обновлении таблицы смен: {e}")

# Обновление всех таблиц из Excel
def update_db_from_excel():
    try:
        # Обновляем таблицу пользователей
        df_users = load_users_table()
        if not df_users.empty:
            # Очистка таблиц перед обновлением
            cursor.execute('DELETE FROM Users_admin_bot')
            cursor.execute('DELETE FROM Users_dir_bot')
            cursor.execute('DELETE FROM Users_user_bot')
            
            # Вставка новых данных
            for _, row in df_users.iterrows():
                tab_number = row['Табельный номер']
                name = row['ФИО']
                role = determine_role(pd.DataFrame([row]))
                t_number = row['Номер телефона']
                location = row['Локация']
                division = row['Подразделение'] if 'Подразделение' in row else ""
                
                add_user_to_db(tab_number, name, role, t_number, location, division)
            
            conn.commit()
            print("Данные пользователей в БД обновлены.")
        
        # Обновляем таблицу смен
        update_shifts_from_excel()
        
    except Exception as e:
        print(f"Ошибка при обновлении БД: {e}")

def daily_update(context: CallbackContext):
    update_db_from_excel()

def cancel(update: Update, context: CallbackContext) -> int:
    """Отменяет текущее действие и возвращает пользователя в главное меню."""
    user = update.message.from_user
    context.user_data.clear()  # Очищаем временные данные пользователя
    
    # Получаем роль пользователя из контекста или базы данных
    role = context.user_data.get('role')
    if not role:
        try:
            tab_number = context.user_data.get('tab_number')
            if tab_number:
                cursor.execute('SELECT role FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
                result = cursor.fetchone()
                role = result[0] if result else 'Пользователь'
        except Exception as e:
            print(f"Ошибка при получении роли: {e}")
            role = 'Пользователь'
    
    update.message.reply_text(
        "❌ Текущее действие отменено.\n\n"
        "Вы можете начать заново с команды /start",
        reply_markup=ReplyKeyboardMarkup([['/start']], one_time_keyboard=True)
    )
    if role:
        show_role_specific_menu(update, role)
    
    return ConversationHandler.END

def return_to_start(update: Update, context: CallbackContext):
    context.user_data.clear()
    
    # Отправляем сообщение с инструкцией
    update.message.reply_text(
        "Вы вернулись в начало работы с ботом.\n\n"
        "Для начала работы введите ваш табельный номер:",
        reply_markup=ReplyKeyboardMarkup([['/start']], one_time_keyboard=True)
    )
    
    # Возвращаем состояние ENTER_TAB_NUMBER, если используется ConversationHandler
    return ENTER_TAB_NUMBER

# Обработчик команды для администраторов
def admin_command(update: Update, context: CallbackContext):
    # Проверка прав доступа
    if not check_access(update, context):
        return
        
    role = context.user_data.get('role')
    if role != 'Администратор':
        update.message.reply_text("Эта команда доступна только для администраторов.")
        return
        
    keyboard = [
        ['Выгрузить данные', 'Редактировать справочники'],
        ['Список пользователей', 'Назад']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    update.message.reply_text(
        "Панель администратора. Выберите действие:",
        reply_markup=reply_markup
    )

# Обработчик команды для руководителей
def manager_command(update: Update, context: CallbackContext):
    # Проверка прав доступа
    if not check_access(update, context):
        return
        
    role = context.user_data.get('role')
    if role != 'Руководитель':
        update.message.reply_text("Эта команда доступна только для руководителей.")
        return
        
    keyboard = [
        ['Выгрузить данные', 'Статистика показаний'],
        ['Список пользователей', 'Назад']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    update.message.reply_text(
        "Панель руководителя. Выберите действие:",
        reply_markup=reply_markup
    )

# Обработчик команды для пользователей
def user_command(update: Update, context: CallbackContext):
    # Проверка прав доступа
    if not check_access(update, context):
        return
        
    keyboard = [
        ['Загрузить показания', 'Мой профиль'],
        ['Назад']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True)
    update.message.reply_text(
        "Панель пользователя. Выберите действие:",
        reply_markup=reply_markup
    )

# Обработчик для кнопки "Загрузить показания"
def handle_upload_readings(update: Update, context: CallbackContext):
    if not check_access(update, context):
        return ConversationHandler.END
        
    tab_number = context.user_data.get('tab_number')
    
    # Получаем информацию о пользователе
    cursor.execute('''
        SELECT name, location, division FROM Users_user_bot 
        WHERE tab_number = ?
    ''', (tab_number,))
    user_data = cursor.fetchone()
    
    if not user_data:
        update.message.reply_text("Ошибка: пользователь не найден в базе данных.")
        return ConversationHandler.END
        
    name, location, division = user_data
    
    keyboard = [
        [InlineKeyboardButton("Загрузить Excel файл", callback_data='upload_excel')],
        [InlineKeyboardButton("Ввести показания вручную", callback_data='enter_readings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"Выберите способ подачи показаний счетчиков:\n\n"
        f"📍 Локация: {location}\n"
        f"🏢 Подразделение: {division}",
        reply_markup=reply_markup
    )
    return ENTER_READINGS

# Обработчик выбора способа загрузки показаний
def readings_choice_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    if query.data == 'upload_excel':
        query.edit_message_text(
            "Пожалуйста, отправьте заполненный Excel файл с показаниями.\n\n"
            "Файл должен содержать столбцы:\n"
            "№ п/п, Гос. номер, Инв. №, Счётчик, Показания, Комментарий"
        )
        # Здесь не возвращаем следующее состояние, так как файл будет обрабатываться отдельным обработчиком
        return ConversationHandler.END
    elif query.data == 'enter_readings':
        # Получаем список оборудования для данного пользователя
        tab_number = context.user_data.get('tab_number')
        
        cursor.execute('''
            SELECT location, division FROM Users_user_bot 
            WHERE tab_number = ?
        ''', (tab_number,))
        user_location = cursor.fetchone()
        
        if not user_location:
            query.edit_message_text("Ошибка: не удалось получить информацию о пользователе")
            return ConversationHandler.END
            
        location, division = user_location
        
        # Получаем список оборудования для данной локации и подразделения
        try:
            from check import MeterValidator
            validator = MeterValidator()
            equipment_df = validator._get_equipment_for_location_division(location, division)
            
            if equipment_df.empty:
                query.edit_message_text(
                    f"На вашей локации ({location}, {division}) нет оборудования для ввода показаний. "
                    f"Обратитесь к администратору."
                )
                return ConversationHandler.END
            
            # Сохраняем список оборудования в контексте пользователя
            context.user_data['equipment'] = equipment_df.to_dict('records')
            
            # Создаем клавиатуру с оборудованием
            keyboard = []
            for index, row in equipment_df.iterrows():
                inv_num = row['Инв. №']
                meter_type = row['Счётчик']
                gos_number = row['Гос. номер'] if 'Гос. номер' in row else "N/A"
                
                # Ограничиваем длину для корректного отображения
                label = f"{gos_number} | {inv_num} | {meter_type}"
                if len(label) > 30:
                    label = label[:27] + "..."
                
                keyboard.append([
                    InlineKeyboardButton(
                        label, 
                        callback_data=f"equip_{index}"
                    )
                ])
            
            # Добавляем кнопку завершения
            keyboard.append([InlineKeyboardButton("🔄 Завершить и отправить", callback_data="finish_readings")])
            
            # Создаем таблицу для сбора показаний в контексте пользователя
            if 'readings_data' not in context.user_data:
                context.user_data['readings_data'] = {}
                
            query.edit_message_text(
                "Выберите оборудование для ввода показаний:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return SELECT_EQUIPMENT
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка оборудования: {e}")
            query.edit_message_text(f"Ошибка при получении списка оборудования: {str(e)}")
            return ConversationHandler.END

# Обработчик выбора оборудования для ввода показаний
def select_equipment_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    
    if query.data == "finish_readings":
        # Проверяем, что есть хотя бы одно введенное показание
        if not context.user_data.get('readings_data'):
            query.edit_message_text("Вы не ввели ни одного показания. Процесс отменен.")
            return ConversationHandler.END
            
        # Переходим к подтверждению и отправке показаний
        return confirm_readings(update, context)
    
    # Получаем индекс выбранного оборудования
    equip_index = int(query.data.split('_')[1])
    equipment = context.user_data['equipment'][equip_index]
    
    # Сохраняем текущий выбор в контексте
    context.user_data['current_equipment'] = equipment
    context.user_data['current_equip_index'] = equip_index
    
    # Получаем последнее показание для этого счетчика
    from check import MeterValidator
    validator = MeterValidator()
    last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
    
    last_reading_info = ""
    if last_reading:
        last_reading_info = f"\n\nПоследнее показание: {last_reading['reading']} ({last_reading['reading_date']})"
    
    # Создаем опции для ввода показаний
    keyboard = [
        [InlineKeyboardButton("Ввести показание", callback_data="enter_value")],
        [
            InlineKeyboardButton("Неисправен", callback_data="comment_Неисправен"),
            InlineKeyboardButton("В ремонте", callback_data="comment_В ремонте")
        ],
        [
            InlineKeyboardButton("Убыло", callback_data="comment_Убыло"),
            InlineKeyboardButton("« Назад", callback_data="back_to_list")
        ]
    ]
    
    query.edit_message_text(
        f"Оборудование:\n"
        f"Гос. номер: {equipment['Гос. номер']}\n"
        f"Инв. №: {equipment['Инв. №']}\n"
        f"Счётчик: {equipment['Счётчик']}{last_reading_info}\n\n"
        f"Выберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return ENTER_VALUE

# Обработчик ввода значения или комментария
def enter_value_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    
    if not query:  # Если это текстовое сообщение (а не нажатие кнопки)
        try:
            value = float(update.message.text)
            if value < 0:
                update.message.reply_text("Показание не может быть отрицательным. Пожалуйста, введите положительное число.")
                return ENTER_VALUE
                
            # Сохраняем введенное значение
            equipment = context.user_data['current_equipment']
            equip_index = context.user_data['current_equip_index']
            
            # Проверяем, что значение не меньше предыдущего
            from check import MeterValidator
            validator = MeterValidator()
            last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
            
            if last_reading and value < last_reading['reading']:
                update.message.reply_text(
                    f"Ошибка: введенное показание ({value}) меньше предыдущего ({last_reading['reading']}). "
                    f"Пожалуйста, введите корректное значение."
                )
                return ENTER_VALUE
            
            # Проверки по типу счетчика
            if last_reading:
                days_between = validator._get_days_between(last_reading['reading_date'])
                if days_between > 0:
                    daily_change = (value - last_reading['reading']) / days_between
                    
                    if equipment['Счётчик'].startswith('PM') and daily_change > 24:
                        update.message.reply_text(
                            f"Предупреждение: Слишком большое изменение для счетчика PM ({daily_change:.2f} в сутки). "
                            f"Максимально допустимое изменение: 24 в сутки."
                        )
                        
                    if equipment['Счётчик'].startswith('KM') and daily_change > 500:
                        update.message.reply_text(
                            f"Предупреждение: Слишком большое изменение для счетчика KM ({daily_change:.2f} в сутки). "
                            f"Максимально допустимое изменение: 500 в сутки."
                        )
            
            context.user_data['readings_data'][equip_index] = {
                'value': value,
                'comment': '',
                'equipment': equipment
            }
            
            # Возвращаемся к списку оборудования
            equipment_keyboard = []
            for i, equip in enumerate(context.user_data['equipment']):
                # Отмечаем оборудование, для которого уже введены данные
                prefix = "✅ " if i in context.user_data['readings_data'] else ""
                
                label = f"{prefix}{equip['Гос. номер']} | {equip['Инв. №']} | {equip['Счётчик']}"
                if len(label) > 30:
                    label = label[:27] + "..."
                    
                equipment_keyboard.append([
                    InlineKeyboardButton(label, callback_data=f"equip_{i}")
                ])
            
            equipment_keyboard.append([InlineKeyboardButton("🔄 Завершить и отправить", callback_data="finish_readings")])
            
            update.message.reply_text(
                f"Показание {value} для {equipment['Инв. №']} ({equipment['Счётчик']}) сохранено.\n\n"
                f"Выберите следующее оборудование или завершите ввод:",
                reply_markup=InlineKeyboardMarkup(equipment_keyboard)
            )
            return SELECT_EQUIPMENT
            
        except ValueError:
            update.message.reply_text("Пожалуйста, введите числовое значение.")
            return ENTER_VALUE
    else:
        query.answer()
        
        if query.data == "back_to_list":
            # Возвращаемся к списку оборудования
            equipment_keyboard = []
            for i, equip in enumerate(context.user_data['equipment']):
                # Отмечаем оборудование, для которого уже введены данные
                prefix = "✅ " if i in context.user_data['readings_data'] else ""
                
                label = f"{prefix}{equip['Гос. номер']} | {equip['Инв. №']} | {equip['Счётчик']}"
                if len(label) > 30:
                    label = label[:27] + "..."
                    
                equipment_keyboard.append([
                    InlineKeyboardButton(label, callback_data=f"equip_{i}")
                ])
            
            equipment_keyboard.append([InlineKeyboardButton("🔄 Завершить и отправить", callback_data="finish_readings")])
            
            query.edit_message_text(
                "Выберите оборудование для ввода показаний:",
                reply_markup=InlineKeyboardMarkup(equipment_keyboard)
            )
            return SELECT_EQUIPMENT
        elif query.data == "enter_value":
            # Запрашиваем ввод числового значения
            query.edit_message_text(
                f"Оборудование: {context.user_data['current_equipment']['Инв. №']} ({context.user_data['current_equipment']['Счётчик']})\n\n"
                f"Введите числовое значение показания:"
            )
            return ENTER_VALUE
        elif query.data.startswith("comment_"):
            # Сохраняем комментарий без значения показания
            comment = query.data.split('_', 1)[1]
            equipment = context.user_data['current_equipment']
            equip_index = context.user_data['current_equip_index']
            
            # Если выбран "В ремонте", автоматически подставляем последнее показание
            value = None
            auto_value_message = ""
            
            if comment == "В ремонте":
                from check import MeterValidator
                validator = MeterValidator()
                last_reading = validator._get_last_reading(equipment['Инв. №'], equipment['Счётчик'])
                
                if last_reading:
                    value = last_reading['reading']
                    auto_value_message = f" (автоматически использовано последнее показание: {value})"
            
            context.user_data['readings_data'][equip_index] = {
                'value': value,
                'comment': comment,
                'equipment': equipment
            }
            
            # Возвращаемся к списку оборудования
            equipment_keyboard = []
            for i, equip in enumerate(context.user_data['equipment']):
                # Отмечаем оборудование, для которого уже введены данные
                prefix = "✅ " if i in context.user_data['readings_data'] else ""
                
                label = f"{prefix}{equip['Гос. номер']} | {equip['Инв. №']} | {equip['Счётчик']}"
                if len(label) > 30:
                    label = label[:27] + "..."
                    
                equipment_keyboard.append([
                    InlineKeyboardButton(label, callback_data=f"equip_{i}")
                ])
            
            equipment_keyboard.append([InlineKeyboardButton("🔄 Завершить и отправить", callback_data="finish_readings")])
            
            query.edit_message_text(
                f"Комментарий '{comment}' для {equipment['Инв. №']} ({equipment['Счётчик']}) сохранен{auto_value_message}.\n\n"
                f"Выберите следующее оборудование или завершите ввод:",
                reply_markup=InlineKeyboardMarkup(equipment_keyboard)
            )
            return SELECT_EQUIPMENT

# Подтверждение и отправка показаний
def confirm_readings(update: Update, context: CallbackContext):
    query = update.callback_query
    if query:
        query.answer()
    
    # Формируем данные для отображения и сохранения
    readings_data = context.user_data.get('readings_data', {})
    
    if not readings_data:
        if query:
            query.edit_message_text("Нет данных для отправки. Процесс отменен.")
        else:
            update.message.reply_text("Нет данных для отправки. Процесс отменен.")
        return ConversationHandler.END
    
    # Формируем таблицу показаний
    df = pd.DataFrame(columns=['№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий'])
    
    row_index = 1
    for equip_index, data in readings_data.items():
        equipment = data['equipment']
        df.loc[row_index] = [
            row_index,
            equipment['Гос. номер'],
            equipment['Инв. №'],
            equipment['Счётчик'],
            data['value'] if data['value'] is not None else '',
            data['comment']
        ]
        row_index += 1
    
    # Получаем данные пользователя
    tab_number = context.user_data.get('tab_number')
    cursor.execute('''
        SELECT name, location, division FROM Users_user_bot 
        WHERE tab_number = ?
    ''', (tab_number,))
    user_data = cursor.fetchone()
    name, location, division = user_data
    
    # Создаем директорию для отчетов, если не существует
    os.makedirs('meter_readings', exist_ok=True)
    
    # Создаем папку для отчетов текущей недели, если не существует
    current_week = datetime.now().strftime('%Y-W%U')  # Год-Номер недели
    report_folder = f'meter_readings/week_{current_week}'
    os.makedirs(report_folder, exist_ok=True)
    
    # Формируем имя файла
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = f'{report_folder}/meters_{location}_{division}_{tab_number}_{timestamp}.xlsx'
    
    # Добавляем метаданные
    user_info = {
        'name': name,
        'location': location,
        'division': division,
        'tab_number': tab_number,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    for key, value in user_info.items():
        df[key] = value
    
    # Сохраняем файл
    df.to_excel(file_path, index=False)
    
    # Валидируем созданный файл
    from check import MeterValidator
    validator = MeterValidator()
    validation_result = validator.validate_file(file_path, user_info)
    
    if not validation_result['is_valid']:
        errors_text = "\n".join(validation_result['errors'])
        error_message = f"Ошибки при проверке введенных показаний:\n\n{errors_text}\n\nПожалуйста, исправьте и попробуйте снова."
        
        if query:
            query.edit_message_text(error_message)
        else:
            update.message.reply_text(error_message)
        
        # Удаляем файл с ошибками
        try:
            os.remove(file_path)
        except:
            pass
        
        return ConversationHandler.END
    
    # Уведомляем пользователя об успешной отправке
    moscow_tz = pytz.timezone('Europe/Moscow')
    moscow_now = datetime.now(moscow_tz)
    moscow_time_str = moscow_now.strftime('%H:%M %d.%m.%Y')
    
    # Проверяем, является ли день пятницей (4) и время до 14:00
    is_on_time = moscow_now.weekday() == 4 and moscow_now.hour < 14
    
    if is_on_time:
        message_text = (f"✅ Спасибо! Ваши показания счетчиков приняты и прошли проверку.\n\n"
                       f"📍 Локация: {location}\n"
                       f"🏢 Подразделение: {division}\n"
                       f"⏰ Время получения: {moscow_time_str} МСК\n\n"
                       f"Показания предоставлены в срок. Благодарим за своевременную подачу данных!")
    else:
        message_text = (f"✅ Спасибо! Ваши показания счетчиков приняты и прошли проверку.\n\n"
                       f"📍 Локация: {location}\n"
                       f"🏢 Подразделение: {division}\n"
                       f"⏰ Время получения: {moscow_time_str} МСК")
    
    if query:
        query.edit_message_text(message_text)
    else:
        update.message.reply_text(message_text)
    
    # Уведомляем администраторов и руководителей
    from meters_handler import notify_admins_and_managers
    notify_admins_and_managers(context, tab_number, name, location, division, file_path)
    
    # Удаляем пользователя из списка тех, кому отправлено напоминание
    if 'missing_reports' in context.bot_data and tab_number in context.bot_data['missing_reports']:
        del context.bot_data['missing_reports'][tab_number]
        logger.info(f"Пользователь {name} удален из списка неотправивших отчеты")
    
    # Очищаем данные показаний
    if 'readings_data' in context.user_data:
        del context.user_data['readings_data']
    
    return ConversationHandler.END

def main():
    # Инициализация бота
    updater = Updater("7575482607:AAG9iLYAO2DFpjHVBDn3-m-tLicdNXBsyBQ", use_context=True)
    dispatcher = updater.dispatcher
    
    # Регистрация генератора итоговых отчетов
    from check import FinalReportGenerator, setup_approval_handler
    report_generator = FinalReportGenerator(updater.bot)
    dispatcher.bot_data['report_generator'] = report_generator
    
    # Регистрация обработчиков
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            ENTER_TAB_NUMBER: [MessageHandler(Filters.text & (~Filters.command), handle_tab_number)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )
    
    # Обработчики команд для разных ролей
    dispatcher.add_handler(CommandHandler('admin_command', admin_command))
    dispatcher.add_handler(CommandHandler('manager_command', manager_command))
    dispatcher.add_handler(CommandHandler('user_command', user_command))
    
    # Обработчик кнопок основного меню
    dispatcher.add_handler(MessageHandler(Filters.regex('^(Я уволился|Я в отпуске|В начало)$'), handle_button))
    
    # Обработчик кнопок интерфейса пользователя
    dispatcher.add_handler(MessageHandler(Filters.regex('^Загрузить показания$'), handle_upload_readings))
    
    # Обработчики для ввода показаний
    readings_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(Filters.regex('^Загрузить показания$'), handle_upload_readings)],
        states={
            ENTER_READINGS: [CallbackQueryHandler(readings_choice_handler)],
            SELECT_EQUIPMENT: [CallbackQueryHandler(select_equipment_handler)],
            ENTER_VALUE: [
                CallbackQueryHandler(enter_value_handler),
                MessageHandler(Filters.text & ~Filters.command, enter_value_handler)
            ],
            CONFIRM_READINGS: [CallbackQueryHandler(confirm_readings)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )
    dispatcher.add_handler(readings_conv_handler)
    
    # Обработчик конверсейшена для отпуска
    dispatcher.add_handler(get_vacation_conversation_handler())
    
    # Обработчик основного диалога
    dispatcher.add_handler(conv_handler)
    
    # Настройка обработчиков для работы с показаниями счетчиков
    from meters_handler import setup_meters_handlers
    setup_meters_handlers(dispatcher)
    
    # Настройка обработчика для кнопки "Всё верно"
    setup_approval_handler(dispatcher)
    
    # Настройка ежедневного обновления в 8:00 по Москве
    job_queue = updater.job_queue
    moscow_tz = pytz.timezone('Europe/Moscow')
    job_queue.run_daily(daily_update, time(hour=8, tzinfo=moscow_tz))

    # Первоначальное обновление БД при запуске
    update_db_from_excel()

    # Запуск бота
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()