import pandas as pd
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler, CallbackContext
import sqlite3
import pytz
from datetime import time, datetime
from buttons_handler import handle_resignation, get_vacation_conversation_handler

# Состояния для ConversationHandler
ENTER_TAB_NUMBER, = range(1)

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
                update.message.reply_text(f"Здравствуйте, {name}! Ваша роль: {role}.\nЛокация: {location}.\nПодразделение: {division}")
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
        update.message.reply_text("Вы в отпуске. Ваш статус обновлен.")
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
    # Очищаем все данные пользователя
    context.user_data.clear()
    
    # Отправляем сообщение с инструкцией
    update.message.reply_text(
        "Вы вернулись в начало работы с ботом.\n\n"
        "Для начала работы введите ваш табельный номер:",
        reply_markup=ReplyKeyboardMarkup([['/start']], one_time_keyboard=True)
    )
    
    # Возвращаем состояние ENTER_TAB_NUMBER, если используется ConversationHandler
    return ENTER_TAB_NUMBER

def main():
    updater = Updater("7575482607:AAG9iLYAO2DFpjHVBDn3-m-tLicdNXBsyBQ", use_context=True)
    dispatcher = updater.dispatcher

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            ENTER_TAB_NUMBER: [MessageHandler(Filters.text & ~Filters.command, handle_tab_number)],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            MessageHandler(Filters.regex('^В начало$'), return_to_start)  # Добавляем обработчик
        ],
    )

    dispatcher.add_handler(conv_handler)
    dispatcher.add_handler(get_vacation_conversation_handler())
    # Обновляем обработчик кнопок
    dispatcher.add_handler(MessageHandler(Filters.regex('^(Я уволился|Я в отпуске|В начало)$'), handle_button))

    # Настройка ежедневного обновления в 8:00 по Москве
    job_queue = updater.job_queue
    moscow_tz = pytz.timezone('Europe/Moscow')
    job_queue.run_daily(daily_update, time(hour=8, tzinfo=moscow_tz))

    # Первоначальное обновление БД при запуске
    update_db_from_excel()

    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()