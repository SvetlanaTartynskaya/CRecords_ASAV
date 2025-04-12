from telegram import Update, ReplyKeyboardRemove
from telegram.ext import CallbackContext, ConversationHandler, MessageHandler, Filters
from datetime import datetime, timedelta, date
import sqlite3

# Подключение к базе данных
conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
cursor = conn.cursor()

# Состояния для ConversationHandler
ENTER_VACATION_START, ENTER_VACATION_END = range(2)

cursor.execute('''
CREATE TABLE IF NOT EXISTS User_Vacation (
    tab_number INTEGER PRIMARY KEY,
    start_date TEXT,
    end_date TEXT
)''')
conn.commit()

# Обработка кнопки "Я уволился"
def handle_resignation(update: Update, context: CallbackContext):
    tab_number = context.user_data.get('tab_number')
    if tab_number:
        delete_user(tab_number)
        update.message.reply_text("Вы уволились. Ваш аккаунт удален. До свидания!", reply_markup=ReplyKeyboardRemove())
    else:
        update.message.reply_text("Ошибка: табельный номер не найден.")
    return ConversationHandler.END

# Удаление пользователя из базы данных
def delete_user(tab_number):
    cursor.execute('DELETE FROM Users_admin_bot WHERE tab_number = ?', (tab_number,))
    cursor.execute('DELETE FROM Users_dir_bot WHERE tab_number = ?', (tab_number,))
    cursor.execute('DELETE FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
    cursor.execute('DELETE FROM User_Vacation WHERE tab_number = ?', (tab_number,))
    conn.commit()

# Обработка кнопки "Я в отпуске"
def handle_vacation_start(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("Введите дату начала отпуска в формате ДД.ММ.ГГГГ:")
    return ENTER_VACATION_START

# Обработка ввода даты начала отпуска
def handle_vacation_end(update: Update, context: CallbackContext) -> int:
    start_date_str = update.message.text
    try:
        # Проверка формата даты
        start_date = datetime.strptime(start_date_str, "%d.%m.%Y").date()
        today = date.today()
        
        # Проверка что дата начала не в прошлом
        if start_date < today:
            update.message.reply_text("Дата начала отпуска не может быть раньше сегодняшнего дня. Введите корректную дату:")
            return ENTER_VACATION_START
            
        context.user_data['vacation_start'] = start_date
        update.message.reply_text("Введите дату окончания отпуска в формате ДД.ММ.ГГГГ:")
        return ENTER_VACATION_END
    except ValueError:
        update.message.reply_text("Неверный формат даты. Введите дату в формате ДД.ММ.ГГГГ:")
        return ENTER_VACATION_START

# Обработка ввода даты окончания отпуска
def handle_vacation_confirmation(update: Update, context: CallbackContext) -> int:
    end_date_str = update.message.text
    try:
        end_date = datetime.strptime(end_date_str, "%d.%m.%Y").date()
        start_date = context.user_data['vacation_start']
        today = date.today()
        
        # Проверка что дата окончания позже даты начала
        if end_date <= start_date:
            update.message.reply_text("Дата окончания отпуска должна быть позже даты начала. Попробуйте снова.")
            return ENTER_VACATION_END
            
        # Проверка что отпуск не длится дольше 3 недель (21 день)
        max_vacation_duration = timedelta(days=21)
        actual_duration = (end_date - start_date).days
        
        if actual_duration > 21:
            update.message.reply_text(f"Отпуск не может длиться больше 3 недель (21 дня). Ваш отпуск: {actual_duration} дней. Введите корректную дату окончания:")
            return ENTER_VACATION_END
            
        # Проверка что дата начала не в прошлом (на случай если пользователь долго вводил данные)
        if start_date < today:
            update.message.reply_text("Дата начала отпуска теперь в прошлом. Пожалуйста, начните процесс заново.")
            return ConversationHandler.END
            
        # Сохранение дат отпуска в базу данных
        tab_number = context.user_data.get('tab_number')
        if tab_number:
            save_vacation_dates(tab_number, start_date, end_date)
            update.message.reply_text(f"Ваш отпуск запланирован с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}. Хорошего отдыха!")
        else:
            update.message.reply_text("Ошибка: табельный номер не найден.")
        return ConversationHandler.END
    except ValueError:
        update.message.reply_text("Неверный формат даты. Введите дату в формате ДД.ММ.ГГГГ:")
        return ENTER_VACATION_END

# Сохранение дат отпуска в базу данных
def save_vacation_dates(tab_number, start_date, end_date):
    cursor.execute('''
    INSERT OR REPLACE INTO User_Vacation (tab_number, start_date, end_date)
    VALUES (?, ?, ?)
    ''', (tab_number, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    conn.commit()


# Создание ConversationHandler для отпуска
def get_vacation_conversation_handler():
    return ConversationHandler(
        entry_points=[MessageHandler(Filters.regex('^Я в отпуске$'), handle_vacation_start)],
        states={
            ENTER_VACATION_START: [MessageHandler(Filters.text & ~Filters.command, handle_vacation_end)],
            ENTER_VACATION_END: [MessageHandler(Filters.text & ~Filters.command, handle_vacation_confirmation)],
        },
        fallbacks=[],
    )