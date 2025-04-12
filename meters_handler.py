import pandas as pd
from telegram import Update, InputFile
from telegram.ext import CallbackContext, MessageHandler, Filters
import io
import os
from datetime import time, datetime, timedelta
import pytz
import sqlite3
import logging
from typing import Dict, List, Tuple

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
cursor = conn.cursor()

# Состояния для ConversationHandler
WAITING_FOR_METERS_DATA = 1

# Часовые пояса России
RUSSIAN_TIMEZONES = {
    'Калининград': 'Europe/Kaliningrad',  # UTC+2
    'Москва': 'Europe/Moscow',            # UTC+3
    'Самара': 'Europe/Samara',            # UTC+4
    'Екатеринбург': 'Asia/Yekaterinburg', # UTC+5
    'Омск': 'Asia/Omsk',                  # UTC+6
    'Красноярск': 'Asia/Krasnoyarsk',     # UTC+7
    'Иркутск': 'Asia/Irkutsk',            # UTC+8
    'Якутск': 'Asia/Yakutsk',             # UTC+9
    'Владивосток': 'Asia/Vladivostok',    # UTC+10
    'Магадан': 'Asia/Magadan',            # UTC+11
    'Камчатка': 'Asia/Kamchatka'          # UTC+12
}

def get_timezone_for_location(location: str) -> str:
    """Определяем часовой пояс по названию локации"""
    location_lower = location.lower()
    
    if 'калининград' in location_lower:
        return RUSSIAN_TIMEZONES['Калининград']
    elif 'самара' in location_lower or 'татарстан' in location_lower or 'удмуртия' in location_lower:
        return RUSSIAN_TIMEZONES['Самара']
    elif 'екатеринбург' in location_lower or 'челябинск' in location_lower or 'тюмен' in location_lower:
        return RUSSIAN_TIMEZONES['Екатеринбург']
    elif 'омск' in location_lower or 'новосибирск' in location_lower or 'томск' in location_lower:
        return RUSSIAN_TIMEZONES['Омск']
    elif 'красноярск' in location_lower or 'хакасия' in location_lower or 'тыва' in location_lower:
        return RUSSIAN_TIMEZONES['Красноярск']
    elif 'иркутск' in location_lower or 'бурятия' in location_lower:
        return RUSSIAN_TIMEZONES['Иркутск']
    elif 'якутск' in location_lower or 'саха' in location_lower:
        return RUSSIAN_TIMEZONES['Якутск']
    elif 'владивосток' in location_lower or 'хабаровск' in location_lower:
        return RUSSIAN_TIMEZONES['Владивосток']
    elif 'магадан' in location_lower or 'сахалин' in location_lower:
        return RUSSIAN_TIMEZONES['Магадан']
    elif 'камчатка' in location_lower or 'чукотка' in location_lower:
        return RUSSIAN_TIMEZONES['Камчатка']
    else:
        return RUSSIAN_TIMEZONES['Москва']

def get_equipment_data() -> pd.DataFrame:
    """Получаем данные об оборудовании из 1С:ERP (заглушка)"""
    try:
        # В реальной реализации здесь будет подключение к 1С:ERP
        equipment_df = pd.read_excel('Equipment.xlsx')
        logger.info("Данные об оборудовании успешно загружены")
        return equipment_df
    except Exception as e:
        logger.error(f"Ошибка загрузки данных об оборудовании: {e}")
        return pd.DataFrame()

def get_users_on_shift() -> List[Tuple[int, str, str, str]]:
    """Получаем список пользователей на вахте"""
    try:
        cursor.execute('''
            SELECT u.tab_number, u.name, u.location, u.division 
            FROM Users_user_bot u
            JOIN shifts s ON u.tab_number = s.tab_number
            WHERE s.is_on_shift = "ДА"
        ''')
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения пользователей на вахте: {e}")
        return []

def schedule_weekly_reminders(context: CallbackContext):
    """Планирование еженедельных напоминаний"""
    try:
        logger.info("Запуск планирования еженедельных напоминаний")
        
        # Москва - базовый часовой пояс для планирования
        moscow_tz = pytz.timezone('Europe/Moscow')
        
        # Планируем задание на среду в 08:00 МСК
        context.job_queue.run_daily(
            callback=prepare_weekly_reminders,
            time=time(hour=8, minute=0, tzinfo=moscow_tz),
            days=(2,),  # 2 - среда
            name="weekly_meters_reminder"
        )
        
        logger.info("Еженедельные напоминания запланированы на среду в 08:00 МСК")
    except Exception as e:
        logger.error(f"Ошибка планирования еженедельных напоминаний: {e}")

def prepare_weekly_reminders(context: CallbackContext):
    """Подготовка еженедельных напоминаний в среду"""
    try:
        logger.info("Подготовка еженедельных напоминаний")
        
        # Получаем данные из 1С:ERP
        equipment_df = get_equipment_data()
        if equipment_df.empty:
            logger.error("Не удалось загрузить данные об оборудовании")
            return
        
        # Получаем пользователей на вахте
        users_on_shift = get_users_on_shift()
        if not users_on_shift:
            logger.info("Нет пользователей на вахте")
            return
        
        # Группируем оборудование по локациям и подразделениям
        grouped_equipment = equipment_df.groupby(['Локация', 'Подразделение'])
        
        # Для каждого пользователя на вахте готовим напоминание
        for user in users_on_shift:
            tab_number, name, location, division = user
            
            # Получаем оборудование для локации и подразделения пользователя
            try:
                equipment = grouped_equipment.get_group((location, division))
                if not equipment.empty:
                    # Рассчитываем местное время для отправки напоминания
                    timezone = get_timezone_for_location(location)
                    tz = pytz.timezone(timezone)
                    moscow_tz = pytz.timezone('Europe/Moscow')
                    
                    # Получаем текущее время в Москве и в локации пользователя
                    now_moscow = datetime.now(moscow_tz)
                    now_local = datetime.now(tz)
                    
                    # Разница во времени между Москвой и локацией пользователя
                    time_diff = now_local - now_moscow
                    
                    # Рассчитываем время отправки напоминания (14:00 МСК - разница во времени)
                    reminder_hour = 14 - (time_diff.total_seconds() // 3600)
                    if reminder_hour < 0:
                        reminder_hour += 24
                    elif reminder_hour >= 24:
                        reminder_hour -= 24
                    
                    # Планируем отправку напоминания
                    schedule_reminder(
                        context=context,
                        tab_number=tab_number,
                        name=name,
                        location=location,
                        division=division,
                        equipment=equipment,
                        hour=int(reminder_hour),
                        timezone=tz
                    )
            except KeyError:
                logger.info(f"Нет оборудования для {location}, {division}")
                continue
                
    except Exception as e:
        logger.error(f"Ошибка подготовки еженедельных напоминаний: {e}")

def schedule_reminder(context: CallbackContext, tab_number: int, name: str, 
                    location: str, division: str, equipment: pd.DataFrame,
                    hour: int, timezone: pytz.timezone):
    """Планирование напоминания"""
    try:
        # Планируем на пятницу в рассчитанное время
        context.job_queue.run_daily(
            callback=send_reminder,
            time=time(hour=hour, minute=0),  # Рассчитанное время
            days=(4,),  # 4 - это пятница
            context={
                'tab_number': tab_number,
                'name': name,
                'location': location,
                'division': division,
                'equipment': equipment.to_dict('records'),
                'deadline': '14:00 МСК'  # Срок сдачи показаний
            },
            name=f"reminder_{tab_number}",
            timezone=timezone
        )
        
        logger.info(f"Напоминание для {name} запланировано на пятницу {hour}:00 ({timezone})")
    except Exception as e:
        logger.error(f"Ошибка планирования напоминания для {tab_number}: {e}")

def send_reminder(context: CallbackContext):
    """Отправка напоминания"""
    job_context = context.job.context
    tab_number = job_context['tab_number']
    name = job_context['name']
    location = job_context['location']
    division = job_context['division']
    equipment = pd.DataFrame.from_records(job_context['equipment'])
    deadline = job_context['deadline']
    
    try:
        # Создаем шаблон таблицы
        template_df = pd.DataFrame(columns=[
            '№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий'
        ])
        
        template_df['№ п/п'] = equipment['№ п/п']
        template_df['Гос. номер'] = equipment['Гос. номер']
        template_df['Инв. №'] = equipment['Инв. №']
        template_df['Счётчик'] = equipment['Счётчик']
        
        # Сохраняем в Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            template_df.to_excel(writer, index=False)
        output.seek(0)
        
        # Отправляем пользователю
        context.bot.send_message(
            chat_id=tab_number,
            text=f"⏰ *Уважаемый {name}, необходимо подать показания счетчиков!*\n\n"
                f"📍 Локация: {location}\n"
                f"🏢 Подразделение: {division}\n"
                f"🕒 Срок подачи: сегодня до {deadline}\n\n"
                "Заполните столбцы 'Показания' и 'Комментарий' и отправьте файл обратно.",
            parse_mode='Markdown'
        )
        
        context.bot.send_document(
            chat_id=tab_number,
            document=InputFile(output, filename=f'Показания_{location}_{division}.xlsx'),
            caption="Шаблон для заполнения показаний счетчиков"
        )
        
        # Сохраняем информацию о пользователе
        context.user_data['waiting_for_meters'] = True
        context.user_data['location'] = location
        context.user_data['division'] = division
        
        logger.info(f"Напоминание отправлено {name} (tab: {tab_number})")
    except Exception as e:
        logger.error(f"Ошибка отправки напоминания {tab_number}: {e}")

def handle_meters_file(update: Update, context: CallbackContext):
    """Обработка полученного файла с показаниями"""
    try:
        if 'waiting_for_meters' not in context.user_data or not context.user_data['waiting_for_meters']:
            update.message.reply_text("Сейчас не время отправки показаний счетчиков.")
            return
        
        user = update.effective_user
        tab_number = user.id
        
        # Проверяем, что отправитель - обычный пользователь
        cursor.execute('SELECT * FROM Users_user_bot WHERE tab_number = ?', (tab_number,))
        if not cursor.fetchone():
            update.message.reply_text("Только пользователи могут отправлять показания счетчиков.")
            return
        
        # Получаем файл
        file = context.bot.get_file(update.message.document.file_id)
        file_name = update.message.document.file_name
        
        # Проверяем расширение файла
        if not file_name.lower().endswith(('.xls', '.xlsx')):
            update.message.reply_text("Пожалуйста, отправьте файл в формате Excel (.xls или .xlsx)")
            return WAITING_FOR_METERS_DATA
        
        # Скачиваем файл
        file_path = f"temp_{tab_number}.xlsx"
        file.download(file_path)
        
        try:
            # Пытаемся прочитать файл
            df = pd.read_excel(file_path)
            
            # Проверяем структуру файла
            required_columns = ['№ п/п', 'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий']
            if not all(col in df.columns for col in required_columns):
                update.message.reply_text("Файл имеет неправильную структуру. Пожалуйста, используйте предоставленный шаблон.")
                return WAITING_FOR_METERS_DATA
            
            # Сохраняем данные
            location = context.user_data['location']
            division = context.user_data['division']
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            save_path = f"meter_readings/{location}_{division}_{tab_number}_{timestamp}.xlsx"
            
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            df.to_excel(save_path, index=False)
            
            # Уведомляем администраторов и руководителей
            notify_admins_and_managers(context, tab_number, user.full_name, location, division, save_path)
            
            update.message.reply_text("Спасибо! Ваши показания сохранены.")
            context.user_data.pop('waiting_for_meters', None)
            
        except Exception as e:
            update.message.reply_text(f"Ошибка при обработке файла: {str(e)}")
            return WAITING_FOR_METERS_DATA
            
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
                
    except Exception as e:
        update.message.reply_text(f"Произошла ошибка: {str(e)}")
        return WAITING_FOR_METERS_DATA

def notify_admins_and_managers(context: CallbackContext, user_tab_number: int, user_name: str, 
                             location: str, division: str, file_path: str):
    """Уведомление администраторов и руководителей о новых показаниях"""
    try:
        # Получаем всех админов и руководителей с такой же локацией и подразделением
        cursor.execute('''
            SELECT tab_number FROM Users_admin_bot 
            WHERE location = ? AND division = ?
            UNION
            SELECT tab_number FROM Users_dir_bot 
            WHERE location = ? AND division = ?
        ''', (location, division, location, division))
        
        recipients = cursor.fetchall()
        
        for (tab_number,) in recipients:
            try:
                # Создаем кнопку для открытия файла
                file_url = f"file://{os.path.abspath(file_path)}"
                
                context.bot.send_message(
                    chat_id=tab_number,
                    text=f"📊 Пользователь <a href='tg://user?id={user_tab_number}'>{user_name}</a> (таб. № {user_tab_number}) "
                         f"отправил <a href='{file_url}'>показания счетчиков</a>.\n\n"
                         f"🔴 Локация: {location}\n"
                         f"🏢 Подразделение: {division}",
                    parse_mode='HTML',
                    disable_web_page_preview=False
                )
                
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления {tab_number}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка уведомления администраторов: {e}")

def setup_meters_handlers(dispatcher):
    """Настройка обработчиков для функционала счетчиков"""
    try:
        # Планируем еженедельные напоминания при старте бота
        dispatcher.job_queue.run_once(
            callback=schedule_weekly_reminders,
            when=0,
            name="init_weekly_schedule"
        )
        
        # Обработчик для получения файлов с показаниями
        dispatcher.add_handler(MessageHandler(
            Filters.document,
            handle_meters_file
        ))
        
        logger.info("Обработчики счетчиков настроены")
    except Exception as e:
        logger.error(f"Ошибка настройки обработчиков: {e}")