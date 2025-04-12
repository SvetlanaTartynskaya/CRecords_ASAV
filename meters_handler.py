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
    # Центральный федеральный округ (UTC+3)
    'Белго': 'Europe/Moscow',
    'Брянс': 'Europe/Moscow',
    'Влади': 'Europe/Moscow',
    'Ворон': 'Europe/Moscow',
    'Ивано': 'Europe/Moscow',
    'Калуж': 'Europe/Moscow',
    'Костр': 'Europe/Moscow',
    'Курск': 'Europe/Moscow',
    'Липец': 'Europe/Moscow',
    'Москв': 'Europe/Moscow',
    'Орлов': 'Europe/Moscow',
    'Рязан': 'Europe/Moscow',
    'Смоле': 'Europe/Moscow',
    'Тамбо': 'Europe/Moscow',
    'Тверс': 'Europe/Moscow',
    'Тульс': 'Europe/Moscow',
    'Яросл': 'Europe/Moscow',
    
    # Северо-Западный федеральный округ
    'Архан': 'Europe/Moscow',
    'Волог': 'Europe/Moscow',
    'Калин': 'Europe/Kaliningrad',  # UTC+2
    'Карел': 'Europe/Moscow',
    'Коми': 'Europe/Moscow',
    'Ленин': 'Europe/Moscow',
    'Мурма': 'Europe/Moscow',
    'Ненец': 'Europe/Moscow',
    'Новго': 'Europe/Moscow',
    'Псков': 'Europe/Moscow',
    'Санкт': 'Europe/Moscow',
    
    # Южный и Северо-Кавказский федеральные округа
    'Адыге': 'Europe/Moscow',
    'Астра': 'Europe/Samara',  # UTC+4
    'Волго': 'Europe/Moscow',
    'Дагес': 'Europe/Moscow',
    'Ингуш': 'Europe/Moscow',
    'Кабар': 'Europe/Moscow',
    'Калмы': 'Europe/Moscow',
    'Карач': 'Europe/Moscow',
    'Красн': 'Europe/Moscow',  # Краснодарский край
    'Крым': 'Europe/Moscow',
    'Росто': 'Europe/Moscow',
    'Север': 'Europe/Moscow',
    'Ставр': 'Europe/Moscow',
    'Чечня': 'Europe/Moscow',
    
    # Приволжский федеральный округ
    'Башко': 'Asia/Yekaterinburg',  # UTC+5
    'Киров': 'Europe/Moscow',
    'Марий': 'Europe/Moscow',
    'Мордо': 'Europe/Moscow',
    'Нижег': 'Europe/Moscow',
    'Оренб': 'Asia/Yekaterinburg',  # UTC+5
    'Пензе': 'Europe/Moscow',
    'Пермс': 'Asia/Yekaterinburg',  # UTC+5
    'Самар': 'Europe/Samara',  # UTC+4
    'Сарат': 'Europe/Samara',  # UTC+4
    'Татар': 'Europe/Moscow',
    'Удмур': 'Europe/Samara',  # UTC+4
    'Ульян': 'Europe/Samara',  # UTC+4
    'Чуваш': 'Europe/Moscow',
    
    # Уральский федеральный округ
    'Курга': 'Asia/Yekaterinburg',  # UTC+5
    'Сверд': 'Asia/Yekaterinburg',  # UTC+5
    'Тюмен': 'Asia/Yekaterinburg',  # UTC+5
    'Ханты': 'Asia/Yekaterinburg',  # UTC+5
    'Челяб': 'Asia/Yekaterinburg',  # UTC+5
    'Ямало': 'Asia/Yekaterinburg',  # UTC+5
    
    # Сибирский федеральный округ
    'Алтай': 'Asia/Krasnoyarsk',  # UTC+7
    'Бурят': 'Asia/Irkutsk',  # UTC+8
    'Забай': 'Asia/Yakutsk',  # UTC+9
    'Иркут': 'Asia/Irkutsk',  # UTC+8
    'Кемер': 'Asia/Krasnoyarsk',  # UTC+7
    'Красн': 'Asia/Krasnoyarsk',  # UTC+7 - Красноярский край
    'Новос': 'Asia/Krasnoyarsk',  # UTC+7
    'Омска': 'Asia/Omsk',  # UTC+6
    'Томск': 'Asia/Krasnoyarsk',  # UTC+7
    'Тыва': 'Asia/Krasnoyarsk',  # UTC+7
    'Хакас': 'Asia/Krasnoyarsk',  # UTC+7
    
    # Дальневосточный федеральный округ
    'Амурс': 'Asia/Yakutsk',  # UTC+9
    'Еврей': 'Asia/Vladivostok',  # UTC+10
    'Камча': 'Asia/Kamchatka',  # UTC+12
    'Магад': 'Asia/Magadan',  # UTC+11
    'Примо': 'Asia/Vladivostok',  # UTC+10
    'Саха': 'Asia/Yakutsk',  # UTC+9
    'Сахал': 'Asia/Magadan',  # UTC+11
    'Хабар': 'Asia/Vladivostok',  # UTC+10
    'Чукот': 'Asia/Kamchatka'  # UTC+12
}

def get_timezone_for_location(location: str) -> str:
    """Определяем часовой пояс по названию локации"""
    # Проверяем первые 5 букв локации
    location_prefix = location.strip()[:5].capitalize()
    
    if location_prefix in RUSSIAN_TIMEZONES:
        return RUSSIAN_TIMEZONES[location_prefix]
    
    # Если не нашли по первым 5 буквам, пробуем найти по содержанию
    location_lower = location.lower()
    
    # Поиск по наиболее характерным частям названий
    if 'москв' in location_lower:
        return 'Europe/Moscow'
    elif 'калин' in location_lower:
        return 'Europe/Kaliningrad'
    elif 'самар' in location_lower or 'саратов' in location_lower:
        return 'Europe/Samara'
    elif 'екатер' in location_lower or 'свердл' in location_lower:
        return 'Asia/Yekaterinburg'
    elif 'омск' in location_lower:
        return 'Asia/Omsk'
    elif 'красноярск' in location_lower:
        return 'Asia/Krasnoyarsk'
    elif 'краснодар' in location_lower:
        return 'Europe/Moscow'
    elif 'иркут' in location_lower or 'бурят' in location_lower:
        return 'Asia/Irkutsk'
    elif 'якут' in location_lower or 'саха' in location_lower:
        return 'Asia/Yakutsk'
    elif 'владив' in location_lower or 'примор' in location_lower:
        return 'Asia/Vladivostok'
    elif 'магад' in location_lower or 'сахал' in location_lower:
        return 'Asia/Magadan'
    elif 'камчат' in location_lower or 'чукот' in location_lower:
        return 'Asia/Kamchatka'
    
    # По умолчанию возвращаем московское время
    return 'Europe/Moscow'

def get_local_datetime(location: str) -> datetime:
    """Получает текущее время в указанной локации"""
    timezone_str = get_timezone_for_location(location)
    timezone = pytz.timezone(timezone_str)
    return datetime.now(timezone)

def format_datetime_for_timezone(dt: datetime, location: str) -> str:
    """Форматирует дату/время с учетом часового пояса локации"""
    timezone_str = get_timezone_for_location(location)
    timezone = pytz.timezone(timezone_str)
    local_dt = dt.astimezone(timezone)
    return local_dt.strftime('%Y-%m-%d %H:%M:%S (%Z)')

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
                    
                    # Планируем отправку напоминания на 10:00 по местному времени пользователя
                    schedule_reminder(
                        context=context,
                        tab_number=tab_number,
                        name=name,
                        location=location,
                        division=division,
                        equipment=equipment,
                        hour=10,  # Фиксированное время по местному времени 
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
        # Получаем местное время
        local_tz = pytz.timezone(get_timezone_for_location(location))
        current_local_time = datetime.now(local_tz)
        formatted_time = current_local_time.strftime('%Y-%m-%d %H:%M:%S (%Z)')
        
        # Получаем московское время для дедлайна
        moscow_tz = pytz.timezone('Europe/Moscow')
        deadline_time = time(hour=14, minute=0, tzinfo=moscow_tz)
        deadline_datetime = datetime.combine(datetime.now(moscow_tz).date(), deadline_time)
        
        # Конвертируем дедлайн в местное время
        local_deadline = deadline_datetime.astimezone(local_tz)
        local_deadline_str = local_deadline.strftime('%H:%M (%Z)')
        
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
                f"🕒 Срок подачи: сегодня до {local_deadline_str}\n"
                f"🕒 Текущее время: {formatted_time}\n\n"
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
    # При получении файла с показаниями, сохраняем с учетом часового пояса
    try:
        if not update.message.document:
            update.message.reply_text("Пожалуйста, отправьте заполненный файл Excel.")
            return
            
        file = update.message.document
        file_id = file.file_id
        new_file = context.bot.get_file(file_id)
        
        # Создаем папку, если не существует
        os.makedirs('meter_readings', exist_ok=True)
        
        # Получаем данные пользователя
        tab_number = context.user_data.get('tab_number')
        if not tab_number:
            update.message.reply_text("Ошибка: не удалось определить ваш табельный номер. Пожалуйста, запустите /start.")
            return
            
        cursor.execute('''
            SELECT name, location, division FROM Users_user_bot WHERE tab_number = ?
        ''', (tab_number,))
        user_data = cursor.fetchone()
        
        if not user_data:
            update.message.reply_text("Ошибка: пользователь не найден в базе данных.")
            return
            
        name, location, division = user_data
        
        # Получаем текущее время в часовом поясе пользователя
        local_time = get_local_datetime(location)
        timestamp = local_time.strftime('%Y%m%d_%H%M%S')
        
        # Формируем имя файла с учетом часового пояса
        file_path = f'meter_readings/meters_{location}_{division}_{timestamp}.xlsx'
        new_file.download(file_path)
        
        # Проверяем, что файл Excel
        if not file.file_name.lower().endswith(('.xlsx', '.xls')):
            update.message.reply_text("Пожалуйста, отправьте файл в формате Excel (.xlsx, .xls)")
            if os.path.exists(file_path):
                os.remove(file_path)
            return
            
        try:
            # Открываем файл и добавляем метаданные
            df = pd.read_excel(file_path)
            
            # Формируем информацию для метаданных
            user_info = {
                'name': name,
                'location': location,
                'division': division,
                'tab_number': tab_number,
                'timestamp': format_datetime_for_timezone(local_time, location)
            }
            
            for key, value in user_info.items():
                if key not in df.columns:
                    df[key] = value
            
            # Сохраняем обновленный файл с метаданными
            df.to_excel(file_path, index=False)
        except Exception as e:
            update.message.reply_text(f"Ошибка при чтении файла Excel: {str(e)}")
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        # Проверяем файл с показаниями через валидатор
        from check import MeterValidator
        validator = MeterValidator()
        validation_result = validator.validate_file(file_path, user_info)
        
        if validation_result['is_valid']:
            update.message.reply_text("✅ Спасибо! Ваши показания счетчиков приняты и прошли проверку.")
            
            # Если есть предупреждения, сообщаем о них
            if validation_result['warnings']:
                warnings_text = "\n".join(validation_result['warnings'])
                update.message.reply_text(f"⚠️ Предупреждения при проверке:\n\n{warnings_text}")
            
            # Уведомляем администраторов и руководителей
            notify_admins_and_managers(context, tab_number, name, location, division, file_path)
        else:
            # Формируем сообщение об ошибках
            errors_text = "\n".join(validation_result['errors'])
            update.message.reply_text(
                f"❌ При проверке показаний обнаружены следующие ошибки:\n\n{errors_text}\n\n"
                "Пожалуйста, исправьте ошибки и отправьте файл повторно."
            )
            
            # Уведомляем администратора о проблемах
            notify_admin_about_errors(context, tab_number, name, location, division, file_path, validation_result['errors'])
            
    except Exception as e:
        logger.error(f"Ошибка обработки файла показаний: {e}")
        update.message.reply_text(f"❌ Произошла ошибка при обработке файла: {str(e)}")

def notify_admins_and_managers(context: CallbackContext, user_tab_number: int, user_name: str, 
                             location: str, division: str, file_path: str):
    """Уведомление администраторов и руководителей о новых показаниях"""
    try:
        # Убеждаемся, что папка meter_readings существует
        os.makedirs('meter_readings', exist_ok=True)
        
        # Загружаем данные отчета
        report_df = pd.read_excel(file_path)
        
        # Получаем список всех администраторов
        cursor.execute('SELECT tab_number, name FROM Users_admin_bot')
        admins = cursor.fetchall()
        
        # Получаем список всех руководителей
        cursor.execute('SELECT tab_number, name FROM Users_dir_bot')
        managers = cursor.fetchall()
        
        # Получаем текущее время в часовом поясе локации
        local_time = get_local_datetime(location)
        formatted_time = format_datetime_for_timezone(local_time, location)
        
        # Сообщение
        message = f"📊 *Получены новые показания счетчиков*\n\n" \
                  f"👤 От: {user_name}\n" \
                  f"📍 Локация: {location}\n" \
                  f"🏢 Подразделение: {division}\n" \
                  f"⏰ Время: {formatted_time}"
                  
        # Для администраторов, получаем генератор отчетов из контекста
        report_generator = context.bot_data.get('report_generator')
        if not report_generator:
            from check import FinalReportGenerator
            report_generator = FinalReportGenerator(context.bot)
            context.bot_data['report_generator'] = report_generator
            
        # Инициализируем новый цикл, если еще не инициализирован
        cycle_id = report_generator.init_new_report_cycle()
        
        # Проверяем, что цикл создан успешно
        if not cycle_id:
            logger.error("Не удалось инициализировать цикл отчётности")
            Update.message.reply_text("❌ Ошибка при создании цикла отчётности. Пожалуйста, свяжитесь с администратором.")
            return
            
        # Добавляем отчет пользователя
        user_info = {
            'name': user_name, 
            'location': location, 
            'division': division, 
            'tab_number': user_tab_number
        }
        report_path = report_generator.add_user_report(file_path, user_info)
        
        # Проверяем, что отчет добавлен успешно
        if not report_path:
            logger.error("Не удалось добавить отчет пользователя в цикл")
            return
            
        # Отправляем запрос на подтверждение администраторам
        report_generator.send_verification_request(context, report_path)
        
        # Уведомляем руководителей (без кнопки подтверждения)
        for manager_id, manager_name in managers:
            try:
                # Отправляем уведомление
                context.bot.send_message(
                    chat_id=manager_id,
                    text=f"{message}\n\nОтчёт прикреплен ниже.",
                    parse_mode='Markdown'
                )
                
                # Проверяем существование файла перед отправкой
                if os.path.exists(file_path):
                    # Отправляем файл
                    with open(file_path, 'rb') as f:
                        context.bot.send_document(
                            chat_id=manager_id,
                            document=f,
                            caption=f"Показания счетчиков от {user_name}"
                        )
                else:
                    logger.error(f"Файл не найден при отправке руководителю: {file_path}")
            except Exception as e:
                logger.error(f"Ошибка уведомления руководителя {manager_id}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка уведомления о новых показаниях: {e}")

def notify_admin_about_errors(context: CallbackContext, user_tab_number: int, user_name: str,
                             location: str, division: str, file_path: str, errors: list):
    """Уведомление администратора о проблемах с файлом показаний"""
    try:
        # Убеждаемся, что папка meter_readings существует
        os.makedirs('meter_readings', exist_ok=True)
        
        # Получаем администраторов данного подразделения
        from check import MeterValidator
        validator = MeterValidator()
        admins = validator.get_admin_for_division(division)
        
        if not admins:
            logger.error(f"Не найдены администраторы для подразделения {division}")
            return
            
        # Формируем текст сообщения
        errors_text = "\n".join(errors)
        local_time = get_local_datetime(location)
        formatted_time = format_datetime_for_timezone(local_time, location)
        
        message = f"⚠️ *Ошибки в показаниях счетчиков*\n\n" \
                  f"👤 От: {user_name}\n" \
                  f"📍 Локация: {location}\n" \
                  f"🏢 Подразделение: {division}\n" \
                  f"⏰ Время: {formatted_time}\n\n" \
                  f"Обнаружены следующие ошибки:\n{errors_text}"
        
        # Отправляем сообщение всем администраторам подразделения
        for admin_id, admin_name in admins:
            try:
                context.bot.send_message(
                    chat_id=admin_id,
                    text=message,
                    parse_mode='Markdown'
                )
                
                # Проверяем существование файла перед отправкой
                if os.path.exists(file_path):
                    # Отправляем файл
                    with open(file_path, 'rb') as f:
                        context.bot.send_document(
                            chat_id=admin_id,
                            document=f,
                            caption=f"Показания счетчиков с ошибками от {user_name}"
                        )
                else:
                    logger.error(f"Файл не найден при отправке администратору: {file_path}")
                    context.bot.send_message(
                        chat_id=admin_id,
                        text=f"⚠️ Файл показаний не найден или был удалён.",
                        parse_mode='Markdown'
                    )
            except Exception as e:
                logger.error(f"Ошибка уведомления администратора {admin_id}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка уведомления администраторов о проблемах: {e}")

def setup_meters_handlers(dispatcher):
    """Настройка обработчиков для работы с показаниями счетчиков"""
    try:
        # Планируем еженедельные напоминания при старте бота
        dispatcher.job_queue.run_once(
            callback=schedule_weekly_reminders,
            when=0,
            name="init_weekly_schedule"
        )
        
        # Регистрация обработчика файлов с показаниями
        dispatcher.add_handler(
            MessageHandler(
                Filters.document.file_extension(['xls', 'xlsx']),
                handle_meters_file
            )
        )
        
        logger.info("Обработчики показаний счетчиков зарегистрированы")
    except Exception as e:
        logger.error(f"Ошибка настройки обработчиков показаний: {e}")