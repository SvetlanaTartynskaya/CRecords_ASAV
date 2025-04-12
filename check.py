import pandas as pd
import os
from datetime import datetime, timedelta
import sqlite3
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext, CallbackQueryHandler

logger = logging.getLogger(__name__)

class MeterValidator:
    """Класс для валидации показаний счетчиков"""
    
    def __init__(self, conn=None):
        """Инициализация валидатора"""
        if conn:
            self.conn = conn
        else:
            self.conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Создаем таблицу истории показаний, если не существует
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS meter_readings_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    inventory_number TEXT,
                    meter_type TEXT,
                    reading REAL,
                    reading_date TEXT,
                    location TEXT,
                    division TEXT,
                    user_tab_number INTEGER,
                    comment TEXT
                )
            ''')
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка создания таблицы истории показаний: {e}")
            self.conn.rollback()
        
        # Допустимые комментарии
        self.valid_comments = ["В ремонте", "Не исправен счетчик", "Нет на локации"]
    
    def validate_file(self, file_path, user_info):
        """Валидация файла с показаниями"""
        try:
            # Проверяем существование файла
            if not os.path.exists(file_path):
                return {
                    'is_valid': False,
                    'errors': ["Файл не найден или недоступен"],
                    'warnings': []
                }
                
            # Загружаем файл
            try:
                df = pd.read_excel(file_path)
            except Exception as e:
                return {
                    'is_valid': False,
                    'errors': [f"Ошибка чтения файла Excel: {str(e)}"],
                    'warnings': []
                }
            
            # Итоговый результат валидации
            validation_result = {
                'is_valid': True,
                'errors': [],
                'warnings': []
            }
            
            # Проверяем наличие необходимых столбцов
            required_columns = ['Инв. №', 'Счётчик', 'Показания', 'Комментарий']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                validation_result['is_valid'] = False
                validation_result['errors'].append(f"Отсутствуют обязательные столбцы: {', '.join(missing_columns)}")
                return validation_result
            
            # Проверка пустого файла
            if df.empty:
                validation_result['is_valid'] = False
                validation_result['errors'].append("Файл не содержит данных")
                return validation_result
                
            # Проводим проверки по каждой строке
            for index, row in df.iterrows():
                try:
                    inv_num = str(row['Инв. №']).strip() if not pd.isna(row['Инв. №']) else ""
                    meter_type = str(row['Счётчик']).strip() if not pd.isna(row['Счётчик']) else ""
                    
                    # Проверка наличия значений инв. номера и счетчика
                    if not inv_num:
                        validation_result['is_valid'] = False
                        validation_result['errors'].append(f"Строка {index+1}: Отсутствует инвентарный номер")
                        continue
                    
                    if not meter_type:
                        validation_result['is_valid'] = False
                        validation_result['errors'].append(f"Строка {index+1}: Отсутствует тип счетчика")
                        continue
                    
                    # Проверка наличия значения в ячейке показаний
                    if pd.isna(row['Показания']) or str(row['Показания']).strip() == '':
                        reading = None
                    else:
                        try:
                            reading = float(row['Показания'])
                            if reading < 0:
                                validation_result['is_valid'] = False
                                validation_result['errors'].append(f"Строка {index+1}: Отрицательное показание '{row['Показания']}'")
                                continue
                        except ValueError:
                            validation_result['is_valid'] = False
                            validation_result['errors'].append(f"Строка {index+1}: Показание '{row['Показания']}' не является числом")
                            continue
                    
                    # Проверка комментария
                    comment = str(row['Комментарий']).strip() if not pd.isna(row['Комментарий']) else ""
                    
                    # Проверка 4: Если показания пустые, обязательно заполнить комментарий
                    if reading is None and comment == "":
                        validation_result['is_valid'] = False
                        validation_result['errors'].append(f"Строка {index+1}: Отсутствуют показания, но не указан комментарий")
                    
                    # Проверка 5: Комментарий должен быть из списка допустимых
                    if comment and comment not in self.valid_comments:
                        validation_result['is_valid'] = False
                        validation_result['errors'].append(
                            f"Строка {index+1}: Недопустимый комментарий '{comment}'. "
                            f"Допустимые значения: {', '.join(self.valid_comments)}"
                        )
                    
                    # Получаем последнее показание для этого счетчика
                    last_reading = self._get_last_reading(inv_num, meter_type)
                    
                    # Проверка 1: Текущее показание должно быть >= последнего
                    if reading is not None and last_reading is not None and reading < last_reading['reading']:
                        validation_result['is_valid'] = False
                        validation_result['errors'].append(
                            f"Строка {index+1}: Текущее показание ({reading}) меньше предыдущего ({last_reading['reading']})"
                        )
                    
                    # Проверка 6: Если показание пустое и комментарий "В ремонте", заполняем последним значением
                    if reading is None and comment == "В ремонте" and last_reading is not None:
                        df.at[index, 'Показания'] = last_reading['reading']
                        validation_result['warnings'].append(
                            f"Строка {index+1}: Автоматически заполнено последним значением ({last_reading['reading']})"
                        )
                    
                    # Проверки по типу счетчика
                    if reading is not None and last_reading is not None:
                        # Получаем дни между показаниями
                        days_between = self._get_days_between(last_reading['reading_date'])
                        if days_between > 0:
                            daily_change = (reading - last_reading['reading']) / days_between
                            
                            # Проверка 2: Счетчики PM - не более 24 в сутки
                            if meter_type.startswith('PM') and daily_change > 24:
                                validation_result['is_valid'] = False
                                validation_result['errors'].append(
                                    f"Строка {index+1}: Слишком большое изменение для счетчика PM ({daily_change:.2f} в сутки)"
                                )
                            
                            # Проверка 3: Счетчики KM - не более 500 в сутки
                            if meter_type.startswith('KM') and daily_change > 500:
                                validation_result['is_valid'] = False
                                validation_result['errors'].append(
                                    f"Строка {index+1}: Слишком большое изменение для счетчика KM ({daily_change:.2f} в сутки)"
                                )
                except Exception as e:
                    validation_result['is_valid'] = False
                    validation_result['errors'].append(f"Строка {index+1}: Ошибка при обработке: {str(e)}")
            
            # Проверка 11: Проверка дублей инв. номер + счетчик в текущем файле
            try:
                duplicate_rows = df.duplicated(subset=['Инв. №', 'Счётчик'], keep=False)
                if duplicate_rows.any():
                    validation_result['is_valid'] = False
                    duplicate_indices = [str(i+1) for i in duplicate_rows[duplicate_rows].index]
                    validation_result['errors'].append(
                        f"Обнаружены дубликаты (инв. номер + счетчик) в строках: {', '.join(duplicate_indices)}"
                    )
            except Exception as e:
                validation_result['warnings'].append(f"Не удалось проверить дубликаты: {str(e)}")
            
            # Если валидация пройдена, сохраняем файл с обновлениями
            if validation_result['is_valid'] or validation_result['warnings']:
                try:
                    df.to_excel(file_path, index=False)
                    
                    # Если валидация успешна, сохраняем показания в историю
                    if validation_result['is_valid']:
                        self._save_readings_to_history(df, user_info)
                except Exception as e:
                    validation_result['warnings'].append(f"Ошибка сохранения файла: {str(e)}")
            
            return validation_result
        
        except Exception as e:
            logger.error(f"Ошибка валидации файла: {e}")
            return {
                'is_valid': False,
                'errors': [f"Ошибка обработки файла: {str(e)}"],
                'warnings': []
            }
    
    def _get_last_reading(self, inv_num, meter_type):
        """Получение последнего показания для данного счетчика"""
        try:
            self.cursor.execute('''
                SELECT reading, reading_date
                FROM meter_readings_history
                WHERE inventory_number = ? AND meter_type = ?
                ORDER BY reading_date DESC
                LIMIT 1
            ''', (inv_num, meter_type))
            
            result = self.cursor.fetchone()
            if result:
                return {
                    'reading': result[0],
                    'reading_date': result[1]
                }
            return None
        except Exception as e:
            logger.error(f"Ошибка получения последнего показания: {e}")
            return None
    
    def _get_days_between(self, last_date_str):
        """Вычисление количества дней между датами"""
        try:
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            delta = now - last_date
            return max(delta.days, 1)  # Минимум 1 день, чтобы избежать деления на ноль
        except Exception as e:
            logger.error(f"Ошибка расчета дней между датами: {e}")
            return 1  # По умолчанию возвращаем 1 день
    
    def _save_readings_to_history(self, df, user_info):
        """Сохранение показаний в историю"""
        try:
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Начинаем транзакцию
            with self.conn:
                for _, row in df.iterrows():
                    try:
                        inv_num = str(row['Инв. №']).strip() if not pd.isna(row['Инв. №']) else ""
                        meter_type = str(row['Счётчик']).strip() if not pd.isna(row['Счётчик']) else ""
                        
                        if not inv_num or not meter_type:
                            continue
                        
                        if not pd.isna(row['Показания']) and str(row['Показания']).strip() != '':
                            try:
                                reading = float(row['Показания'])
                                comment = str(row['Комментарий']).strip() if not pd.isna(row['Комментарий']) else ""
                                
                                # Проверка 7: Пропускаем записи с комментариями "Неисправен" или "Убыло"
                                if comment in ["Неисправен", "Убыло"]:
                                    continue
                                
                                self.cursor.execute('''
                                    INSERT INTO meter_readings_history
                                    (inventory_number, meter_type, reading, reading_date, location, division, user_tab_number, comment)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    inv_num,
                                    meter_type,
                                    reading,
                                    current_date,
                                    user_info['location'],
                                    user_info['division'],
                                    user_info['tab_number'],
                                    comment
                                ))
                            except (ValueError, TypeError) as e:
                                logger.error(f"Ошибка при сохранении показания: {e}")
                                continue
                    except Exception as e:
                        logger.error(f"Ошибка обработки строки при сохранении истории: {e}")
                        continue
        except Exception as e:
            logger.error(f"Ошибка сохранения показаний в историю: {e}")
            if not self.conn.__enter__:  # Если не в контексте with
                self.conn.rollback()

    def get_admin_for_division(self, division):
        """Получение ID администратора для данного подразделения"""
        try:
            # Проверяем наличие подразделения
            if not division:
                return []
                
            self.cursor.execute('''
                SELECT tab_number, name
                FROM Users_admin_bot
                WHERE division = ?
            ''', (division,))
            
            admins = self.cursor.fetchall()
            
            # Если нет администраторов для подразделения, вернем всех администраторов
            if not admins:
                self.cursor.execute('''
                    SELECT tab_number, name
                    FROM Users_admin_bot
                ''')
                admins = self.cursor.fetchall()
                
            return admins
        except Exception as e:
            logger.error(f"Ошибка получения администратора для подразделения: {e}")
            return []

class FinalReportGenerator:
    def __init__(self, bot):
        self.bot = bot
        self.conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.approved_admins = set()
        # Создаем таблицу для отслеживания подтверждений администраторов
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS admin_approvals (
            cycle_id TEXT,
            admin_id INTEGER,
            approved BOOLEAN DEFAULT 0,
            approved_time TEXT,
            PRIMARY KEY (cycle_id, admin_id)
        )
        ''')
        self.conn.commit()
        
        # Создаем экземпляр валидатора
        self.validator = MeterValidator(self.conn)
        
    def init_new_report_cycle(self):
        """Инициализация нового цикла отчетности"""
        try:
            # Создаем папку, если не существует
            os.makedirs('meter_readings', exist_ok=True)
            
            # Создаем папку для текущего цикла
            cycle_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.current_cycle_dir = f"meter_readings/cycle_{cycle_id}"
            os.makedirs(self.current_cycle_dir, exist_ok=True)
            
            # Сбрасываем список подтвердивших администраторов
            self.approved_admins = set()
            
            # Сбрасываем все подтверждения в базе данных
            self.cursor.execute('DELETE FROM admin_approvals WHERE cycle_id = ?', (cycle_id,))
            self.conn.commit()
            
            logger.info(f"Инициализирован новый цикл отчетности: {cycle_id}")
            return cycle_id
        except Exception as e:
            logger.error(f"Ошибка инициализации нового цикла: {e}")
            return None

    def add_user_report(self, user_report_path: str, user_info: dict):
        """Добавление отчета пользователя в текущий цикл"""
        try:
            # Копируем файл в папку текущего цикла
            filename = os.path.basename(user_report_path)
            new_path = os.path.join(self.current_cycle_dir, filename)
            
            # Добавляем метаданные пользователя в файл
            df = pd.read_excel(user_report_path)
            df['Отправитель'] = user_info['name']
            df['Подразделение'] = user_info['division']
            df['Локация'] = user_info['location']
            df['Дата'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Сохраняем в папке цикла
            df.to_excel(new_path, index=False)
            logger.info(f"Добавлен отчет пользователя {user_info['name']} в цикл")
            
            # Проверяем на совпадения по инв.номеру и счетчику (проверка 12)
            self._check_for_duplicates_in_cycle(df, user_info)
            
            return new_path
        except Exception as e:
            logger.error(f"Ошибка добавления отчета пользователя: {e}")
            return None
    
    def _check_for_duplicates_in_cycle(self, new_df, user_info):
        """Проверка на дубликаты в текущем цикле (правило 12)"""
        try:
            # Проверяем существование директории
            if not os.path.exists(self.current_cycle_dir):
                logger.error(f"Директория цикла не существует: {self.current_cycle_dir}")
                return
                
            # Получаем все файлы из текущего цикла
            cycle_files = [f for f in os.listdir(self.current_cycle_dir) 
                         if f.endswith('.xlsx') and f != os.path.basename(new_df)]
            
            for file in cycle_files:
                file_path = os.path.join(self.current_cycle_dir, file)
                try:
                    existing_df = pd.read_excel(file_path)
                    
                    # Проверяем на совпадения по инв. номеру и счетчику
                    for _, new_row in new_df.iterrows():
                        if 'Инв. №' not in new_row or 'Счётчик' not in new_row:
                            continue
                        
                        inv_num = str(new_row['Инв. №']).strip() if not pd.isna(new_row['Инв. №']) else ""
                        meter_type = str(new_row['Счётчик']).strip() if not pd.isna(new_row['Счётчик']) else ""
                        
                        if not inv_num or not meter_type:
                            continue
                        
                        # Поиск совпадений в существующем файле
                        for idx, existing_row in existing_df.iterrows():
                            if 'Инв. №' not in existing_row or 'Счётчик' not in existing_row:
                                continue
                                
                            existing_inv = str(existing_row['Инв. №']).strip() if not pd.isna(existing_row['Инв. №']) else ""
                            existing_meter = str(existing_row['Счётчик']).strip() if not pd.isna(existing_row['Счётчик']) else ""
                            
                            if not existing_inv or not existing_meter:
                                continue
                                
                            if inv_num == existing_inv and meter_type == existing_meter:
                                # Если текущее показание >= предыдущего, обновляем
                                if ('Показания' in new_row and 'Показания' in existing_row and 
                                    not pd.isna(new_row['Показания']) and not pd.isna(existing_row['Показания'])):
                                    
                                    try:
                                        new_reading = float(new_row['Показания'])
                                        existing_reading = float(existing_row['Показания'])
                                        
                                        if new_reading >= existing_reading:
                                            # Обновляем значение в существующем файле
                                            existing_df.at[idx, 'Показания'] = new_reading
                                            existing_df.at[idx, 'Отправитель'] = user_info['name']
                                            existing_df.at[idx, 'Дата'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                            existing_df.to_excel(file_path, index=False)
                                            
                                            # Уведомляем администраторов о перезаписи данных
                                            admins = self.validator.get_admin_for_division(user_info['division'])
                                            for admin_id, _ in admins:
                                                try:
                                                    self.bot.send_message(
                                                        chat_id=admin_id,
                                                        text=f"🔄 *Обновление показаний*\n\n"
                                                            f"Пользователь {user_info['name']} обновил показания для:\n"
                                                            f"- Инв.№: {inv_num}\n"
                                                            f"- Счетчик: {meter_type}\n"
                                                            f"- Старое значение: {existing_reading}\n"
                                                            f"- Новое значение: {new_reading}",
                                                        parse_mode='Markdown'
                                                    )
                                                except Exception as e:
                                                    logger.error(f"Ошибка уведомления администратора {admin_id}: {e}")
                                    except (ValueError, TypeError) as e:
                                        logger.error(f"Ошибка обработки числовых значений при обновлении: {e}")
                except Exception as e:
                    logger.error(f"Ошибка чтения файла {file}: {e}")
                    continue
        except Exception as e:
            logger.error(f"Ошибка проверки дубликатов в цикле: {e}")

    def admin_approval(self, admin_id: int, context: CallbackContext) -> bool:
        """Обработка подтверждения администратора"""
        try:
            # Проверяем, что это действительно администратор
            self.cursor.execute('SELECT division FROM Users_admin_bot WHERE tab_number = ?', (admin_id,))
            admin_data = self.cursor.fetchone()
            
            if not admin_data:
                logger.error(f"Попытка подтверждения от неадминистратора: {admin_id}")
                return False
            
            division = admin_data[0]
            
            # Получаем текущий цикл
            cycles = sorted([d for d in os.listdir("meter_readings") if d.startswith("cycle_")], reverse=True)
            if not cycles:
                logger.error("Нет активных циклов отчётности")
                return False
                
            current_cycle = cycles[0]
            cycle_id = current_cycle.replace("cycle_", "")
            
            # Записываем подтверждение администратора
            self.cursor.execute('''
                INSERT OR REPLACE INTO admin_approvals (cycle_id, admin_id, approved, approved_time)
                VALUES (?, ?, 1, ?)
            ''', (cycle_id, admin_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            self.conn.commit()
            
            self.approved_admins.add(admin_id)
            
            # Проверяем, все ли администраторы подтвердили
            if self._check_all_admins_approved(cycle_id):
                self._generate_final_report(context, cycle_id)
                return True
            
            return False
        except Exception as e:
            logger.error(f"Ошибка обработки подтверждения администратора: {e}")
            return False

    def _check_all_admins_approved(self, cycle_id: str) -> bool:
        """Проверяем, все ли администраторы подтвердили"""
        try:
            # Получаем список всех администраторов
            self.cursor.execute('SELECT tab_number FROM Users_admin_bot')
            all_admins = {row[0] for row in self.cursor.fetchall()}
            
            if not all_admins:
                logger.error("Нет администраторов в системе")
                return False
                
            # Получаем список всех подтвердивших администраторов для данного цикла
            self.cursor.execute('''
                SELECT admin_id FROM admin_approvals 
                WHERE cycle_id = ? AND approved = 1
            ''', (cycle_id,))
            approved_admins = {row[0] for row in self.cursor.fetchall()}
            
            # Логируем информацию о подтверждениях
            logger.info(f"Всего администраторов: {len(all_admins)}, подтвердили: {len(approved_admins)}")
            
            # Проверяем, что все подтвердили
            return approved_admins.issuperset(all_admins)
        except Exception as e:
            logger.error(f"Ошибка проверки подтверждений администраторов: {e}")
            return False

    def _generate_final_report(self, context: CallbackContext, cycle_id: str):
        """Генерация итогового отчета"""
        try:
            # Проверяем существование директории цикла
            cycle_dir = os.path.join("meter_readings", f"cycle_{cycle_id}")
            if not os.path.exists(cycle_dir):
                logger.error(f"Директория цикла не существует: {cycle_dir}")
                
                # Уведомляем администраторов
                self.cursor.execute('SELECT tab_number FROM Users_admin_bot')
                admins = self.cursor.fetchall()
                for (admin_id,) in admins:
                    try:
                        context.bot.send_message(
                            chat_id=admin_id,
                            text="❌ *Ошибка генерации отчета!*\n\n"
                                f"Директория цикла не найдена: cycle_{cycle_id}",
                            parse_mode='Markdown'
                        )
                    except Exception as e:
                        logger.error(f"Ошибка уведомления администратора {admin_id}: {e}")
                return
                
            # Собираем все файлы из папки цикла
            all_files = []
            
            xlsx_files = [f for f in os.listdir(cycle_dir) if f.endswith('.xlsx') and f != "FINAL_REPORT.xlsx"]
            
            if not xlsx_files:
                logger.error("Нет файлов для формирования итогового отчета")
                # Уведомляем администраторов
                self.cursor.execute('SELECT tab_number FROM Users_admin_bot')
                admins = self.cursor.fetchall()
                for (admin_id,) in admins:
                    try:
                        context.bot.send_message(
                            chat_id=admin_id,
                            text="⚠️ *Внимание*\n\n"
                                f"В цикле отчетности нет файлов для формирования итогового отчета.",
                            parse_mode='Markdown'
                        )
                    except Exception as e:
                        logger.error(f"Ошибка уведомления администратора {admin_id}: {e}")
                return
            
            # Загружаем данные файлов
            for file in xlsx_files:
                try:
                    file_path = os.path.join(cycle_dir, file)
                    df = pd.read_excel(file_path)
                    all_files.append(df)
                except Exception as e:
                    logger.error(f"Ошибка чтения файла {file}: {e}")
                    continue
            
            if not all_files:
                logger.error("Не удалось прочитать ни один файл для формирования итогового отчета")
                return
            
            try:
                # Объединяем все отчеты
                final_df = pd.concat(all_files, ignore_index=True)
                
                # Проверка 7: Исключаем записи с комментариями "Неисправен" или "Убыло"
                if 'Комментарий' in final_df.columns:
                    final_df = final_df[~final_df['Комментарий'].isin(['Неисправен', 'Убыло'])]
                
                # Приводим к нужному формату
                columns_to_include = [
                    'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий',
                    'Отправитель', 'Дата', 'Подразделение', 'Локация'
                ]
                
                # Выбираем только нужные столбцы, игнорируя отсутствующие
                available_columns = [col for col in columns_to_include if col in final_df.columns]
                
                if not available_columns:
                    logger.error("В объединенном DataFrame нет необходимых столбцов")
                    return
                    
                final_df = final_df[available_columns]
                
                # Сохраняем итоговый отчет
                final_path = os.path.join(cycle_dir, "FINAL_REPORT.xlsx")
                final_df.to_excel(final_path, index=False)
                
                # Отправляем уведомление всем администраторам
                self._notify_admins_about_final_report(context, final_path)
                
                logger.info("Итоговый отчет успешно сформирован")
            except Exception as e:
                logger.error(f"Ошибка при формировании итогового отчета: {e}")
                
                # Уведомляем администраторов об ошибке
                self.cursor.execute('SELECT tab_number FROM Users_admin_bot')
                admins = self.cursor.fetchall()
                for (admin_id,) in admins:
                    try:
                        context.bot.send_message(
                            chat_id=admin_id,
                            text="❌ *Ошибка генерации отчета!*\n\n"
                                f"Произошла ошибка при формировании итогового отчета: {str(e)}",
                            parse_mode='Markdown'
                        )
                    except Exception as e:
                        logger.error(f"Ошибка уведомления администратора {admin_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка генерации итогового отчета: {e}")

    def _notify_admins_about_final_report(self, context: CallbackContext, report_path: str):
        """Уведомление администраторов о готовности итогового отчета"""
        try:
            # Получаем список всех администраторов
            self.cursor.execute('SELECT tab_number FROM Users_admin_bot')
            admins = self.cursor.fetchall()
            
            for (admin_id,) in admins:
                try:
                    context.bot.send_message(
                        chat_id=admin_id,
                        text="✅ *Итоговый отчет по показаниям сформирован!*\n\n"
                            "Все пользователи отправили показания, и все администраторы подтвердили проверку.",
                        parse_mode='Markdown'
                    )
                    
                    # Отправляем сам файл отчета
                    with open(report_path, 'rb') as f:
                        context.bot.send_document(
                            chat_id=admin_id,
                            document=f,
                            caption="Итоговый отчет по показаниям счетчиков",
                            filename="FINAL_METERS_REPORT.xlsx"
                        )
                except Exception as e:
                    logger.error(f"Ошибка отправки отчета администратору {admin_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка уведомления администраторов: {e}")

    def send_verification_request(self, context: CallbackContext, file_path: str):
        """Отправка запроса на подтверждение администраторам"""
        try:
            # Получаем список всех администраторов
            self.cursor.execute('SELECT tab_number, name FROM Users_admin_bot')
            admins = self.cursor.fetchall()
            
            # Получаем текущий цикл
            cycle_id = os.path.basename(os.path.dirname(file_path)).replace("cycle_", "")
            
            for admin_id, admin_name in admins:
                try:
                    # Создаем клавиатуру с кнопкой подтверждения
                    keyboard = [
                        [InlineKeyboardButton("✅ Всё верно", callback_data=f"approve_{cycle_id}")]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    # Отправляем сообщение с запросом подтверждения
                    context.bot.send_message(
                        chat_id=admin_id,
                        text=f"*Уважаемый {admin_name}!*\n\n"
                            f"Пожалуйста, проверьте отчёты по показаниям счетчиков. "
                            f"После проверки нажмите кнопку 'Всё верно'.\n\n"
                            f"Когда все администраторы подтвердят проверку, будет сформирован итоговый отчёт.",
                        parse_mode='Markdown',
                        reply_markup=reply_markup
                    )
                    
                    # Отправляем файл отчета для проверки
                    with open(file_path, 'rb') as f:
                        context.bot.send_document(
                            chat_id=admin_id,
                            document=f,
                            caption="Файл с показаниями для проверки"
                        )
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки запроса администратору {admin_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Ошибка отправки запросов администраторам: {e}")

    def get_current_final_report(self):
        """Получение текущего итогового отчета (если есть)"""
        try:
            # Ищем последний цикл с отчетом
            cycles = sorted([d for d in os.listdir("meter_readings") if d.startswith("cycle_")], reverse=True)
            
            if not cycles:
                return None
                
            latest_cycle = cycles[0]
            report_path = os.path.join("meter_readings", latest_cycle, "FINAL_REPORT.xlsx")
            
            if os.path.exists(report_path):
                return pd.read_excel(report_path)
            return None
        except Exception as e:
            logger.error(f"Ошибка получения текущего итогового отчета: {e}")
            return None

def handle_approval_callback(update: Update, context: CallbackContext):
    """Обработчик нажатия кнопки 'Всё верно'"""
    query = update.callback_query
    data = query.data
    
    if data.startswith("approve_"):
        try:
            cycle_id = data.split("_")[1]
            admin_id = update.effective_user.id
            
            # Получаем генератор отчетов из контекста
            report_generator = context.bot_data.get('report_generator')
            if not report_generator:
                report_generator = FinalReportGenerator(context.bot)
                context.bot_data['report_generator'] = report_generator
            
            # Регистрируем подтверждение
            approved = report_generator.admin_approval(admin_id, context)
            
            # Отвечаем на запрос
            if approved:
                query.answer("Спасибо! Все администраторы подтвердили проверку. Формируется итоговый отчёт.")
                query.edit_message_text(
                    text="✅ *Отчёт проверен.* Все администраторы подтвердили проверку. Итоговый отчёт сформирован.",
                    parse_mode='Markdown'
                )
            else:
                query.answer("Спасибо! Ваше подтверждение учтено.")
                query.edit_message_text(
                    text="✅ *Отчёт проверен.* Ваше подтверждение учтено. Ожидаем подтверждения от других администраторов.",
                    parse_mode='Markdown'
                )
        except Exception as e:
            logger.error(f"Ошибка обработки подтверждения: {e}")
            query.answer("Произошла ошибка при обработке подтверждения.")

def setup_approval_handler(dispatcher):
    """Настройка обработчика подтверждений"""
    dispatcher.add_handler(CallbackQueryHandler(handle_approval_callback, pattern=r'^approve_'))