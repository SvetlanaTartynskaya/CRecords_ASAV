import pandas as pd
import os
from datetime import datetime
import sqlite3
import logging
from telegram import Update
from telegram.ext import CallbackContext

logger = logging.getLogger(__name__)

class FinalReportGenerator:
    def __init__(self, bot):
        self.bot = bot
        self.conn = sqlite3.connect('Users_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.approved_admins = set()
        
    def init_new_report_cycle(self):
        """Инициализация нового цикла отчетности"""
        try:
            # Создаем папку для текущего цикла
            cycle_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.current_cycle_dir = f"meter_readings/cycle_{cycle_id}"
            os.makedirs(self.current_cycle_dir, exist_ok=True)
            
            # Сбрасываем список подтвердивших администраторов
            self.approved_admins = set()
            logger.info(f"Инициализирован новый цикл отчетности: {cycle_id}")
        except Exception as e:
            logger.error(f"Ошибка инициализации нового цикла: {e}")

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
        except Exception as e:
            logger.error(f"Ошибка добавления отчета пользователя: {e}")

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
            self.approved_admins.add(admin_id)
            
            # Проверяем, все ли администраторы подтвердили
            if self._check_all_admins_approved():
                self._generate_final_report(context)
                return True
            
            return False
        except Exception as e:
            logger.error(f"Ошибка обработки подтверждения администратора: {e}")
            return False

    def _check_all_admins_approved(self) -> bool:
        """Проверяем, все ли администраторы подтвердили"""
        try:
            # Получаем список всех администраторов
            self.cursor.execute('SELECT tab_number FROM Users_admin_bot')
            all_admins = {row[0] for row in self.cursor.fetchall()}
            
            # Проверяем, что все подтвердили
            return self.approved_admins.issuperset(all_admins)
        except Exception as e:
            logger.error(f"Ошибка проверки подтверждений администраторов: {e}")
            return False

    def _generate_final_report(self, context: CallbackContext):
        """Генерация итогового отчета"""
        try:
            # Собираем все файлы из папки цикла
            all_files = []
            for file in os.listdir(self.current_cycle_dir):
                if file.endswith('.xlsx'):
                    file_path = os.path.join(self.current_cycle_dir, file)
                    df = pd.read_excel(file_path)
                    all_files.append(df)
            
            if not all_files:
                logger.error("Нет файлов для формирования итогового отчета")
                return
            
            # Объединяем все отчеты
            final_df = pd.concat(all_files, ignore_index=True)
            
            # Приводим к нужному формату
            final_df = final_df[[
                'Гос. номер', 'Инв. №', 'Счётчик', 'Показания', 'Комментарий',
                'Наименование', 'Дата', 'Подразделение', 'Локация', 'Отправитель'
            ]]
            
            # Сохраняем итоговый отчет
            final_path = os.path.join(self.current_cycle_dir, "FINAL_REPORT.xlsx")
            final_df.to_excel(final_path, index=False)
            
            # Отправляем уведомление всем администраторам
            self._notify_admins_about_final_report(context, final_path)
            
            logger.info("Итоговый отчет успешно сформирован")
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