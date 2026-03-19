import aiomysql
import asyncio
import logging
import warnings
from typing import Optional, Dict, Any, List
from config import (
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, 
    DB_POOL_MINSIZE, DB_POOL_MAXSIZE, DB_POOL_RECYCLE, 
    DB_POOL_TIMEOUT, DB_POOL_PRE_PING, DB_CHARSET, 
    DB_AUTOCOMMIT, DB_CONNECT_TIMEOUT, DB_READ_TIMEOUT, DB_WRITE_TIMEOUT
)
import time
from functools import lru_cache

# Подавляем предупреждения MySQL
warnings.filterwarnings('ignore', category=Warning, module='aiomysql')

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.host = DB_HOST
        self.port = DB_PORT
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.db_name = DB_NAME
        self.pool = None

    async def create_pool(self):
        """Создает оптимизированный пул соединений с базой данных"""
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db_name,
            charset=DB_CHARSET,
            autocommit=DB_AUTOCOMMIT,
            minsize=DB_POOL_MINSIZE,
            maxsize=DB_POOL_MAXSIZE,
            pool_recycle=DB_POOL_RECYCLE,
            connect_timeout=DB_CONNECT_TIMEOUT,
            use_unicode=True,
            sql_mode='TRADITIONAL',
            init_command="SET SESSION sql_mode='STRICT_TRANS_TABLES'"
        )

    async def close_pool(self):
        """Закрывает пул соединений"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def init_database(self):
        """Инициализирует базу данных с оптимизированной структурой"""
        # Создаем базу данных если не существует
        connection = await aiomysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            charset=DB_CHARSET,
            connect_timeout=DB_CONNECT_TIMEOUT
        )
        
        async with connection.cursor() as cursor:
            await cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            await connection.commit()
        
        await connection.ensure_closed()

        # Подключаемся к созданной базе данных
        connection = await aiomysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db_name,
            charset=DB_CHARSET,
            connect_timeout=DB_CONNECT_TIMEOUT
        )

        async with connection.cursor() as cursor:
            # Создаем оптимизированную таблицу пользователей
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED UNIQUE NOT NULL,
                    username VARCHAR(255) NULL,
                    fullname VARCHAR(255) NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    
                    INDEX idx_user_id (user_id),
                    INDEX idx_username (username),
                    INDEX idx_created_at (created_at),
                    INDEX idx_updated_at (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Создаем таблицу тарифов
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS tariffs (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type ENUM('per_minute', 'hold', 'no_hold') NOT NULL,
                    country ENUM('RU', 'KZ') NOT NULL,
                    prices JSON NOT NULL,
                    payout_amount DECIMAL(10, 2) DEFAULT 0.00,
                    max_duration INT UNSIGNED NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    
                    INDEX idx_type (type),
                    INDEX idx_country (country),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Создаем универсальную таблицу номеров с расширенной логикой
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS phone_numbers (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    phone_number VARCHAR(20) NOT NULL,
                    country ENUM('RU', 'KZ') NOT NULL,
                    
                    -- Тарифная информация
                    tariff_name VARCHAR(255) NOT NULL,
                    tariff_type ENUM('per_minute', 'hold', 'no_hold') NOT NULL,
                    tariff_prices JSON NOT NULL,
                    
                    -- Статус и позиция в очереди
                    status ENUM('waiting', 'active', 'completed', 'cancelled', 'failed', 'paused') DEFAULT 'waiting',
                    queue_position INT UNSIGNED NOT NULL DEFAULT 0,
                    priority INT DEFAULT 0,
                    
                    -- Поля для работы операторов
                    operator_chat_id BIGINT NULL,
                    operator_topic_id INT UNSIGNED NULL,
                    operator_message_id INT UNSIGNED NULL,
                    operator_status ENUM('requested_code', 'code_received', 'code_verified', 'no_code', 'skipped', 'completed_success', 'completed_error') NULL,
                    code_requested_at TIMESTAMP NULL,
                    code_received_at TIMESTAMP NULL,
                    code_sent_to_operator_at TIMESTAMP NULL,
                    verified_at TIMESTAMP NULL,
                    verification_status ENUM('pending', 'success', 'failed') NULL,
                    
                    -- Временные метки
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP NULL,
                    completed_at TIMESTAMP NULL,
                    last_activity TIMESTAMP NULL,
                    
                    -- Дополнительные поля для расширенной логики
                    attempts_count INT UNSIGNED DEFAULT 0,
                    max_attempts INT UNSIGNED DEFAULT 3,
                    error_message TEXT NULL,
                    notes TEXT NULL,
                    
                    -- Метаданные для гибкости
                    metadata JSON NULL,
                    settings JSON NULL,
                    
                    -- Индексы для оптимизации
                    INDEX idx_user_id (user_id),
                    INDEX idx_phone_number (phone_number),
                    INDEX idx_country (country),
                    INDEX idx_status (status),
                    INDEX idx_queue_position (queue_position),
                    INDEX idx_priority (priority),
                    INDEX idx_created_at (created_at),
                    INDEX idx_started_at (started_at),
                    INDEX idx_completed_at (completed_at),
                    INDEX idx_last_activity (last_activity),
                    INDEX idx_tariff_type (tariff_type),
                    INDEX idx_status_position (status, queue_position),
                    INDEX idx_user_status (user_id, status),
                    INDEX idx_operator_chat (operator_chat_id),
                    INDEX idx_operator_status (operator_status),
                    
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Создаем таблицу для истории выплат
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL,
                    status ENUM('pending', 'approved', 'rejected', 'completed', 'failed') DEFAULT 'pending',
                    check_id VARCHAR(255) NULL,
                    check_url TEXT NULL,
                    admin_id BIGINT UNSIGNED NULL,
                    admin_comment TEXT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP NULL,
                    
                    INDEX idx_user_id (user_id),
                    INDEX idx_status (status),
                    INDEX idx_created_at (created_at),
                    
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Создаем таблицу для привязки чатов/топиков
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS linked_chats (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    topic_id INT NULL,
                    chat_title VARCHAR(255) NOT NULL,
                    topic_title VARCHAR(255) NULL,
                    linked_by_user_id BIGINT UNSIGNED NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    
                    UNIQUE KEY unique_chat_topic (chat_id, topic_id),
                    INDEX idx_chat_id (chat_id),
                    INDEX idx_topic_id (topic_id),
                    INDEX idx_is_active (is_active),
                    INDEX idx_created_at (created_at),
                    
                    FOREIGN KEY (linked_by_user_id) REFERENCES users(user_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Создаем таблицу для отслеживания запросов кода (для обработки ответов)
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS code_requests (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    phone_number_id INT UNSIGNED NOT NULL,
                    operator_chat_id BIGINT NOT NULL,
                    operator_topic_id INT UNSIGNED NULL,
                    operator_message_id INT UNSIGNED NOT NULL,
                    owner_chat_id BIGINT NOT NULL,
                    owner_message_id INT UNSIGNED NOT NULL,
                    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    code_received VARCHAR(10) NULL,
                    code_received_at TIMESTAMP NULL,
                    
                    INDEX idx_phone_number_id (phone_number_id),
                    INDEX idx_operator_message (operator_chat_id, operator_message_id),
                    INDEX idx_owner_message (owner_chat_id, owner_message_id),
                    INDEX idx_requested_at (requested_at),
                    
                    FOREIGN KEY (phone_number_id) REFERENCES phone_numbers(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Создаем таблицу для настроек уведомлений (ключ-значение для гибкости)
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS notification_settings (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    notification_key VARCHAR(100) NOT NULL UNIQUE,
                    is_enabled BOOLEAN DEFAULT TRUE,
                    message_text TEXT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    
                    INDEX idx_notification_key (notification_key),
                    INDEX idx_is_enabled (is_enabled)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Инициализируем настройки уведомлений по умолчанию, если их еще нет
            default_notifications = [
                ('number_taken', True, 'Ваш номер {phone_number} взят оператором'),
                ('number_verified', True, 'Ваш номер {phone_number} встал'),
                ('number_failed', True, 'Ваш номер {phone_number} слетел'),
                ('number_error', True, 'Ваш номер {phone_number} выдал ошибку'),
                ('number_skipped', True, 'Ваш номер {phone_number} был отменен, добавьте номер снова в очередь'),
                ('wrong_code', True, 'Код по номеру {phone_number} неверен'),
                ('account_ban', True, 'На вашем аккаунте по номеру {phone_number}: бан'),
                ('account_password', True, 'На вашем аккаунте по номеру {phone_number}: есть пароль'),
            ]
            
            for key, enabled, default_text in default_notifications:
                await cursor.execute("""
                    INSERT IGNORE INTO notification_settings (notification_key, is_enabled, message_text)
                    VALUES (%s, %s, %s)
                """, (key, enabled, default_text))
            
            # Создаем таблицу для системных настроек
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_settings (
                    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    setting_key VARCHAR(100) NOT NULL UNIQUE,
                    setting_value TEXT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    
                    INDEX idx_setting_key (setting_key)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Инициализируем настройки по умолчанию
            default_settings = [
                ('max_phone_limit', '0'),  # 0 = без лимита
                ('queue_relevance_minutes', '0'),  # 0 = выключено
                ('activity_check_enabled', 'false'),  # Система проверки активности
                ('activity_check_interval', '5'),  # Интервал отправки проверок (минуты)
                ('activity_response_timeout', '3'),  # Время на ответ (минуты)
                ('tariff_distribution_enabled', 'false'),  # Выдача номеров по тарифам
                ('withdrawals_enabled', 'false'),  # Выводы включены/выключены
                ('auto_withdraw_limit', '0'),  # Лимит для автовывода (0 = без лимита, все требуют подтверждения)
                ('require_username', 'false'),  # Требование username для добавления номеров
                ('auto_success_enabled', 'false'),  # Автоматическое подтверждение "Встал"
                ('auto_success_timeout_minutes', '30'),  # Время через которое автоматически подтверждается "Встал" (минуты)
                ('auto_skip_enabled', 'true'),  # Автоматический скип при отсутствии кода
                ('auto_skip_timeout_minutes', '3'),  # Время через которое автоматически скипается номер без кода (минуты)
                ('support_enabled', 'true'),  # Кнопка техподдержки включена/выключена
                ('support_url', ''),  # Ссылка на техподдержку
            ]
            
            for key, default_value in default_settings:
                await cursor.execute("""
                    INSERT IGNORE INTO system_settings (setting_key, setting_value)
                    VALUES (%s, %s)
                """, (key, default_value))
            
            # Оптимизируем таблицы для высокой нагрузки
            await cursor.execute("""
                ALTER TABLE users 
                ROW_FORMAT=COMPRESSED,
                KEY_BLOCK_SIZE=8
            """)
            
            await cursor.execute("""
                ALTER TABLE tariffs 
                ROW_FORMAT=COMPRESSED,
                KEY_BLOCK_SIZE=8
            """)
            
            await cursor.execute("""
                ALTER TABLE phone_numbers 
                ROW_FORMAT=COMPRESSED,
                KEY_BLOCK_SIZE=8
            """)
            
            await cursor.execute("""
                ALTER TABLE linked_chats 
                ROW_FORMAT=COMPRESSED,
                KEY_BLOCK_SIZE=8
            """)
            
            await cursor.execute("""
                ALTER TABLE code_requests 
                ROW_FORMAT=COMPRESSED,
                KEY_BLOCK_SIZE=8
            """)
            
            # Миграция: добавляем поля для операторов, если их нет
            try:
                await cursor.execute("""
                    SELECT COUNT(*) as cnt 
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = %s 
                    AND TABLE_NAME = 'phone_numbers' 
                    AND COLUMN_NAME = 'operator_chat_id'
                """, (self.db_name,))
                result = await cursor.fetchone()
                if result and result[0] == 0:
                    # Добавляем поля для операторов по одному для избежания ошибок
                    try:
                        await cursor.execute("""
                            ALTER TABLE phone_numbers
                            ADD COLUMN operator_chat_id BIGINT NULL AFTER priority
                        """)
                    except Exception:
                        pass  # Поле уже существует
                    
                    try:
                        await cursor.execute("""
                            ALTER TABLE phone_numbers
                            ADD COLUMN operator_topic_id INT UNSIGNED NULL AFTER operator_chat_id
                        """)
                    except Exception:
                        pass
                    
                    try:
                        await cursor.execute("""
                            ALTER TABLE phone_numbers
                            ADD COLUMN operator_message_id INT UNSIGNED NULL AFTER operator_topic_id
                        """)
                    except Exception:
                        pass
                    
                    try:
                        await cursor.execute("""
                            ALTER TABLE phone_numbers
                            ADD COLUMN operator_status ENUM('requested_code', 'code_received', 'code_verified', 'no_code', 'skipped', 'completed_success', 'completed_error') NULL AFTER operator_message_id
                        """)
                    except Exception:
                        pass
                    
                    try:
                        await cursor.execute("""
                            ALTER TABLE phone_numbers
                            ADD COLUMN code_requested_at TIMESTAMP NULL AFTER operator_status,
                            ADD COLUMN code_received_at TIMESTAMP NULL AFTER code_requested_at,
                            ADD COLUMN code_sent_to_operator_at TIMESTAMP NULL AFTER code_received_at,
                            ADD COLUMN verified_at TIMESTAMP NULL AFTER code_sent_to_operator_at,
                            ADD COLUMN verification_status ENUM('pending', 'success', 'failed') NULL AFTER verified_at
                        """)
                    except Exception:
                        pass
                    
                    try:
                        await cursor.execute("""
                            ALTER TABLE phone_numbers
                            ADD COLUMN operator_message_text TEXT NULL AFTER operator_message_id
                        """)
                    except Exception:
                        pass
                    
                    # Добавляем индексы, если их нет
                    try:
                        await cursor.execute("""
                            CREATE INDEX idx_operator_chat ON phone_numbers(operator_chat_id)
                        """)
                    except Exception:
                        pass
                    
                    try:
                        await cursor.execute("""
                            CREATE INDEX idx_operator_status ON phone_numbers(operator_status)
                        """)
                    except Exception:
                        pass
                    
                    print("✅ Миграция: добавлены поля для операторов в таблицу phone_numbers")
                else:
                    # Поле уже существует, проверяем и меняем тип на SIGNED для поддержки отрицательных chat_id
                    try:
                        await cursor.execute("""
                            SELECT COLUMN_TYPE 
                            FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = %s 
                            AND TABLE_NAME = 'phone_numbers' 
                            AND COLUMN_NAME = 'operator_chat_id'
                        """, (self.db_name,))
                        col_result = await cursor.fetchone()
                        if col_result and 'UNSIGNED' in col_result[0].upper():
                            # Изменяем тип на SIGNED
                            await cursor.execute("""
                                ALTER TABLE phone_numbers
                                MODIFY COLUMN operator_chat_id BIGINT NULL
                            """)
                            print("✅ Миграция: изменен тип operator_chat_id на BIGINT (поддержка отрицательных ID)")
                    except Exception as e:
                        print(f"⚠️ Предупреждение при изменении типа operator_chat_id: {e}")
                    
                    # Проверяем и добавляем operator_topic_id, если его нет
                    try:
                        await cursor.execute("""
                            SELECT COUNT(*) as cnt 
                            FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = %s 
                            AND TABLE_NAME = 'phone_numbers' 
                            AND COLUMN_NAME = 'operator_topic_id'
                        """, (self.db_name,))
                        topic_result = await cursor.fetchone()
                        if topic_result and topic_result[0] == 0:
                            await cursor.execute("""
                                ALTER TABLE phone_numbers
                                ADD COLUMN operator_topic_id INT UNSIGNED NULL AFTER operator_chat_id
                            """)
                            print("✅ Миграция: добавлено поле operator_topic_id в таблицу phone_numbers")
                    except Exception as e:
                        print(f"⚠️ Предупреждение при добавлении operator_topic_id: {e}")
                    
                    # Проверяем и добавляем operator_topic_id в code_requests, если его нет
                    try:
                        await cursor.execute("""
                            SELECT COUNT(*) as cnt 
                            FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = %s 
                            AND TABLE_NAME = 'code_requests' 
                            AND COLUMN_NAME = 'operator_topic_id'
                        """, (self.db_name,))
                        code_topic_result = await cursor.fetchone()
                        if code_topic_result and code_topic_result[0] == 0:
                            await cursor.execute("""
                                ALTER TABLE code_requests
                                ADD COLUMN operator_topic_id INT UNSIGNED NULL AFTER operator_chat_id
                            """)
                            print("✅ Миграция: добавлено поле operator_topic_id в таблицу code_requests")
                    except Exception as e:
                        print(f"⚠️ Предупреждение при добавлении operator_topic_id в code_requests: {e}")
                    
                    # Проверяем и добавляем balance в users, если его нет
                    try:
                        await cursor.execute("""
                            SELECT COUNT(*) as cnt 
                            FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = %s 
                            AND TABLE_NAME = 'users' 
                            AND COLUMN_NAME = 'balance'
                        """, (self.db_name,))
                        balance_result = await cursor.fetchone()
                        if balance_result and balance_result[0] == 0:
                            await cursor.execute("""
                                ALTER TABLE users
                                ADD COLUMN balance DECIMAL(10, 2) DEFAULT 0.00 AFTER fullname
                            """)
                            print("✅ Миграция: добавлено поле balance в таблицу users")
                    except Exception as e:
                        print(f"⚠️ Предупреждение при добавлении balance: {e}")
                    
                    # Проверяем и добавляем payout_amount в tariffs, если его нет
                    try:
                        await cursor.execute("""
                            SELECT COUNT(*) as cnt 
                            FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = %s 
                            AND TABLE_NAME = 'tariffs' 
                            AND COLUMN_NAME = 'payout_amount'
                        """, (self.db_name,))
                        payout_result = await cursor.fetchone()
                        if payout_result and payout_result[0] == 0:
                            await cursor.execute("""
                                ALTER TABLE tariffs
                                ADD COLUMN payout_amount DECIMAL(10, 2) DEFAULT 0.00 AFTER prices
                            """)
                            print("✅ Миграция: добавлено поле payout_amount в таблицу tariffs")
                    except Exception as e:
                        print(f"⚠️ Предупреждение при добавлении payout_amount: {e}")
                    
                    # Создаем таблицу для отслеживания проверок активности
                    await cursor.execute("""
                        CREATE TABLE IF NOT EXISTS activity_checks (
                            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                            user_id BIGINT UNSIGNED NOT NULL,
                            phone_number_id INT UNSIGNED NOT NULL,
                            message_id INT UNSIGNED NOT NULL,
                            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            responded_at TIMESTAMP NULL,
                            is_responded BOOLEAN DEFAULT FALSE,
                            will_delete_at TIMESTAMP NULL,
                            
                            INDEX idx_user_id (user_id),
                            INDEX idx_phone_number_id (phone_number_id),
                            INDEX idx_sent_at (sent_at),
                            INDEX idx_will_delete_at (will_delete_at),
                            INDEX idx_is_responded (is_responded),
                            
                            FOREIGN KEY (phone_number_id) REFERENCES phone_numbers(id) ON DELETE CASCADE,
                            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """)
                    
                    # Проверяем и добавляем поля активности в phone_numbers, если их нет
                    try:
                        await cursor.execute("""
                            SELECT COUNT(*) as cnt 
                            FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = %s 
                            AND TABLE_NAME = 'phone_numbers' 
                            AND COLUMN_NAME = 'last_activity_check'
                        """, (self.db_name,))
                        check_result = await cursor.fetchone()
                        if check_result and check_result[0] == 0:
                            await cursor.execute("""
                                ALTER TABLE phone_numbers
                                ADD COLUMN last_activity_check TIMESTAMP NULL AFTER last_activity
                            """)
                            print("✅ Миграция: добавлено поле last_activity_check в таблицу phone_numbers")
                    except Exception as e:
                        print(f"⚠️ Предупреждение при добавлении last_activity_check: {e}")
                    
                    # Проверяем и добавляем tariff_id в linked_chats, если его нет
                    try:
                        await cursor.execute("""
                            SELECT COUNT(*) as cnt 
                            FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = %s 
                            AND TABLE_NAME = 'linked_chats' 
                            AND COLUMN_NAME = 'tariff_id'
                        """, (self.db_name,))
                        tariff_result = await cursor.fetchone()
                        if tariff_result and tariff_result[0] == 0:
                            await cursor.execute("""
                                ALTER TABLE linked_chats
                                ADD COLUMN tariff_id INT UNSIGNED NULL AFTER topic_title,
                                ADD FOREIGN KEY (tariff_id) REFERENCES tariffs(id) ON DELETE SET NULL
                            """)
                            print("✅ Миграция: добавлено поле tariff_id в таблицу linked_chats")
                    except Exception as e:
                        print(f"⚠️ Предупреждение при добавлении tariff_id: {e}")
            except Exception as e:
                print(f"⚠️ Предупреждение при миграции полей операторов: {e}")
            
            await connection.commit()

        await connection.ensure_closed()

    async def add_or_update_user(self, user_id: int, username: str = None, fullname: str = None) -> bool:
        """Оптимизированное добавление или обновление пользователя"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Используем подготовленный запрос для лучшей производительности
                    await cursor.execute("""
                        INSERT INTO users (user_id, username, fullname) 
                        VALUES (%s, %s, %s) 
                        ON DUPLICATE KEY UPDATE 
                        username = VALUES(username), 
                        fullname = VALUES(fullname),
                        updated_at = CURRENT_TIMESTAMP
                    """, (user_id, username, fullname))
                    return True
        except Exception as e:
            print(f"Ошибка при добавлении/обновлении пользователя: {e}")
            return False

    async def add_or_update_users_batch(self, users_data: List[tuple]) -> bool:
        """Batch операция для добавления/обновления множества пользователей"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.executemany("""
                        INSERT INTO users (user_id, username, fullname) 
                        VALUES (%s, %s, %s) 
                        ON DUPLICATE KEY UPDATE 
                        username = VALUES(username), 
                        fullname = VALUES(fullname),
                        updated_at = CURRENT_TIMESTAMP
                    """, users_data)
                    return True
        except Exception as e:
            print(f"Ошибка при batch добавлении пользователей: {e}")
            return False

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Оптимизированное получение информации о пользователе"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT * FROM users WHERE user_id = %s LIMIT 1", (user_id,))
                    result = await cursor.fetchone()
                    return result
        except Exception as e:
            print(f"Ошибка при получении пользователя: {e}")
            return None

    async def get_users_paginated(self, page: int = 1, limit: int = 50) -> List[Dict[str, Any]]:
        """Получение пользователей с пагинацией для оптимизации"""
        try:
            offset = (page - 1) * limit
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM users 
                        ORDER BY created_at DESC 
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении пользователей: {e}")
            return []

    async def get_users_count(self) -> int:
        """Получение общего количества пользователей"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT COUNT(*) as count FROM users")
                    result = await cursor.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            print(f"Ошибка при получении количества пользователей: {e}")
            return 0

    async def get_recent_users(self, hours: int = 24, limit: int = 100) -> List[Dict[str, Any]]:
        """Получение недавно зарегистрированных пользователей"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM users 
                        WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
                        ORDER BY created_at DESC 
                        LIMIT %s
                    """, (hours, limit))
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении недавних пользователей: {e}")
            return []
    
    async def get_users_statistics(self, period: str = "today") -> Dict[str, Any]:
        """Получает статистику по пользователям за указанный период"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Определяем условие для фильтрации по периоду
                    if period == "today":
                        where_clause = "DATE(created_at) = CURDATE()"
                    elif period == "yesterday":
                        where_clause = "DATE(created_at) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)"
                    elif period == "week":
                        where_clause = "created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)"
                    elif period == "month":
                        where_clause = "created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)"
                    elif period == "30days":
                        where_clause = "created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)"
                    else:  # all_time
                        where_clause = "1=1"
                    
                    # Количество новых пользователей
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count FROM users WHERE {where_clause}
                    """)
                    new_users = await cursor.fetchone()
                    
                    # Всего пользователей
                    await cursor.execute("SELECT COUNT(*) as count FROM users")
                    total_users = await cursor.fetchone()
                    
                    return {
                        'new_users': new_users['count'] if new_users else 0,
                        'total_users': total_users['count'] if total_users else 0
                    }
        except Exception as e:
            print(f"Ошибка при получении статистики пользователей: {e}")
            return {'new_users': 0, 'total_users': 0}
    
    async def get_numbers_statistics(self, period: str = "today") -> Dict[str, Any]:
        """Получает статистику по номерам за указанный период"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Определяем условие для фильтрации по периоду
                    if period == "today":
                        where_clause = "DATE(created_at) = CURDATE()"
                    elif period == "yesterday":
                        where_clause = "DATE(created_at) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)"
                    elif period == "week":
                        where_clause = "created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)"
                    elif period == "month":
                        where_clause = "created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)"
                    elif period == "30days":
                        where_clause = "created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)"
                    else:  # all_time
                        where_clause = "1=1"
                    
                    # Всего номеров за период
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count FROM phone_numbers WHERE {where_clause}
                    """)
                    total_numbers = await cursor.fetchone()
                    
                    # Встало (completed_success)
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count FROM phone_numbers 
                        WHERE {where_clause}
                        AND operator_status = 'completed_success'
                    """)
                    success_count = await cursor.fetchone()
                    
                    # Слетело (встал и потом слетел)
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count FROM phone_numbers 
                        WHERE {where_clause}
                        AND operator_status = 'completed_success'
                        AND verified_at IS NOT NULL
                        AND completed_at IS NOT NULL
                        AND verified_at != completed_at
                    """)
                    failed_count = await cursor.fetchone()
                    
                    # Ошибок
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count FROM phone_numbers 
                        WHERE {where_clause}
                        AND operator_status = 'completed_error'
                    """)
                    errors_count = await cursor.fetchone()
                    
                    # В очереди
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count FROM phone_numbers 
                        WHERE {where_clause}
                        AND status = 'waiting'
                    """)
                    waiting_count = await cursor.fetchone()
                    
                    # В работе
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count FROM phone_numbers 
                        WHERE {where_clause}
                        AND status = 'active'
                        AND operator_status NOT IN ('completed_success', 'completed_error', 'skipped', 'no_code')
                    """)
                    active_count = await cursor.fetchone()
                    
                    return {
                        'total': total_numbers['count'] if total_numbers else 0,
                        'success': success_count['count'] if success_count else 0,
                        'failed': failed_count['count'] if failed_count else 0,
                        'errors': errors_count['count'] if errors_count else 0,
                        'waiting': waiting_count['count'] if waiting_count else 0,
                        'active': active_count['count'] if active_count else 0
                    }
        except Exception as e:
            print(f"Ошибка при получении статистики номеров: {e}")
            return {
                'total': 0,
                'success': 0,
                'failed': 0,
                'errors': 0,
                'waiting': 0,
                'active': 0
            }

    # Методы для работы с тарифами
    async def create_tariff(self, name: str, tariff_type: str, country: str, prices: dict, max_duration: int = None, payout_amount: float = 0.00) -> bool:
        """Создает новый тариф"""
        try:
            import json
            prices_json = json.dumps(prices, ensure_ascii=False)
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO tariffs (name, type, country, prices, payout_amount, max_duration) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (name, tariff_type, country, prices_json, payout_amount, max_duration))
                    return True
        except Exception as e:
            print(f"Ошибка при создании тарифа: {e}")
            return False

    async def get_all_tariffs(self) -> List[Dict[str, Any]]:
        """Получает все тарифы"""
        try:
            import json
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT * FROM tariffs ORDER BY created_at DESC")
                    result = await cursor.fetchall()
                    # Десериализуем JSON строки обратно в словари
                    for tariff in result:
                        prices = tariff.get('prices')
                        if prices is not None:
                            if isinstance(prices, dict):
                                # Уже словарь, оставляем как есть
                                continue
                            elif isinstance(prices, (str, bytes, bytearray)):
                                # JSON строка или bytes, парсим
                                try:
                                    prices_str = prices.decode('utf-8') if isinstance(prices, (bytes, bytearray)) else prices
                                    tariff['prices'] = json.loads(prices_str)
                                except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as e:
                                    print(f"Ошибка парсинга цен для тарифа {tariff.get('id', 'unknown')}: {e}")
                                    tariff['prices'] = {}
                            else:
                                # Неизвестный тип
                                print(f"Неожиданный тип цен для тарифа {tariff.get('id', 'unknown')}: {type(prices)}")
                                tariff['prices'] = {}
                    return result
        except Exception as e:
            print(f"Ошибка при получении тарифов: {e}")
            return []

    async def get_tariff_by_id(self, tariff_id: int) -> Optional[Dict[str, Any]]:
        """Получает тариф по ID"""
        try:
            import json
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT * FROM tariffs WHERE id = %s", (tariff_id,))
                    result = await cursor.fetchone()
                    if result:
                        prices = result.get('prices')
                        if prices is not None:
                            if isinstance(prices, dict):
                                # Уже словарь, оставляем как есть
                                pass
                            elif isinstance(prices, (str, bytes, bytearray)):
                                # JSON строка или bytes, парсим
                                try:
                                    prices_str = prices.decode('utf-8') if isinstance(prices, (bytes, bytearray)) else prices
                                    result['prices'] = json.loads(prices_str)
                                except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as e:
                                    print(f"Ошибка парсинга цен для тарифа {result.get('id', 'unknown')}: {e}")
                                    result['prices'] = {}
                            else:
                                # Неизвестный тип
                                print(f"Неожиданный тип цен для тарифа {result.get('id', 'unknown')}: {type(prices)}")
                                result['prices'] = {}
                    return result
        except Exception as e:
            print(f"Ошибка при получении тарифа: {e}")
            return None

    async def delete_tariff(self, tariff_id: int) -> bool:
        """Удаляет тариф"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("DELETE FROM tariffs WHERE id = %s", (tariff_id,))
                    return True
        except Exception as e:
            print(f"Ошибка при удалении тарифа: {e}")
            return False

    # Методы для работы с универсальной таблицей номеров
    async def add_phone_number(self, user_id: int, phone_number: str, country: str, 
                              tariff_name: str, tariff_type: str, tariff_prices: dict,
                              priority: int = 0, metadata: dict = None, settings: dict = None) -> bool:
        """Добавляет номер в универсальную таблицу"""
        try:
            import json
            
            # Конвертируем словари в JSON строки
            tariff_prices_json = json.dumps(tariff_prices, ensure_ascii=False)
            metadata_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
            settings_json = json.dumps(settings, ensure_ascii=False) if settings else None
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Получаем следующую позицию в очереди
                    await cursor.execute("""
                        SELECT COALESCE(MAX(queue_position), 0) + 1 
                        FROM phone_numbers 
                        WHERE status = 'waiting'
                    """)
                    next_position = await cursor.fetchone()
                    queue_position = next_position[0] if next_position else 1
                    
                    await cursor.execute("""
                        INSERT INTO phone_numbers (
                            user_id, phone_number, country, tariff_name, tariff_type, 
                            tariff_prices, queue_position, priority, metadata, settings
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (user_id, phone_number, country, tariff_name, tariff_type, 
                          tariff_prices_json, queue_position, priority, metadata_json, settings_json))
                    return True
        except Exception as e:
            print(f"Ошибка при добавлении номера: {e}")
            return False

    async def get_user_phone_numbers(self, user_id: int, status: str = None) -> List[Dict[str, Any]]:
        """Получает номера пользователя с возможностью фильтрации по статусу"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    if status:
                        await cursor.execute("""
                            SELECT * FROM phone_numbers 
                            WHERE user_id = %s AND status = %s
                            ORDER BY queue_position ASC, created_at ASC
                        """, (user_id, status))
                    else:
                        await cursor.execute("""
                            SELECT * FROM phone_numbers 
                            WHERE user_id = %s
                            ORDER BY queue_position ASC, created_at ASC
                        """, (user_id,))
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении номеров пользователя: {e}")
            return []
    
    async def get_user_archived_numbers(self, user_id: int, page: int = 1, limit: int = 10) -> tuple:
        """Получает завершенные номера пользователя для архива с пагинацией"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Получаем завершенные номера (completed, failed, cancelled)
                    offset = (page - 1) * limit
                    await cursor.execute("""
                        SELECT * FROM phone_numbers 
                        WHERE user_id = %s 
                        AND status IN ('completed', 'failed', 'cancelled')
                        ORDER BY completed_at DESC, created_at DESC
                        LIMIT %s OFFSET %s
                    """, (user_id, limit, offset))
                    result = await cursor.fetchall()
                    
                    # Получаем общее количество
                    await cursor.execute("""
                        SELECT COUNT(*) FROM phone_numbers 
                        WHERE user_id = %s 
                        AND status IN ('completed', 'failed', 'cancelled')
                    """, (user_id,))
                    total_result = await cursor.fetchone()
                    total = total_result[0] if total_result else 0
                    
                    return result, total
        except Exception as e:
            return [], 0

    async def get_phone_numbers_count(self, status: str = None) -> int:
        """Получает количество номеров с возможностью фильтрации по статусу"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    if status:
                        await cursor.execute("SELECT COUNT(*) FROM phone_numbers WHERE status = %s", (status,))
                    else:
                        await cursor.execute("SELECT COUNT(*) FROM phone_numbers")
                    result = await cursor.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            print(f"Ошибка при получении количества номеров: {e}")
            return 0

    async def update_phone_number_status(self, phone_id: int, status: str, 
                                        error_message: str = None, notes: str = None) -> bool:
        """Обновляет статус номера"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Обновляем статус и временные метки
                    if status == 'active':
                        await cursor.execute("""
                            UPDATE phone_numbers 
                            SET status = %s, started_at = CURRENT_TIMESTAMP, last_activity = CURRENT_TIMESTAMP,
                                error_message = %s, notes = %s
                            WHERE id = %s
                        """, (status, error_message, notes, phone_id))
                    elif status in ['completed', 'cancelled', 'failed']:
                        await cursor.execute("""
                            UPDATE phone_numbers 
                            SET status = %s, completed_at = CURRENT_TIMESTAMP, last_activity = CURRENT_TIMESTAMP,
                                error_message = %s, notes = %s
                            WHERE id = %s
                        """, (status, error_message, notes, phone_id))
                    else:
                        await cursor.execute("""
                            UPDATE phone_numbers 
                            SET status = %s, last_activity = CURRENT_TIMESTAMP,
                                error_message = %s, notes = %s
                            WHERE id = %s
                        """, (status, error_message, notes, phone_id))
                    return True
        except Exception as e:
            print(f"Ошибка при обновлении статуса номера: {e}")
            return False

    async def remove_phone_number(self, phone_id: int) -> bool:
        """Удаляет номер из таблицы"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("DELETE FROM phone_numbers WHERE id = %s", (phone_id,))
                    return True
        except Exception as e:
            print(f"Ошибка при удалении номера: {e}")
            return False

    async def clear_user_phone_numbers(self, user_id: int, status: str = None) -> bool:
        """Очищает номера пользователя с возможностью фильтрации по статусу"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    if status:
                        await cursor.execute("DELETE FROM phone_numbers WHERE user_id = %s AND status = %s", 
                                           (user_id, status))
                    else:
                        await cursor.execute("DELETE FROM phone_numbers WHERE user_id = %s", (user_id,))
                    return True
        except Exception as e:
            print(f"Ошибка при очистке номеров пользователя: {e}")
            return False

    async def get_phone_numbers_by_status(self, status: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Получает номера по статусу с лимитом"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM phone_numbers 
                        WHERE status = %s
                        ORDER BY queue_position ASC, priority DESC, created_at ASC
                        LIMIT %s
                    """, (status, limit))
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении номеров по статусу: {e}")
            return []

    async def update_phone_number_metadata(self, phone_id: int, metadata: dict) -> bool:
        """Обновляет метаданные номера"""
        try:
            import json
            metadata_json = json.dumps(metadata, ensure_ascii=False)
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE phone_numbers 
                        SET metadata = %s, last_activity = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (metadata_json, phone_id))
                    return True
        except Exception as e:
            print(f"Ошибка при обновлении метаданных: {e}")
            return False

    async def increment_attempts(self, phone_id: int) -> bool:
        """Увеличивает счетчик попыток"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE phone_numbers 
                        SET attempts_count = attempts_count + 1, last_activity = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (phone_id,))
                    return True
        except Exception as e:
            print(f"Ошибка при увеличении счетчика попыток: {e}")
            return False

    async def get_phone_number_by_id(self, phone_id: int) -> Optional[Dict[str, Any]]:
        """Получает номер по ID"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT * FROM phone_numbers WHERE id = %s", (phone_id,))
                    result = await cursor.fetchone()
                    return result
        except Exception as e:
            print(f"Ошибка при получении номера по ID: {e}")
            return None

    async def was_phone_verified_today(self, phone_number: str) -> bool:
        """Проверяет, был ли номер уже успешно сдан сегодня (независимо от пользователя)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM phone_numbers 
                        WHERE phone_number = %s 
                        AND status = 'completed'
                        AND completed_at IS NOT NULL
                        AND DATE(completed_at) = CURDATE()
                        AND verification_status = 'success'
                    """, (phone_number,))
                    result = await cursor.fetchone()
                    return result[0] > 0 if result else False
        except Exception as e:
            print(f"Ошибка при проверке номера на сегодняшнюю сдачу: {e}")
            return False

    async def get_phones_for_auto_success(self, timeout_minutes: int) -> List[Dict[str, Any]]:
        """Получает номера для автоматического подтверждения 'Встал'"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT id, phone_number, operator_chat_id, operator_topic_id, 
                               operator_message_id, user_id, tariff_name, started_at, last_activity,
                               operator_status, verification_status, status
                        FROM phone_numbers
                        WHERE status = 'active'
                        AND operator_chat_id IS NOT NULL
                        AND operator_status IN ('code_received', 'code_verified')
                        AND (verification_status IS NULL OR verification_status != 'success')
                        AND (
                            (started_at IS NOT NULL 
                             AND TIMESTAMPDIFF(MINUTE, started_at, NOW()) >= %s)
                            OR 
                            (started_at IS NULL 
                             AND last_activity IS NOT NULL
                             AND TIMESTAMPDIFF(MINUTE, last_activity, NOW()) >= %s)
                        )
                    """, (timeout_minutes, timeout_minutes))
                    return await cursor.fetchall()
        except Exception as e:
            print(f"Ошибка при получении номеров для авто-подтверждения: {e}")
            return []

    async def get_phones_for_auto_skip(self, timeout_minutes: int) -> List[Dict[str, Any]]:
        """Получает номера для автоматического скипа при отсутствии кода"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT id, phone_number, operator_chat_id, operator_topic_id, 
                               operator_message_id, user_id, tariff_name, code_requested_at, started_at,
                               operator_status, verification_status, status
                        FROM phone_numbers
                        WHERE status = 'active'
                        AND operator_chat_id IS NOT NULL
                        AND operator_status = 'requested_code'
                        AND code_requested_at IS NOT NULL
                        AND TIMESTAMPDIFF(MINUTE, code_requested_at, NOW()) >= %s
                    """, (timeout_minutes,))
                    return await cursor.fetchall()
        except Exception as e:
            print(f"Ошибка при получении номеров для авто-скипа: {e}")
            return []

    # Методы для работы с приоритетами
    async def set_phone_number_priority(self, phone_id: int, priority: int) -> bool:
        """Устанавливает приоритет номера"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE phone_numbers 
                        SET priority = %s, last_activity = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (priority, phone_id))
                    return True
        except Exception as e:
            print(f"Ошибка при установке приоритета: {e}")
            return False

    async def remove_phone_number_priority(self, phone_id: int) -> bool:
        """Снимает приоритет с номера"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE phone_numbers 
                        SET priority = 0, last_activity = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (phone_id,))
                    return True
        except Exception as e:
            print(f"Ошибка при снятии приоритета: {e}")
            return False

    async def get_phone_numbers_by_priority(self, priority: int, status: str = None) -> List[Dict[str, Any]]:
        """Получает номера по приоритету"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    if status:
                        await cursor.execute("""
                            SELECT * FROM phone_numbers 
                            WHERE priority = %s AND status = %s
                            ORDER BY queue_position ASC, created_at ASC
                        """, (priority, status))
                    else:
                        await cursor.execute("""
                            SELECT * FROM phone_numbers 
                            WHERE priority = %s
                            ORDER BY queue_position ASC, created_at ASC
                        """, (priority,))
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении номеров по приоритету: {e}")
            return []

    async def get_priority_statistics(self) -> Dict[str, int]:
        """Получает статистику по приоритетам"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT priority, COUNT(*) as count 
                        FROM phone_numbers 
                        WHERE status = 'waiting'
                        GROUP BY priority
                        ORDER BY priority DESC
                    """)
                    result = await cursor.fetchall()
                    
                    stats = {}
                    for row in result:
                        priority = row[0]
                        count = row[1]
                        if priority == 0:
                            stats['normal'] = count
                        else:
                            stats[f'priority_{priority}'] = count
                    
                    return stats
        except Exception as e:
            print(f"Ошибка при получении статистики приоритетов: {e}")
            return {}

    async def get_all_phone_numbers_for_admin(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Получает все номера для админ панели с пагинацией"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT pn.*, u.username, u.fullname
                        FROM phone_numbers pn
                        LEFT JOIN users u ON pn.user_id = u.user_id
                        ORDER BY pn.priority DESC, pn.queue_position ASC, pn.created_at ASC
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении номеров для админа: {e}")
            return []

    # Методы для работы с привязанными чатами
    async def link_chat(self, chat_id: int, topic_id: int, chat_title: str, topic_title: str, user_id: int, tariff_id: int = None) -> bool:
        """Привязывает чат/топик к боту"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO linked_chats (chat_id, topic_id, chat_title, topic_title, linked_by_user_id, tariff_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        chat_title = VALUES(chat_title),
                        topic_title = VALUES(topic_title),
                        linked_by_user_id = VALUES(linked_by_user_id),
                        tariff_id = VALUES(tariff_id),
                        is_active = TRUE,
                        updated_at = CURRENT_TIMESTAMP
                    """, (chat_id, topic_id if topic_id else None, chat_title, topic_title, user_id, tariff_id))
                    return True
        except Exception as e:
            print(f"Ошибка при привязке чата: {e}")
            return False
    
    async def get_linked_chat(self, chat_id: int, topic_id: int = None) -> Optional[Dict[str, Any]]:
        """Получает информацию о привязанном чате/топике"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM linked_chats
                        WHERE chat_id = %s AND (topic_id = %s OR (topic_id IS NULL AND %s IS NULL))
                        AND is_active = TRUE
                        LIMIT 1
                    """, (chat_id, topic_id, topic_id))
                    result = await cursor.fetchone()
                    return result
        except Exception as e:
            print(f"Ошибка при получении привязанного чата: {e}")
            return None
    
    async def unlink_chat(self, chat_id: int, topic_id: int = None) -> bool:
        """Отвязывает чат/топик"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE linked_chats
                        SET is_active = FALSE
                        WHERE chat_id = %s AND (topic_id = %s OR (topic_id IS NULL AND %s IS NULL))
                    """, (chat_id, topic_id, topic_id))
                    return True
        except Exception as e:
            print(f"Ошибка при отвязке чата: {e}")
            return False
    
    # Методы для работы с операторами и кодами
    async def assign_number_to_operator(self, phone_id: int, operator_chat_id: int, operator_topic_id: int, operator_message_id: int) -> bool:
        """Назначает номер оператору"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE phone_numbers
                        SET operator_chat_id = %s,
                            operator_topic_id = %s,
                            operator_message_id = %s,
                            status = 'active',
                            started_at = CURRENT_TIMESTAMP,
                            last_activity = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (operator_chat_id, operator_topic_id, operator_message_id, phone_id))
                    return True
        except Exception as e:
            print(f"Ошибка при назначении номера оператору: {e}")
            return False
    
    async def get_next_waiting_number(self, tariff_name: str = None) -> Optional[Dict[str, Any]]:
        """Получает следующий номер в очереди с учетом приоритета и тарифа (если указан)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    if tariff_name:
                        # Если указан тариф, ищем только номера этого тарифа
                        await cursor.execute("""
                            SELECT * FROM phone_numbers
                            WHERE status = 'waiting' 
                            AND operator_chat_id IS NULL
                            AND tariff_name = %s
                            ORDER BY priority DESC, queue_position ASC, created_at ASC
                            LIMIT 1
                        """, (tariff_name,))
                    else:
                        # Если тариф не указан, ищем все номера
                        await cursor.execute("""
                            SELECT * FROM phone_numbers
                            WHERE status = 'waiting' AND operator_chat_id IS NULL
                            ORDER BY priority DESC, queue_position ASC, created_at ASC
                            LIMIT 1
                        """)
                    result = await cursor.fetchone()
                    return result
        except Exception as e:
            print(f"Ошибка при получении следующего номера: {e}")
            return None
    
    async def create_code_request(self, phone_id: int, operator_chat_id: int, operator_topic_id: int, 
                                  operator_message_id: int, owner_chat_id: int, owner_message_id: int) -> int:
        """Создает запрос на код"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO code_requests (phone_number_id, operator_chat_id, operator_topic_id, operator_message_id,
                                                  owner_chat_id, owner_message_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (phone_id, operator_chat_id, operator_topic_id, operator_message_id, owner_chat_id, owner_message_id))
                    return cursor.lastrowid
        except Exception as e:
            print(f"Ошибка при создании запроса кода: {e}")
            return 0
    
    async def update_code_request(self, owner_chat_id: int, owner_message_id: int, code: str) -> bool:
        """Обновляет запрос кода (код получен от дропа)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE code_requests
                        SET code_received = %s, code_received_at = CURRENT_TIMESTAMP
                        WHERE owner_chat_id = %s AND owner_message_id = %s
                        AND code_received IS NULL
                        LIMIT 1
                    """, (code, owner_chat_id, owner_message_id))
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при обновлении запроса кода: {e}")
            return False
    
    async def get_code_request_by_owner_message(self, owner_chat_id: int, owner_message_id: int) -> Optional[Dict[str, Any]]:
        """Получает запрос кода по сообщению владельца"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM code_requests
                        WHERE owner_chat_id = %s AND owner_message_id = %s
                        AND code_received IS NULL
                        LIMIT 1
                    """, (owner_chat_id, owner_message_id))
                    result = await cursor.fetchone()
                    return result
        except Exception as e:
            print(f"Ошибка при получении запроса кода: {e}")
            return None
    
    async def get_code_request_by_operator_message(self, operator_chat_id: int, operator_message_id: int) -> Optional[Dict[str, Any]]:
        """Получает запрос кода по сообщению оператора"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM code_requests
                        WHERE operator_chat_id = %s AND operator_message_id = %s
                        LIMIT 1
                    """, (operator_chat_id, operator_message_id))
                    result = await cursor.fetchone()
                    return result
        except Exception as e:
            print(f"Ошибка при получении запроса кода по оператору: {e}")
            return None
    
    async def update_phone_operator_status(self, phone_id: int, operator_status: str, code: str = None) -> bool:
        """Обновляет статус работы оператора с номером"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    status_updates = {
                        'requested_code': 'code_requested_at = CURRENT_TIMESTAMP',
                        'code_received': 'code_received_at = CURRENT_TIMESTAMP',
                        'code_verified': 'code_sent_to_operator_at = CURRENT_TIMESTAMP',
                        'completed_success': 'completed_at = CURRENT_TIMESTAMP, verification_status = "success"',
                        'completed_error': 'completed_at = CURRENT_TIMESTAMP, verification_status = "failed"'
                    }
                    
                    update_clause = status_updates.get(operator_status, '')
                    if update_clause:
                        query = f"""
                            UPDATE phone_numbers
                            SET operator_status = %s,
                                {update_clause},
                                last_activity = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """
                    else:
                        query = """
                            UPDATE phone_numbers
                            SET operator_status = %s,
                                last_activity = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """
                    await cursor.execute(query, (operator_status, phone_id))
                    return True
        except Exception as e:
            print(f"Ошибка при обновлении статуса оператора: {e}")
            return False
    
    async def update_phone_verification_result(self, phone_id: int, verification_status: str) -> bool:
        """Обновляет результат верификации (встал/слетел/ошибка)"""
        try:
            import json
            status_map = {
                'success': 'completed_success',
                'failed': 'completed_error'
            }
            operator_status = status_map.get(verification_status)
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE phone_numbers
                        SET operator_status = %s,
                            verification_status = %s,
                            verified_at = CURRENT_TIMESTAMP,
                            completed_at = CURRENT_TIMESTAMP,
                            status = 'completed',
                            last_activity = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (operator_status, verification_status, phone_id))
                    return True
        except Exception as e:
            print(f"Ошибка при обновлении результата верификации: {e}")
            return False

    async def check_phone_number_today_success(self, phone_number: str) -> bool:
        """Проверяет, был ли номер сегодня в статусе 'встал' (completed_success)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM phone_numbers
                        WHERE phone_number = %s
                        AND operator_status = 'completed_success'
                        AND DATE(verified_at) = CURDATE()
                        LIMIT 1
                    """, (phone_number,))
                    result = await cursor.fetchone()
                    return result and result['cnt'] > 0
        except Exception as e:
            print(f"Ошибка при проверке статуса номера: {e}")
            return False
    
    async def check_phone_number_in_queue(self, phone_number: str) -> bool:
        """Проверяет, не находится ли номер уже в очереди (статус waiting)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM phone_numbers
                        WHERE phone_number = %s
                        AND status = 'waiting'
                        AND operator_chat_id IS NULL
                        LIMIT 1
                    """, (phone_number,))
                    result = await cursor.fetchone()
                    return result and result['cnt'] > 0
        except Exception as e:
            print(f"Ошибка при проверке номера в очереди: {e}")
            return False
    
    async def check_phone_number_taken_by_operator(self, phone_number: str) -> bool:
        """Проверяет, не взят ли номер оператором (активно работает с оператором)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT COUNT(*) as cnt
                        FROM phone_numbers
                        WHERE phone_number = %s
                        AND operator_chat_id IS NOT NULL
                        AND status = 'active'
                        AND (operator_status IS NULL 
                             OR operator_status NOT IN ('completed_success', 'completed_error', 'skipped', 'no_code'))
                        LIMIT 1
                    """, (phone_number,))
                    result = await cursor.fetchone()
                    return result and result['cnt'] > 0
        except Exception as e:
            print(f"Ошибка при проверке занятости номера: {e}")
            return False

    # Методы для работы с отчетами
    async def get_tariff_reports(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Получает отчеты по тарифам (номера которые встали и слетели с расчетом времени)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT 
                            phone_number,
                            operator_status,
                            verification_status,
                            verified_at,
                            completed_at,
                            CASE 
                                WHEN completed_at IS NOT NULL AND verified_at IS NOT NULL 
                                THEN TIMESTAMPDIFF(MINUTE, verified_at, completed_at)
                                ELSE NULL
                            END as standing_minutes
                        FROM phone_numbers
                        WHERE verified_at IS NOT NULL
                        AND DATE(verified_at) = CURDATE()
                        AND (
                            operator_status = 'completed_success'
                            OR operator_status = 'completed_error'
                        )
                        ORDER BY verified_at DESC
                        LIMIT %s
                    """, (limit,))
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении отчетов по тарифам: {e}")
            return []
    
    async def get_all_linked_chats(self, page: int = 1, limit: int = 10) -> tuple:
        """Получает все привязанные чаты/топики с пагинацией"""
        try:
            offset = (page - 1) * limit
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Получаем общее количество
                    await cursor.execute("""
                        SELECT COUNT(*) as total
                        FROM linked_chats
                        WHERE is_active = TRUE
                    """)
                    total_result = await cursor.fetchone()
                    total = total_result['total'] if total_result else 0
                    
                    # Получаем страницу
                    await cursor.execute("""
                        SELECT * FROM linked_chats
                        WHERE is_active = TRUE
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                    result = await cursor.fetchall()
                    return result, total
        except Exception as e:
            print(f"Ошибка при получении привязанных чатов: {e}")
            return [], 0
    
    async def get_linked_chat_statistics(self, chat_id: int, topic_id: int = None) -> Dict[str, Any]:
        """Получает статистику по привязанному чату/топику"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Формируем условие для фильтрации по чату и топику
                    if topic_id is not None:
                        # Если есть topic_id, фильтруем по обоим
                        where_clause = "operator_chat_id = %s AND operator_topic_id = %s"
                        params = (chat_id, topic_id)
                    else:
                        # Если нет topic_id, только по чату и только те, у кого нет топика
                        where_clause = "operator_chat_id = %s AND (operator_topic_id IS NULL OR operator_topic_id = 0)"
                        params = (chat_id,)
                    
                    # Сколько номеров встало сегодня
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM phone_numbers
                        WHERE {where_clause}
                        AND operator_status = 'completed_success'
                        AND DATE(verified_at) = CURDATE()
                    """, params)
                    success_today = await cursor.fetchone()
                    
                    # Сколько номеров слетело сегодня (встали и потом слетели)
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM phone_numbers
                        WHERE {where_clause}
                        AND operator_status = 'completed_success'
                        AND verified_at IS NOT NULL
                        AND completed_at IS NOT NULL
                        AND DATE(completed_at) = CURDATE()
                    """, params)
                    failed_today = await cursor.fetchone()
                    
                    # Сколько ошибок сегодня
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM phone_numbers
                        WHERE {where_clause}
                        AND operator_status = 'completed_error'
                        AND DATE(completed_at) = CURDATE()
                    """, params)
                    errors_today = await cursor.fetchone()
                    
                    # Сколько номеров в работе сейчас (только за сегодня)
                    # Учитываем номера со status='active' и operator_status NULL или не завершенные статусы
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM phone_numbers
                        WHERE {where_clause}
                        AND status = 'active'
                        AND (operator_status IS NULL OR operator_status NOT IN ('completed_success', 'completed_error', 'skipped', 'no_code'))
                        AND DATE(started_at) = CURDATE()
                    """, params)
                    in_progress = await cursor.fetchone()
                    
                    # Всего номеров обработано сегодня
                    await cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM phone_numbers
                        WHERE {where_clause}
                        AND DATE(created_at) = CURDATE()
                    """, params)
                    total_today = await cursor.fetchone()
                    
                    return {
                        'success_today': success_today['count'] if success_today else 0,
                        'failed_today': failed_today['count'] if failed_today else 0,
                        'errors_today': errors_today['count'] if errors_today else 0,
                        'in_progress': in_progress['count'] if in_progress else 0,
                        'total_today': total_today['count'] if total_today else 0
                    }
        except Exception as e:
            print(f"Ошибка при получении статистики привязки: {e}")
            return {
                'success_today': 0,
                'failed_today': 0,
                'errors_today': 0,
                'in_progress': 0,
                'total_today': 0
            }
    
    async def get_linked_chat_detailed_numbers(self, chat_id: int, topic_id: int = None) -> List[Dict[str, Any]]:
        """Получает детальную информацию о всех номерах по привязке за текущий день"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Формируем условие для фильтрации по чату и топику
                    if topic_id is not None:
                        # Если есть topic_id, фильтруем по обоим
                        where_clause = "operator_chat_id = %s AND operator_topic_id = %s"
                        params = (chat_id, topic_id)
                    else:
                        # Если нет topic_id, только по чату и только те, у кого нет топика
                        where_clause = "operator_chat_id = %s AND (operator_topic_id IS NULL OR operator_topic_id = 0)"
                        params = (chat_id,)
                    
                    # Получаем все номера за сегодня
                    await cursor.execute(f"""
                        SELECT 
                            phone_number,
                            tariff_name,
                            status,
                            operator_status,
                            verification_status,
                            created_at,
                            verified_at,
                            completed_at,
                            code_requested_at,
                            code_received_at,
                            error_message,
                            user_id
                        FROM phone_numbers
                        WHERE {where_clause}
                        AND DATE(created_at) = CURDATE()
                        ORDER BY created_at DESC
                    """, params)
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении детальных номеров привязки: {e}")
            return []
    
    async def update_linked_chat_title(self, chat_id: int, topic_id: int = None, 
                                       new_chat_title: str = None, new_topic_title: str = None) -> bool:
        """Обновляет название привязанного чата/топика (только визуально)"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    updates = []
                    params = []
                    
                    if new_chat_title:
                        updates.append("chat_title = %s")
                        params.append(new_chat_title)
                    
                    if new_topic_title is not None:
                        updates.append("topic_title = %s")
                        params.append(new_topic_title)
                    
                    if not updates:
                        return False
                    
                    query = f"""
                        UPDATE linked_chats
                        SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                        WHERE chat_id = %s AND (topic_id = %s OR (topic_id IS NULL AND %s IS NULL))
                    """
                    params.extend([chat_id, topic_id if topic_id else None, topic_id if topic_id else None])
                    
                    await cursor.execute(query, params)
                    return True
        except Exception as e:
            print(f"Ошибка при обновлении названия привязки: {e}")
            return False

    async def validate_phone_number(self, phone_number: str) -> tuple:
        """Валидирует номер телефона и определяет страну, автоматически форматирует"""
        # Убираем все пробелы, дефисы, скобки и другие символы
        cleaned = ''.join(c for c in phone_number.strip() if c.isdigit() or c == '+')
        
        # Обрабатываем разные форматы
        if cleaned.startswith('+7'):
            # Формат: +7XXXXXXXXXX или +77XXXXXXXXXX
            if cleaned.startswith('+77') and len(cleaned) == 13:
                # Казахстан
                return True, 'KZ', cleaned
            elif len(cleaned) == 12:
                # Россия
                return True, 'RU', cleaned
        elif cleaned.startswith('8'):
            # Формат: 8XXXXXXXXXX (Россия)
            if len(cleaned) == 11:
                # Преобразуем 8XXXXXXXXXX в +7XXXXXXXXXX
                formatted = '+7' + cleaned[1:]
                return True, 'RU', formatted
        elif cleaned.startswith('7'):
            # Формат: 7XXXXXXXXXX или 77XXXXXXXXXX
            if cleaned.startswith('77') and len(cleaned) == 12:
                # Казахстан: 77XXXXXXXXXX
                formatted = '+' + cleaned
                return True, 'KZ', formatted
            elif len(cleaned) == 11:
                # Россия: 7XXXXXXXXXX
                formatted = '+' + cleaned
                return True, 'RU', formatted
        
        return False, None, None
    
    # Методы для работы с уведомлениями
    async def get_all_notification_settings(self) -> Dict[str, Dict[str, Any]]:
        """Получает все настройки уведомлений"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT notification_key, is_enabled, message_text
                        FROM notification_settings
                        ORDER BY notification_key
                    """)
                    result = await cursor.fetchall()
                    
                    settings = {}
                    for row in result:
                        settings[row['notification_key']] = {
                            'is_enabled': bool(row['is_enabled']),
                            'message_text': row['message_text'] or ''
                        }
                    return settings
        except Exception as e:
            print(f"Ошибка при получении настроек уведомлений: {e}")
            return {}
    
    async def toggle_notification(self, notification_key: str) -> bool:
        """Переключает состояние уведомления"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE notification_settings
                        SET is_enabled = NOT is_enabled,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE notification_key = %s
                    """, (notification_key,))
                    
                    if cursor.rowcount == 0:
                        # Если записи нет, создаем с включенным состоянием
                        await cursor.execute("""
                            INSERT INTO notification_settings (notification_key, is_enabled)
                            VALUES (%s, TRUE)
                        """, (notification_key,))
                    
                    return True
        except Exception as e:
            print(f"Ошибка при переключении уведомления: {e}")
            return False
    
    async def is_notification_enabled(self, notification_key: str) -> bool:
        """Проверяет, включено ли уведомление"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT is_enabled FROM notification_settings
                        WHERE notification_key = %s
                    """, (notification_key,))
                    result = await cursor.fetchone()
                    return bool(result[0]) if result else False
        except Exception as e:
            print(f"Ошибка при проверке уведомления: {e}")
            return False
    
    async def get_notification_message(self, notification_key: str, **kwargs) -> Optional[str]:
        """Получает текст сообщения уведомления с подстановкой переменных"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT message_text FROM notification_settings
                        WHERE notification_key = %s AND is_enabled = TRUE
                    """, (notification_key,))
                    result = await cursor.fetchone()
                    
                    if result and result['message_text']:
                        message = result['message_text']
                        # Подставляем переменные (например, {phone_number})
                        try:
                            return message.format(**kwargs)
                        except KeyError:
                            # Если переменной нет в kwargs, возвращаем как есть
                            return message
                    return None
        except Exception as e:
            print(f"Ошибка при получении сообщения уведомления: {e}")
            return None
    
    # Методы для работы с балансом
    async def get_user_balance(self, user_id: int) -> float:
        """Получает баланс пользователя"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT balance FROM users WHERE user_id = %s
                    """, (user_id,))
                    result = await cursor.fetchone()
                    return float(result[0]) if result and result[0] is not None else 0.00
        except Exception as e:
            print(f"Ошибка при получении баланса: {e}")
            return 0.00
    
    async def add_to_user_balance(self, user_id: int, amount: float) -> bool:
        """Начисляет сумму на баланс пользователя"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE users
                        SET balance = balance + %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s
                    """, (amount, user_id))
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при начислении баланса: {e}")
            return False
    
    async def update_tariff_payout(self, tariff_id: int, payout_amount: float) -> bool:
        """Обновляет сумму выплаты для тарифа"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE tariffs
                        SET payout_amount = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (payout_amount, tariff_id))
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при обновлении суммы выплаты тарифа: {e}")
            return False
    
    async def get_tariff_by_name(self, tariff_name: str) -> Optional[Dict[str, Any]]:
        """Получает тариф по имени"""
        try:
            import json
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM tariffs WHERE name = %s LIMIT 1
                    """, (tariff_name,))
                    result = await cursor.fetchone()
                    if result:
                        prices = result.get('prices')
                        if prices is not None:
                            if isinstance(prices, dict):
                                result['prices'] = prices
                            elif isinstance(prices, str):
                                try:
                                    result['prices'] = json.loads(prices)
                                except:
                                    result['prices'] = {}
                    return result
        except Exception as e:
            print(f"Ошибка при получении тарифа: {e}")
            return None
    
    # Методы для работы с системными настройками
    async def get_system_setting(self, setting_key: str, default_value: str = None) -> str:
        """Получает значение системной настройки"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT setting_value FROM system_settings WHERE setting_key = %s
                    """, (setting_key,))
                    result = await cursor.fetchone()
                    return result[0] if result and result[0] else (default_value or '0')
        except Exception as e:
            print(f"Ошибка при получении настройки: {e}")
            return default_value or '0'
    
    async def set_system_setting(self, setting_key: str, setting_value: str) -> bool:
        """Устанавливает значение системной настройки"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO system_settings (setting_key, setting_value)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE setting_value = %s, updated_at = CURRENT_TIMESTAMP
                    """, (setting_key, setting_value, setting_value))
                    return True
        except Exception as e:
            print(f"Ошибка при установке настройки: {e}")
            return False
    
    async def get_user_phones_in_queue_count(self, user_id: int) -> int:
        """Получает количество номеров пользователя в очереди (status = 'waiting')"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT COUNT(*) FROM phone_numbers
                        WHERE user_id = %s AND status = 'waiting'
                    """, (user_id,))
                    result = await cursor.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            print(f"Ошибка при подсчете номеров в очереди: {e}")
            return 0
    
    async def delete_outdated_phone_numbers(self, minutes: int) -> int:
        """Удаляет номера из очереди, которые добавлены более N минут назад"""
        try:
            if minutes <= 0:
                return 0
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        DELETE FROM phone_numbers
                        WHERE status = 'waiting'
                        AND created_at < DATE_SUB(NOW(), INTERVAL %s MINUTE)
                    """, (minutes,))
                    deleted_count = cursor.rowcount
                    return deleted_count
        except Exception as e:
            print(f"Ошибка при удалении устаревших номеров: {e}")
            return 0
    
    # Методы для работы с проверками активности
    async def get_users_with_waiting_numbers(self) -> List[Dict[str, Any]]:
        """Получает список уникальных пользователей, у которых есть номера в очереди"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT DISTINCT user_id 
                        FROM phone_numbers 
                        WHERE status = 'waiting'
                    """)
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении пользователей с номерами в очереди: {e}")
            return []
    
    async def get_user_waiting_numbers(self, user_id: int) -> List[Dict[str, Any]]:
        """Получает номера пользователя в очереди"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM phone_numbers 
                        WHERE user_id = %s AND status = 'waiting'
                    """, (user_id,))
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении номеров пользователя: {e}")
            return []
    
    async def create_activity_check(self, user_id: int, phone_number_id: int, message_id: int, timeout_minutes: int) -> int:
        """Создает запись о проверке активности"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO activity_checks (user_id, phone_number_id, message_id, will_delete_at)
                        VALUES (%s, %s, %s, DATE_ADD(NOW(), INTERVAL %s MINUTE))
                    """, (user_id, phone_number_id, message_id, timeout_minutes))
                    return cursor.lastrowid
        except Exception as e:
            print(f"Ошибка при создании проверки активности: {e}")
            return 0
    
    async def mark_activity_check_responded(self, user_id: int, message_id: int) -> bool:
        """Отмечает все проверки активности пользователя для данного сообщения как отвеченные"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE activity_checks
                        SET is_responded = TRUE,
                            responded_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND message_id = %s
                        AND is_responded = FALSE
                    """, (user_id, message_id))
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при отметке проверки: {e}")
            return False
    
    async def get_unresponded_activity_checks(self) -> List[Dict[str, Any]]:
        """Получает все неотвеченные проверки активности, у которых истекло время"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM activity_checks
                        WHERE is_responded = FALSE
                        AND will_delete_at < NOW()
                    """)
                    result = await cursor.fetchall()
                    return result
        except Exception as e:
            print(f"Ошибка при получении неотвеченных проверок: {e}")
            return []
    
    async def has_active_check(self, user_id: int) -> bool:
        """Проверяет, есть ли у пользователя активная (не истекшая) проверка"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        SELECT COUNT(*) FROM activity_checks
                        WHERE user_id = %s
                        AND is_responded = FALSE
                        AND will_delete_at > NOW()
                    """, (user_id,))
                    result = await cursor.fetchone()
                    return result[0] > 0 if result else False
        except Exception as e:
            print(f"Ошибка при проверке активной проверки: {e}")
            return False
    
    async def delete_activity_checks_by_phone(self, phone_number_id: int) -> bool:
        """Удаляет все проверки активности для указанного номера"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        DELETE FROM activity_checks
                        WHERE phone_number_id = %s
                    """, (phone_number_id,))
                    return True
        except Exception as e:
            print(f"Ошибка при удалении проверок: {e}")
            return False
    
    async def delete_phone_number_by_id(self, phone_id: int) -> bool:
        """Удаляет номер из очереди по ID"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        DELETE FROM phone_numbers
                        WHERE id = %s
                    """, (phone_id,))
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при удалении номера: {e}")
            return False
    
    # Методы для работы с выплатами
    async def create_withdrawal(self, user_id: int, amount: float, status: str = 'pending') -> int:
        """Создает запрос на вывод средств"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO withdrawals (user_id, amount, status)
                        VALUES (%s, %s, %s)
                    """, (user_id, amount, status))
                    return cursor.lastrowid
        except Exception as e:
            print(f"Ошибка при создании запроса на вывод: {e}")
            return 0
    
    async def update_withdrawal(self, withdrawal_id: int, status: str, 
                                check_id: str = None, check_url: str = None,
                                admin_id: int = None, admin_comment: str = None) -> bool:
        """Обновляет статус выплаты"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    updates = []
                    params = []
                    
                    if status:
                        updates.append("status = %s")
                        params.append(status)
                    if check_id:
                        updates.append("check_id = %s")
                        params.append(check_id)
                    if check_url:
                        updates.append("check_url = %s")
                        params.append(check_url)
                    if admin_id:
                        updates.append("admin_id = %s")
                        params.append(admin_id)
                    if admin_comment:
                        updates.append("admin_comment = %s")
                        params.append(admin_comment)
                    
                    if status in ['approved', 'completed']:
                        updates.append("processed_at = CURRENT_TIMESTAMP")
                    
                    params.append(withdrawal_id)
                    
                    query = f"""
                        UPDATE withdrawals
                        SET {', '.join(updates)}
                        WHERE id = %s
                    """
                    await cursor.execute(query, params)
                    return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при обновлении выплаты: {e}")
            return False
    
    async def get_withdrawals(self, user_id: int = None, status: str = None, 
                             limit: int = 50) -> List[Dict[str, Any]]:
        """Получает историю выплат"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    where_clauses = []
                    params = []
                    
                    if user_id:
                        where_clauses.append("user_id = %s")
                        params.append(user_id)
                    if status:
                        where_clauses.append("status = %s")
                        params.append(status)
                    
                    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                    params.append(limit)
                    
                    await cursor.execute(f"""
                        SELECT * FROM withdrawals
                        {where_sql}
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, params)
                    return await cursor.fetchall()
        except Exception as e:
            print(f"Ошибка при получении выплат: {e}")
            return []
    
    async def get_pending_withdrawals(self) -> List[Dict[str, Any]]:
        """Получает ожидающие подтверждения выплаты"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT w.*, u.fullname, u.username
                        FROM withdrawals w
                        LEFT JOIN users u ON w.user_id = u.user_id
                        WHERE w.status = 'pending'
                        ORDER BY w.created_at ASC
                    """)
                    return await cursor.fetchall()
        except Exception as e:
            print(f"Ошибка при получении ожидающих выплат: {e}")
            return []
    
    async def get_withdrawal_by_id(self, withdrawal_id: int) -> Optional[Dict[str, Any]]:
        """Получает выплату по ID"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("""
                        SELECT * FROM withdrawals WHERE id = %s
                    """, (withdrawal_id,))
                    return await cursor.fetchone()
        except Exception as e:
            print(f"Ошибка при получении выплаты: {e}")
            return None

# Глобальный экземпляр базы данных
db = Database()
