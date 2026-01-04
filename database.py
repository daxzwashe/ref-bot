import aiosqlite
import os
import logging
from datetime import datetime
from typing import Optional, List, Dict, Tuple

# Логгер для database
logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = "bot.db"):
        self.db_path = db_path

    async def init_db(self):
        """Инициализация базы данных и создание таблиц"""
        logger.info(f"🔧 Инициализация БД по пути: {self.db_path}")
        
        async with aiosqlite.connect(self.db_path) as db:
            # Таблица пользователей
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_subscribed INTEGER DEFAULT 0,
                    partner_code TEXT,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ref_partner_code TEXT
                )
            """)
            logger.debug("✅ Таблица users создана/проверена")
            
            # Таблица партнеров
            await db.execute("""
                CREATE TABLE IF NOT EXISTS partners (
                    partner_code TEXT PRIMARY KEY,
                    username TEXT,
                    user_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.debug("✅ Таблица partners создана/проверена")
            
            # Таблица покупок
            await db.execute("""
                CREATE TABLE IF NOT EXISTS purchases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount REAL,
                    comment TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            logger.debug("✅ Таблица purchases создана/проверена")
            
            # Добавляем столбец comment если его нет
            try:
                cursor = await db.execute("PRAGMA table_info(purchases)")
                columns = await cursor.fetchall()
                column_names = [col[1] for col in columns]
                if 'comment' not in column_names:
                    await db.execute("ALTER TABLE purchases ADD COLUMN comment TEXT")
                    logger.info("✅ Столбец comment добавлен в таблицу purchases")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при проверке столбца comment: {e}")
            
            await db.commit()
        
        logger.info("✅ БД полностью инициализирована")

    async def add_user(self, user_id: int, username: Optional[str], first_name: Optional[str], 
                      last_name: Optional[str], ref_partner_code: Optional[str] = None):
        """Добавить пользователя"""
        async with aiosqlite.connect(self.db_path) as db:
            # Проверяем, существует ли пользователь
            cursor = await db.execute(
                "SELECT user_id FROM users WHERE user_id = ?", (user_id,)
            )
            exists = await cursor.fetchone()
            
            if not exists:
                await db.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name, ref_partner_code)
                    VALUES (?, ?, ?, ?, ?)
                """, (user_id, username, first_name, last_name, ref_partner_code))
                await db.commit()
                logger.info(f"📝 Добавлен новый пользователь: ID={user_id}, @{username}, реф={ref_partner_code}")
            else:
                logger.debug(f"ℹ️ Пользователь {user_id} уже существует")

    async def update_subscription(self, user_id: int, is_subscribed: bool):
        """Обновить статус подписки"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET is_subscribed = ? WHERE user_id = ?",
                (1 if is_subscribed else 0, user_id)
            )
            await db.commit()
        
        status_text = "✅ подписан" if is_subscribed else "❌ не подписан"
        logger.debug(f"📢 Обновлен статус подписки пользователя {user_id}: {status_text}")

    async def is_subscribed(self, user_id: int) -> bool:
        """Проверить подписку пользователя"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT is_subscribed FROM users WHERE user_id = ?", (user_id,)
            )
            result = await cursor.fetchone()
            return result[0] == 1 if result else False

    async def add_partner(self, partner_code: str, username: str, user_id: int) -> bool:
        """Добавить партнера"""
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute("""
                    INSERT INTO partners (partner_code, username, user_id)
                    VALUES (?, ?, ?)
                """, (partner_code, username, user_id))
                await db.commit()
                logger.info(f"✅ Добавлен партнер: @{username}, код={partner_code}, ID={user_id}")
                return True
            except aiosqlite.IntegrityError as e:
                logger.warning(f"⚠️ Ошибка добавления партнера @{username}: партнер с таким кодом уже существует")
                return False
            except Exception as e:
                logger.error(f"❌ Ошибка добавления партнера @{username}: {e}")
                return False

    async def remove_partner(self, username: str) -> bool:
        """Удалить партнера"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT partner_code FROM partners WHERE username = ?", (username,)
            )
            result = await cursor.fetchone()
            if result:
                partner_code = result[0]
                await db.execute("DELETE FROM partners WHERE username = ?", (username,))
                await db.commit()
                logger.info(f"✅ Удален партнер: @{username}, код={partner_code}")
                return True
            
            logger.warning(f"⚠️ Партнер @{username} не найден")
            return False

    async def get_partner_by_code(self, partner_code: str) -> Optional[Dict]:
        """Получить партнера по коду"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT * FROM partners WHERE partner_code = ?", (partner_code,)
            )
            row = await cursor.fetchone()
            if row:
                return {
                    'partner_code': row[0],
                    'username': row[1],
                    'user_id': row[2],
                    'created_at': row[3]
                }
            return None

    async def get_partner_by_username(self, username: str) -> Optional[Dict]:
        """Получить партнера по username"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT * FROM partners WHERE username = ?", (username,)
            )
            row = await cursor.fetchone()
            if row:
                return {
                    'partner_code': row[0],
                    'username': row[1],
                    'user_id': row[2],
                    'created_at': row[3]
                }
            return None

    async def get_all_partners(self) -> List[Dict]:
        """Получить всех партнеров"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT * FROM partners")
            rows = await cursor.fetchall()
            return [
                {
                    'partner_code': row[0],
                    'username': row[1],
                    'user_id': row[2],
                    'created_at': row[3]
                }
                for row in rows
            ]

    async def get_users_by_ref(self, partner_code: str, limit: int = 15, offset: int = 0) -> List[Dict]:
        """Получить пользователей по реферальному коду с пагинацией"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT u.user_id, u.username, u.first_name, u.last_name, u.registered_at, p.username as partner_username
                FROM users u
                LEFT JOIN partners p ON u.ref_partner_code = p.partner_code
                WHERE u.ref_partner_code = ?
                ORDER BY u.registered_at DESC
                LIMIT ? OFFSET ?
            """, (partner_code, limit, offset))
            rows = await cursor.fetchall()
            return [
                {
                    'user_id': row[0],
                    'username': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'registered_at': row[4],
                    'partner_username': row[5]
                }
                for row in rows
            ]

    async def get_all_users(self, limit: int = 15, offset: int = 0) -> Tuple[List[Dict], int]:
        """Получить всех пользователей с пагинацией"""
        async with aiosqlite.connect(self.db_path) as db:
            # Получаем общее количество
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total = (await cursor.fetchone())[0]
            
            # Получаем пользователей
            cursor = await db.execute("""
                SELECT u.user_id, u.username, u.first_name, u.last_name, u.registered_at, u.is_subscribed, p.username as partner_username, u.ref_partner_code
                FROM users u
                LEFT JOIN partners p ON u.ref_partner_code = p.partner_code
                ORDER BY u.registered_at DESC
                LIMIT ? OFFSET ?
            """, (limit, offset))
            rows = await cursor.fetchall()
            users = [
                {
                    'user_id': row[0],
                    'username': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'registered_at': row[4],
                    'is_subscribed': row[5],
                    'partner_username': row[6],
                    'ref_partner_code': row[7]
                }
                for row in rows
            ]
            return users, total

    async def search_users(self, query: str) -> List[Dict]:
        """Поиск пользователей по username или user_id"""
        async with aiosqlite.connect(self.db_path) as db:
            # Очищаем query от @ если есть
            clean_query = query.replace("@", "").strip()
            
            # Пытаемся найти по user_id
            try:
                user_id = int(clean_query)
                cursor = await db.execute("""
                    SELECT u.user_id, u.username, u.first_name, u.last_name, u.registered_at, u.is_subscribed, p.username as partner_username, u.ref_partner_code
                    FROM users u
                    LEFT JOIN partners p ON u.ref_partner_code = p.partner_code
                    WHERE u.user_id = ?
                """, (user_id,))
            except ValueError:
                # Поиск по username (с и без @), имени или фамилии
                cursor = await db.execute("""
                    SELECT u.user_id, u.username, u.first_name, u.last_name, u.registered_at, u.is_subscribed, p.username as partner_username, u.ref_partner_code
                    FROM users u
                    LEFT JOIN partners p ON u.ref_partner_code = p.partner_code
                    WHERE u.username LIKE ? OR u.first_name LIKE ? OR u.last_name LIKE ?
                    ORDER BY u.first_name, u.username
                """, (f"%{clean_query}%", f"%{clean_query}%", f"%{clean_query}%"))
            
            rows = await cursor.fetchall()
            return [
                {
                    'user_id': row[0],
                    'username': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'registered_at': row[4],
                    'is_subscribed': row[5],
                    'partner_username': row[6],
                    'ref_partner_code': row[7]
                }
                for row in rows
            ]

    async def get_partner_stats(self, partner_code: str) -> Dict:
        """Получить статистику партнера"""
        async with aiosqlite.connect(self.db_path) as db:
            # За сегодня
            cursor = await db.execute("""
                SELECT COUNT(*) FROM users 
                WHERE ref_partner_code = ? 
                AND registered_at >= date('now')
            """, (partner_code,))
            today = (await cursor.fetchone())[0]
            
            # За неделю
            cursor = await db.execute("""
                SELECT COUNT(*) FROM users 
                WHERE ref_partner_code = ? 
                AND registered_at >= datetime('now', '-7 days')
            """, (partner_code,))
            week = (await cursor.fetchone())[0]
            
            # За месяц
            cursor = await db.execute("""
                SELECT COUNT(*) FROM users 
                WHERE ref_partner_code = ? 
                AND registered_at >= datetime('now', '-30 days')
            """, (partner_code,))
            month = (await cursor.fetchone())[0]
            
            # Все время
            cursor = await db.execute(
                "SELECT COUNT(*) FROM users WHERE ref_partner_code = ?", (partner_code,)
            )
            total = (await cursor.fetchone())[0]
            
            return {
                'today': today,
                'week': week,
                'month': month,
                'total': total
            }

    async def get_user_partner_code(self, user_id: int) -> Optional[str]:
        """Получить код партнера пользователя"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT partner_code FROM partners WHERE user_id = ?", (user_id,)
            )
            result = await cursor.fetchone()
            return result[0] if result else None

    async def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Получить пользователя по username"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT user_id, username, first_name, last_name FROM users WHERE username = ?",
                (username,)
            )
            row = await cursor.fetchone()
            if row:
                return {
                    'user_id': row[0],
                    'username': row[1],
                    'first_name': row[2],
                    'last_name': row[3]
                }
            return None

    async def update_partner_user_id(self, username: str, user_id: int):
        """Обновить user_id партнера по username"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE partners SET user_id = ? WHERE username = ? AND (user_id = 0 OR user_id IS NULL)",
                (user_id, username)
            )
            await db.commit()

    async def add_purchase(self, user_id: int, amount: float, comment: str = "") -> bool:
        """Добавить покупку пользователю"""
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute("""
                    INSERT INTO purchases (user_id, amount, comment)
                    VALUES (?, ?, ?)
                """, (user_id, amount, comment))
                await db.commit()
                logger.info(f"💰 Добавлена покупка: user_id={user_id}, сумма={amount}, комментарий='{comment}'")
                return True
            except Exception as e:
                logger.error(f"❌ Ошибка добавления покупки для user_id={user_id}: {e}")
                return False

    async def get_all_purchases(self, limit: int = 15, offset: int = 0) -> Tuple[List[Dict], int]:
        """Получить все покупки с пагинацией"""
        async with aiosqlite.connect(self.db_path) as db:
            # Получаем общее количество
            cursor = await db.execute("SELECT COUNT(*) FROM purchases")
            total = (await cursor.fetchone())[0]
            
            # Получаем покупки
            cursor = await db.execute("""
                SELECT p.id, p.user_id, p.amount, p.comment, p.created_at, 
                       u.username, u.first_name, u.last_name, u.ref_partner_code, pt.username as partner_username
                FROM purchases p
                LEFT JOIN users u ON p.user_id = u.user_id
                LEFT JOIN partners pt ON u.ref_partner_code = pt.partner_code
                ORDER BY p.created_at DESC
                LIMIT ? OFFSET ?
            """, (limit, offset))
            rows = await cursor.fetchall()
            purchases = [
                {
                    'id': row[0],
                    'user_id': row[1],
                    'amount': row[2],
                    'comment': row[3],
                    'created_at': row[4],
                    'username': row[5],
                    'first_name': row[6],
                    'last_name': row[7],
                    'ref_partner_code': row[8],
                    'partner_username': row[9]
                }
                for row in rows
            ]
            return purchases, total

    async def get_user_by_id_or_username(self, query: str) -> Optional[Dict]:
        """Получить пользователя по ID или username"""
        try:
            user_id = int(query)
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    "SELECT user_id, username, first_name, last_name FROM users WHERE user_id = ?",
                    (user_id,)
                )
                row = await cursor.fetchone()
                if row:
                    return {
                        'user_id': row[0],
                        'username': row[1],
                        'first_name': row[2],
                        'last_name': row[3]
                    }
        except ValueError:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    "SELECT user_id, username, first_name, last_name FROM users WHERE username = ?",
                    (query.replace("@", ""),)
                )
                row = await cursor.fetchone()
                if row:
                    return {
                        'user_id': row[0],
                        'username': row[1],
                        'first_name': row[2],
                        'last_name': row[3]
                    }
        return None

    async def get_purchases_by_ref(self, partner_code: str) -> List[Dict]:
        """Получить все покупки пользователей по реферальному коду"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT p.id, p.user_id, p.amount, p.comment, p.created_at, 
                       u.username, u.first_name, u.last_name
                FROM purchases p
                JOIN users u ON p.user_id = u.user_id
                WHERE u.ref_partner_code = ?
                ORDER BY p.created_at DESC
            """, (partner_code,))
            rows = await cursor.fetchall()
            return [
                {
                    'id': row[0],
                    'user_id': row[1],
                    'amount': row[2],
                    'comment': row[3],
                    'created_at': row[4],
                    'username': row[5],
                    'first_name': row[6],
                    'last_name': row[7]
                }
                for row in rows
            ]

