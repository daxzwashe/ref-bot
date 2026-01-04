import asyncio
import logging
import logging.handlers
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
from aiogram.exceptions import TelegramUnauthorizedError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import config
from database import Database
import secrets
import string
from datetime import datetime

# Настройка логирования с ротацией файлов
log_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Логирование в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_formatter)

# Логирование в файл с ротацией
file_handler = logging.handlers.RotatingFileHandler(
    'bot.log',
    maxBytes=10485760,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(log_formatter)

# Основной логгер
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Инициализация бота и диспетчера
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
db = Database(config.DATABASE_PATH)

# Состояния для FSM
class PartnerStates(StatesGroup):
    waiting_username = State()
    waiting_delete_username = State()

class UserSearchStates(StatesGroup):
    waiting_query = State()

class PurchaseStates(StatesGroup):
    waiting_user_input = State()
    waiting_amount = State()
    waiting_comment = State()


def generate_partner_code() -> str:
    """Генерация уникального кода партнера"""
    return ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))


def is_admin(user_id: int) -> bool:
    """Проверка, является ли пользователь админом"""
    return user_id in config.ADMIN_IDS


async def check_subscription(user_id: int) -> bool:
    """Проверка подписки пользователя на канал"""
    try:
        member = await bot.get_chat_member(config.CHANNEL_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.error(f"Ошибка проверки подписки: {type(e).__name__} - {e}")
        return False


def get_subscription_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для подписки"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/{config.CHANNEL_ID.replace('@', '')}")],
        [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription")]
    ])
    return keyboard


@dp.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    
    logger.info(f"🔵 /start - Пользователь: {user_id} | Username: @{username} | Имя: {first_name} {last_name}")
    
    # Получаем реферальный код из аргументов
    ref_code = None
    if len(message.text.split()) > 1:
        ref_arg = message.text.split()[1]
        if ref_arg.startswith("ref_"):
            ref_code = ref_arg.replace("ref_", "")
            # Проверяем, существует ли такой партнер
            partner = await db.get_partner_by_code(ref_code)
            if not partner:
                logger.warning(f"⚠️ Неверный код реферала: {ref_code}")
                ref_code = None
            else:
                logger.info(f"✅ Найден партнер по коду {ref_code}: @{partner['username']}")
    
    # Добавляем пользователя в БД
    await db.add_user(user_id, username, first_name, last_name, ref_code)
    logger.info(f"📝 Пользователь {user_id} добавлен в БД")
    
    # Если пользователь является партнером, обновляем его user_id
    if username:
        await db.update_partner_user_id(username, user_id)
        logger.debug(f"🔄 Обновлен user_id для партнера @{username}")
    
    # Проверяем подписку
    is_sub = await check_subscription(user_id)
    await db.update_subscription(user_id, is_sub)
    logger.info(f"📢 Статус подписки {user_id}: {'✅ Подписан' if is_sub else '❌ Не подписан'}")
    
    if is_sub:
        await message.answer(
            "✅ Вы успешно подписаны на канал!\n\n"
            "👋 Добро пожаловать в бота!\n\n"
            f"👤 ID: {user_id}\n"
            f"📱 Username: @{username}\n"
            f"📛 Имя: {first_name} {last_name or ''}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[])
        )
        logger.info(f"✅ Проверка подписки пройдена для {user_id}")
    else:
        await message.answer(
            "Привет! 👋\n\n"
            "Чтобы попасть внутрь и получить все материалы — подпишись на канал 👇\n\n"
            "Там ты найдёшь:\n"
            "📉 похудение без откатов (кето / низкоуглеводка)\n"
            "⚡️ больше энергии и ясная голова каждый день\n"
            "  🧬 оздоровление организма: снижение инсулина, воспалений и метаболической нагрузки\n"
            "🍽️ схемы питания и “тарелки” — что есть, сколько и из чего собирать\n"
            "🔥 чёткий план действий: как зайти, как удержать результат, что делать при тяге\n\n"
            "✅ Жми кнопку «Подписаться на канал»",
            reply_markup=get_subscription_keyboard()
        )
        logger.info(f"⚠️ Требуется подписка для {user_id}")


@dp.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: CallbackQuery):
    """Проверка подписки по кнопке"""
    user_id = callback.from_user.id
    logger.info(f"🔄 Проверка подписки для {user_id}")
    
    is_sub = await check_subscription(user_id)
    await db.update_subscription(user_id, is_sub)
    
    if is_sub:
        logger.info(f"✅ Подписка подтверждена для {user_id}")
        await callback.message.edit_text(
            "✅ Вы успешно подписаны на канал!\n\n"
            "Добро пожаловать в боту!"
        )
        await callback.answer("✅ Подписка подтверждена!")
    else:
        logger.warning(f"❌ Пользователь {user_id} не подписан")
        await callback.answer(
            "❌ Вы еще не подписались на канал. Пожалуйста, подпишитесь и попробуйте снова.",
            show_alert=True
        )


@dp.message(Command("ref"))
async def cmd_ref(message: Message):
    """Личный кабинет реферала"""
    user_id = message.from_user.id
    username = message.from_user.username
    logger.info(f"🔵 /ref - Пользователь: {user_id} | @{username}")
    
    # Проверяем подписку
    is_sub = await db.is_subscribed(user_id)
    if not is_sub:
        logger.warning(f"❌ Пользователь {user_id} не подписан, /ref недоступна")
        await message.answer(
            "Для использования бота необходимо подписаться на канал.",
            reply_markup=get_subscription_keyboard()
        )
        return
    
    # Проверяем, является ли пользователь партнером
    partner_code = await db.get_user_partner_code(user_id)
    if not partner_code:
        logger.info(f"ℹ️ Пользователь {user_id} не является партнером")
        await message.answer("❌ Вы не являетесь партнером.")
        return
    
    logger.info(f"✅ Открыт личный кабинет для партнера {user_id} | код: {partner_code}")
    
    username = message.from_user.username or "Без username"
    bot_username = (await bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{partner_code}"
    
    # Получаем статистику
    stats = await db.get_partner_stats(partner_code)
    logger.debug(f"📊 Статистика партнера {partner_code}: {stats}")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Аналитика", callback_data=f"ref_stats_{partner_code}")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data=f"ref_users_{partner_code}")]
    ])
    
    stats_text = (
        f"👤 Личный кабинет реферала\n\n"
        f"{'='*40}\n"
        f"Username: @{username}\n"
        f"ID: {user_id}\n"
        f"Код партнера: {partner_code}\n"
        f"{'='*40}\n\n"
        f"🔗 Реферальная ссылка:\n{ref_link}\n\n"
        f"{'='*40}\n"
        f"📊 СТАТИСТИКА:\n"
        f"{'='*40}\n"
        f"🆕 За сегодня: {stats['today']}\n"
        f"📅 За неделю: {stats['week']}\n"
        f"📊 За месяц: {stats['month']}\n"
        f"👥 За все время: {stats['total']}\n"
        f"{'='*40}"
    )
    
    await message.answer(stats_text, reply_markup=keyboard)


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    """Админ панель"""
    user_id = message.from_user.id
    logger.info(f"🔵 /admin - Попытка доступа от пользователя: {user_id}")
    
    if not is_admin(user_id):
        logger.warning(f"🚫 Запрещен доступ в админ панель для пользователя {user_id}")
        await message.answer("❌ У вас нет доступа к админ панели.")
        return
    
    logger.info(f"✅ Открыта админ панель для {user_id}")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Партнеры", callback_data="admin_partners")],
        [InlineKeyboardButton(text="👤 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="💰 Покупки", callback_data="admin_purchases")]
    ])
    
    admin_info = (
        "🔐 АДМИН ПАНЕЛЬ\n\n"
        f"Admin ID: {user_id}\n"
        f"Время входа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        "Выберите раздел:"
    )
    
    await message.answer(admin_info, reply_markup=keyboard)


@dp.callback_query(F.data == "admin_partners")
async def admin_partners_menu(callback: CallbackQuery):
    """Меню управления партнерами"""
    user_id = callback.from_user.id
    logger.debug(f"👥 Партнеры - доступ пользователя {user_id}")
    
    if not is_admin(user_id):
        logger.warning(f"🚫 Запрещен доступ к партнерам для {user_id}")
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Все партнеры", callback_data="partners_list")],
        [InlineKeyboardButton(text="➕ Добавить партнера", callback_data="partner_add")],
        [InlineKeyboardButton(text="➖ Удалить партнера", callback_data="partner_delete")],
        [InlineKeyboardButton(text="📊 Аналитика партнеров", callback_data="partners_analytics")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]
    ])
    
    await callback.message.edit_text(
        "👥 УПРАВЛЕНИЕ ПАРТНЕРАМИ\n\n"
        "Выберите действие:",
        reply_markup=keyboard
    )
    await callback.answer()


@dp.callback_query(F.data == "partners_list")
async def partners_list(callback: CallbackQuery):
    """Список всех партнеров"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    partners = await db.get_all_partners()
    logger.info(f"📋 Просмотр списка партнеров: найдено {len(partners)}")
    
    bot_username = (await bot.get_me()).username
    
    if not partners:
        text = "📋 Список партнеров пуст."
        logger.info("⚠️ Партнеров не найдено")
    else:
        text = f"📋 ВСЕ ПАРТНЕРЫ ({len(partners)} шт.)\n\n"
        text += "="*40 + "\n\n"
        for i, partner in enumerate(partners, 1):
            ref_link = f"https://t.me/{bot_username}?start=ref_{partner['partner_code']}"
            text += f"{i}. @{partner['username']}\n"
            text += f"   ID: {partner['user_id']}\n"
            text += f"   Код: {partner['partner_code']}\n"
            text += f"   Ссылка: {ref_link}\n"
            text += f"   Добавлен: {partner['created_at']}\n"
            text += "\n"
        text += "="*40
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_partners")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data == "partner_add")
async def partner_add_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления партнера"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    logger.info(f"➕ Начало добавления партнера (admin: {callback.from_user.id})")
    
    await callback.message.edit_text(
        "➕ ДОБАВЛЕНИЕ ПАРТНЕРА\n\n"
        "Введите username партнера (без @):"
    )
    await state.set_state(PartnerStates.waiting_username)
    await callback.answer()


@dp.message(PartnerStates.waiting_username)
async def partner_add_process(message: Message, state: FSMContext):
    """Обработка добавления партнера"""
    if not is_admin(message.from_user.id):
        logger.warning(f"🚫 Неавторизованная попытка добавления партнера от {message.from_user.id}")
        await message.answer("❌ Нет доступа")
        await state.clear()
        return
    
    username = message.text.strip().replace("@", "")
    logger.info(f"➕ Попытка добавления партнера: @{username}")
    
    # Проверяем, не является ли уже партнером
    existing = await db.get_partner_by_username(username)
    if existing:
        logger.warning(f"⚠️ Пользователь @{username} уже является партнером")
        await message.answer("❌ Этот пользователь уже является партнером.")
        await state.clear()
        return
    
    # Пытаемся найти пользователя в БД по username
    user = await db.get_user_by_username(username)
    user_id = user['user_id'] if user else 0
    
    # Генерируем код партнера
    partner_code = generate_partner_code()
    
    try:
        success = await db.add_partner(partner_code, username, user_id)
        if success:
            logger.info(f"✅ Партнер @{username} успешно добавлен с кодом: {partner_code}")
            bot_username = (await bot.get_me()).username
            ref_link = f"https://t.me/{bot_username}?start=ref_{partner_code}"
            await message.answer(
                f"✅ ПАРТНЕР ДОБАВЛЕН\n\n"
                f"Username: @{username}\n"
                f"User ID: {user_id}\n"
                f"Код партнера: {partner_code}\n\n"
                f"🔗 Реферальная ссылка:\n{ref_link}"
            )
        else:
            logger.error(f"❌ Ошибка при добавлении партнера @{username}")
            await message.answer("❌ Ошибка при добавлении партнера.")
    except Exception as e:
        logger.error(f"❌ Исключение при добавлении партнера: {e}")
        await message.answer("❌ Произошла ошибка при добавлении партнера.")
    
    await state.clear()


@dp.callback_query(F.data == "partner_delete")
async def partner_delete_start(callback: CallbackQuery, state: FSMContext):
    """Начало удаления партнера"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    logger.info(f"➖ Начало удаления партнера (admin: {callback.from_user.id})")
    
    await callback.message.edit_text(
        "➖ УДАЛЕНИЕ ПАРТНЕРА\n\n"
        "Введите username партнера для удаления (без @):"
    )
    await state.set_state(PartnerStates.waiting_delete_username)
    await callback.answer()


@dp.message(PartnerStates.waiting_delete_username)
async def partner_delete_process(message: Message, state: FSMContext):
    """Обработка удаления партнера"""
    if not is_admin(message.from_user.id):
        logger.warning(f"🚫 Неавторизованная попытка удаления партнера от {message.from_user.id}")
        await message.answer("❌ Нет доступа")
        await state.clear()
        return
    
    username = message.text.strip().replace("@", "")
    logger.info(f"➖ Попытка удаления партнера: @{username}")
    
    success = await db.remove_partner(username)
    
    if success:
        logger.info(f"✅ Партнер @{username} удален")
        await message.answer(f"✅ Партнер @{username} успешно удален!")
    else:
        logger.warning(f"❌ Партнер @{username} не найден")
        await message.answer(f"❌ Пользователь @{username} не является партнером.")
    
    await state.clear()


@dp.callback_query(F.data == "partners_analytics")
async def partners_analytics_menu(callback: CallbackQuery):
    """Меню аналитики партнеров"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    partners = await db.get_all_partners()
    
    if not partners:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_partners")]
        ])
        await callback.message.edit_text("📊 Нет партнеров для аналитики.", reply_markup=keyboard)
        await callback.answer()
        return
    
    keyboard_buttons = []
    for partner in partners:
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"@{partner['username']}",
                callback_data=f"partner_stats_{partner['partner_code']}"
            )
        ])
    keyboard_buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_partners")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    await callback.message.edit_text("📊 Выберите партнера для просмотра аналитики:", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data.startswith("partner_stats_"))
async def partner_stats_detail(callback: CallbackQuery):
    """Детальная статистика партнера"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    partner_code = callback.data.replace("partner_stats_", "")
    partner = await db.get_partner_by_code(partner_code)
    
    if not partner:
        await callback.answer("❌ Партнер не найден", show_alert=True)
        return
    
    stats = await db.get_partner_stats(partner_code)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="partners_analytics")]
    ])
    
    await callback.message.edit_text(
        f"📊 Аналитика партнера @{partner['username']}\n\n"
        f"📈 Статистика:\n"
        f"🆕 За сегодня: {stats['today']}\n"
        f"📅 За неделю: {stats['week']}\n"
        f"📊 За месяц: {stats['month']}\n"
        f"👥 За все время: {stats['total']}",
        reply_markup=keyboard
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_users")
async def admin_users_menu(callback: CallbackQuery):
    """Меню управления пользователями"""
    if not is_admin(callback.from_user.id):
        logger.warning(f"🚫 Запрещен доступ к пользователям для {callback.from_user.id}")
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    logger.info(f"👤 Открыто управление пользователями для админа {callback.from_user.id}")
    await show_users_page(callback, 0)


async def show_users_page(callback: CallbackQuery, page: int, search_query: str = None):
    """Показать страницу пользователей"""
    limit = 15
    offset = page * limit
    
    if search_query:
        users = await db.search_users(search_query)
        total = len(users)
        users = users[offset:offset + limit]
        logger.debug(f"🔍 Поиск пользователей по '{search_query}': найдено {total}")
    else:
        users, total = await db.get_all_users(limit, offset)
        logger.debug(f"👤 Страница {page + 1} пользователей: {len(users)} из {total}")
    
    if not users:
        text = "👤 Пользователи не найдены."
        keyboard_buttons = []
    else:
        text = f"👤 ПОЛЬЗОВАТЕЛИ (всего: {total}, страница {page + 1})\n\n"
        text += "="*50 + "\n\n"
        
        for i, user in enumerate(users, 1):
            name = user['first_name'] or ""
            if user['last_name']:
                name += f" {user['last_name']}"
            username = f"@{user['username']}" if user['username'] else "Без username"
            user_id = user['user_id']
            ref_link = user['ref_partner_code'] or "Нет"
            partner_username = f"@{user['partner_username']}" if user['partner_username'] else "Нет"
            sub_status = "✅" if user.get('is_subscribed') else "❌"
            
            text += f"{i}. {name} {sub_status}\n"
            text += f"   ID: {user_id}\n"
            text += f"   Username: {username}\n"
            text += f"   Подписка: {sub_status} {'(подписан)' if user.get('is_subscribed') else '(не подписан)'}\n"
            text += f"   Реф код: {ref_link}\n"
            text += f"   Партнер: {partner_username}\n"
            text += f"   Дата: {user.get('registered_at', 'N/A')}\n\n"
        
        text += "="*50
    
    keyboard_buttons = []
    
    # Кнопки пагинации
    if total > limit:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"users_page_{page - 1}"))
        if (page + 1) * limit < total:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"users_page_{page + 1}"))
        if nav_buttons:
            keyboard_buttons.append(nav_buttons)
    
    # Кнопки управления
    keyboard_buttons.extend([
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="users_search")],
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="admin_menu")]
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    if isinstance(callback, CallbackQuery):
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer()
    else:
        await callback.edit_text(text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("users_page_"))
async def users_page_handler(callback: CallbackQuery):
    """Обработчик пагинации пользователей"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    page = int(callback.data.replace("users_page_", ""))
    await show_users_page(callback, page)
    await callback.answer()


@dp.callback_query(F.data == "users_search")
async def users_search_start(callback: CallbackQuery, state: FSMContext):
    """Начало поиска пользователей"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔍 Поиск пользователей\n\n"
        "Введите username или ID пользователя для поиска:"
    )
    await state.set_state(UserSearchStates.waiting_query)
    await callback.answer()


@dp.message(UserSearchStates.waiting_query)
async def users_search_process(message: Message, state: FSMContext):
    """Обработка поиска пользователей"""
    if not is_admin(message.from_user.id):
        logger.warning(f"🚫 Неавторизованная попытка поиска от {message.from_user.id}")
        await message.answer("❌ Нет доступа")
        await state.clear()
        return
    
    query = message.text.strip()
    # Очищаем запрос от @ если есть
    query_clean = query.replace("@", "").strip()
    logger.info(f"🔍 Поиск пользователей по запросу: '{query}'")
    
    users = await db.search_users(query)
    
    if not users:
        text = f"❌ Пользователи по запросу '{query}' не найдены."
        logger.info(f"❌ Поиск по '{query}' - результатов не найдено")
    else:
        text = f"🔍 РЕЗУЛЬТАТЫ ПОИСКА: '{query}'\n\n"
        text += f"Найдено: {len(users)}\n\n"
        text += "="*50 + "\n\n"
        
        for i, user in enumerate(users[:15], 1):  # Ограничиваем 15 результатами
            name = user['first_name'] or ""
            if user['last_name']:
                name += f" {user['last_name']}"
            username = f"@{user['username']}" if user['username'] else "Без username"
            user_id = user['user_id']
            sub_status = "✅" if user.get('is_subscribed') else "❌"
            partner_username = f"@{user['partner_username']}" if user.get('partner_username') else "Нет"
            
            text += f"{i}. {name} {sub_status}\n"
            text += f"   ID: {user_id}\n"
            text += f"   Username: {username}\n"
            text += f"   Подписка: {sub_status}\n"
            text += f"   Партнер: {partner_username}\n"
            text += f"   Дата: {user.get('registered_at', 'N/A')}\n\n"
        
        text += "="*50
        logger.info(f"✅ Поиск по '{query}' - найдено {len(users)} пользователей")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="users_search")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_users")]
    ])
    
    await message.answer(text, reply_markup=keyboard)
    await state.clear()


@dp.callback_query(F.data == "admin_purchases")
async def admin_purchases(callback: CallbackQuery):
    """Меню покупок"""
    if not is_admin(callback.from_user.id):
        logger.warning(f"🚫 Запрещен доступ к покупкам для {callback.from_user.id}")
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    logger.info(f"💰 Открыто управление покупками для админа {callback.from_user.id}")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Все покупки", callback_data="purchases_list")],
        [InlineKeyboardButton(text="➕ Добавить покупку", callback_data="purchase_add")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]
    ])
    
    await callback.message.edit_text(
        "💰 УПРАВЛЕНИЕ ПОКУПКАМИ\n\n"
        "Выберите действие:",
        reply_markup=keyboard
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_menu")
async def admin_menu_back(callback: CallbackQuery):
    """Возврат в главное меню админки"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Партнеры", callback_data="admin_partners")],
        [InlineKeyboardButton(text="👤 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="💰 Покупки", callback_data="admin_purchases")]
    ])
    
    await callback.message.edit_text("🔐 Админ панель", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data == "purchases_list")
async def purchases_list(callback: CallbackQuery):
    """Список всех покупок"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await show_purchases_page(callback, 0)


async def show_purchases_page(callback: CallbackQuery, page: int):
    """Показать страницу покупок"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    limit = 10
    offset = page * limit
    
    purchases, total = await db.get_all_purchases(limit, offset)
    logger.debug(f"💰 Страница {page + 1} покупок: {len(purchases)} из {total}")
    
    if not purchases:
        text = "💰 Покупок не найдено."
        logger.info("ℹ️ Нет покупок в системе")
        keyboard_buttons = []
    else:
        text = f"💰 ВСЕ ПОКУПКИ (всего: {total}, страница {page + 1})\n\n"
        text += "="*50 + "\n\n"
        
        for i, purchase in enumerate(purchases, 1):
            name = purchase['first_name'] or ""
            if purchase['last_name']:
                name += f" {purchase['last_name']}"
            username = f"@{purchase['username']}" if purchase['username'] else "ID: " + str(purchase['user_id'])
            partner = f"@{purchase['partner_username']}" if purchase['partner_username'] else "Нет"
            
            text += f"{i}. Покупка\n"
            text += f"   Пользователь: {name} ({username})\n"
            text += f"   💰 Сумма: {purchase['amount']} €\n"
            text += f"   📝 Комментарий: {purchase['comment'] or 'Нет'}\n"
            text += f"   🤝 Реф: {partner}\n"
            text += f"   📅 Дата: {purchase['created_at']}\n\n"
        
        text += "="*50
    
    keyboard_buttons = []
    
    # Кнопки пагинации
    if total > limit:
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"purchases_page_{page - 1}"))
        if (page + 1) * limit < total:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"purchases_page_{page + 1}"))
        if nav_buttons:
            keyboard_buttons.append(nav_buttons)
    
    # Кнопки управления
    keyboard_buttons.extend([
        [InlineKeyboardButton(text="➕ Добавить покупку", callback_data="purchase_add")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_purchases")]
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data.startswith("purchases_page_"))
async def purchases_page_handler(callback: CallbackQuery):
    """Обработчик пагинации покупок"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    page = int(callback.data.replace("purchases_page_", ""))
    await show_purchases_page(callback, page)


@dp.callback_query(F.data == "purchase_add")
async def purchase_add_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления покупки"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    logger.info(f"➕ Начало добавления покупки (admin: {callback.from_user.id})")
    
    await callback.message.edit_text(
        "➕ ДОБАВЛЕНИЕ ПОКУПКИ\n\n"
        "Введите ID или username пользователя:"
    )
    await state.set_state(PurchaseStates.waiting_user_input)
    await callback.answer()


@dp.message(PurchaseStates.waiting_user_input)
async def purchase_user_input(message: Message, state: FSMContext):
    """Обработка ввода пользователя"""
    if not is_admin(message.from_user.id):
        logger.warning(f"🚫 Неавторизованная попытка добавления покупки от {message.from_user.id}")
        await message.answer("❌ Нет доступа")
        await state.clear()
        return
    
    user_input = message.text.strip()
    logger.debug(f"🔍 Поиск пользователя по: {user_input}")
    
    user = await db.get_user_by_id_or_username(user_input)
    
    if not user:
        logger.warning(f"❌ Пользователь не найден по {user_input}")
        await message.answer("❌ Пользователь не найден. Попробуйте еще раз:")
        return
    
    logger.info(f"✅ Найден пользователь: {user['user_id']} | @{user['username']}")
    
    # Сохраняем user_id в контекст
    await state.update_data(user_id=user['user_id'], username=user['username'], first_name=user['first_name'])
    
    await message.answer(
        f"✅ Пользователь найден:\n\n"
        f"Имя: {user['first_name']}\n"
        f"Username: @{user['username']}\n"
        f"ID: {user['user_id']}\n\n"
        f"Введите сумму покупки:"
    )
    await state.set_state(PurchaseStates.waiting_amount)


@dp.message(PurchaseStates.waiting_amount)
async def purchase_amount_input(message: Message, state: FSMContext):
    """Обработка ввода суммы"""
    if not is_admin(message.from_user.id):
        logger.warning(f"🚫 Неавторизованная попытка ввода суммы от {message.from_user.id}")
        await message.answer("❌ Нет доступа")
        await state.clear()
        return
    
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            logger.warning(f"⚠️ Попытка добавления покупки с суммой <= 0")
            await message.answer("❌ Сумма должна быть больше 0. Попробуйте еще раз:")
            return
    except ValueError:
        logger.warning(f"⚠️ Неверный формат суммы: {message.text}")
        await message.answer("❌ Неверный формат суммы. Введите число:")
        return
    
    logger.info(f"✅ Сумма покупки: {amount}")
    await state.update_data(amount=amount)
    
    await message.answer(
        f"✅ Сумма: {amount} €\n\n"
        f"Введите комментарий (например, 'club' или 'premium') или напишите '-' для пропуска:"
    )
    await state.set_state(PurchaseStates.waiting_comment)


@dp.message(PurchaseStates.waiting_comment)
async def purchase_comment_input(message: Message, state: FSMContext):
    """Обработка ввода комментария"""
    if not is_admin(message.from_user.id):
        logger.warning(f"🚫 Неавторизованная попытка ввода комментария от {message.from_user.id}")
        await message.answer("❌ Нет доступа")
        await state.clear()
        return
    
    comment = message.text.strip()
    if comment == "-":
        comment = ""
    
    # Получаем данные из контекста
    data = await state.get_data()
    user_id = data['user_id']
    username = data['username']
    first_name = data['first_name']
    amount = data['amount']
    
    # Добавляем покупку в БД
    success = await db.add_purchase(user_id, amount, comment)
    
    if success:
        logger.info(f"✅ Покупка добавлена: user_id={user_id}, amount={amount}, comment='{comment}'")
        await message.answer(
            f"✅ ПОКУПКА ДОБАВЛЕНА\n\n"
            f"{'='*40}\n"
            f"Пользователь: {first_name}\n"
            f"Username: @{username}\n"
            f"User ID: {user_id}\n"
            f"{'='*40}\n"
            f"💰 Сумма: {amount} €\n"
            f"📝 Комментарий: {comment or 'Нет'}\n"
            f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'='*40}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить еще", callback_data="purchase_add")],
                [InlineKeyboardButton(text="📋 Все покупки", callback_data="purchases_list")],
                [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="admin_purchases")]
            ])
        )
    else:
        logger.error(f"❌ Ошибка при добавлении покупки для user_id={user_id}")
        await message.answer(
            "❌ Ошибка при добавлении покупки.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_purchases")]
            ])
        )
    
    await state.clear()


@dp.callback_query(F.data.startswith("ref_stats_"))
async def ref_stats_detail(callback: CallbackQuery):
    """Детальная статистика реферала"""
    partner_code = callback.data.replace("ref_stats_", "")
    logger.debug(f"📊 Просмотр статистики партнера: {partner_code}")
    
    stats = await db.get_partner_stats(partner_code)
    bot_username = (await bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{partner_code}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Пользователи", callback_data=f"ref_users_{partner_code}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="ref_menu")]
    ])
    
    stats_text = (
        f"📊 АНАЛИТИКА ПАРТНЕРА\n\n"
        f"{'='*40}\n"
        f"Код: {partner_code}\n"
        f"{'='*40}\n\n"
        f"🔗 Реферальная ссылка:\n{ref_link}\n\n"
        f"{'='*40}\n"
        f"📈 СТАТИСТИКА:\n"
        f"{'='*40}\n"
        f"🆕 За сегодня: {stats['today']}\n"
        f"📅 За неделю: {stats['week']}\n"
        f"📊 За месяц: {stats['month']}\n"
        f"👥 За все время: {stats['total']}\n"
        f"{'='*40}"
    )
    
    await callback.message.edit_text(stats_text, reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data.startswith("ref_users_"))
async def ref_users_list(callback: CallbackQuery):
    """Список пользователей реферала с их покупками"""
    partner_code = callback.data.replace("ref_users_", "")
    users = await db.get_users_by_ref(partner_code, limit=50)  # Больше для рефералов
    purchases = await db.get_purchases_by_ref(partner_code)
    logger.debug(f"👥 Просмотр пользователей партнера {partner_code}: найдено {len(users)} пользователей, {len(purchases)} покупок")
    
    if not users:
        text = "👥 Пока нет пользователей, перешедших по вашей ссылке."
    else:
        text = f"👥 ПОЛЬЗОВАТЕЛИ И ПОКУПКИ ({len(users)} пользователей, {len(purchases)} покупок)\n\n"
        text += "="*50 + "\n\n"
        
        for i, user in enumerate(users, 1):
            name = user['first_name'] or ""
            if user['last_name']:
                name += f" {user['last_name']}"
            username = f"@{user['username']}" if user['username'] else "Без username"
            user_id = user['user_id']
            reg_date = user.get('registered_at', 'N/A')
            
            text += f"{i}. {name}\n"
            text += f"   ID: {user_id}\n"
            text += f"   Username: {username}\n"
            text += f"   Дата регистрации: {reg_date}\n"
            
            # Показываем покупки этого пользователя
            user_purchases = [p for p in purchases if p['user_id'] == user_id]
            if user_purchases:
                text += f"   💰 Покупки ({len(user_purchases)}):\n"
                for p in user_purchases:
                    text += f"      • {p['amount']}₽"
                    if p['comment']:
                        text += f" ({p['comment']})"
                    text += f" - {p['created_at']}\n"
            else:
                text += f"   💰 Покупок нет\n"
            
            text += "\n"
        
        text += "="*50
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Аналитика", callback_data=f"ref_stats_{partner_code}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="ref_menu")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data == "ref_menu")
async def ref_menu_back(callback: CallbackQuery):
    """Возврат в меню реферала"""
    user_id = callback.from_user.id
    partner_code = await db.get_user_partner_code(user_id)
    
    if not partner_code:
        logger.warning(f"❌ Пользователь {user_id} не является партнером")
        await callback.answer("❌ Вы не являетесь партнером", show_alert=True)
        return
    
    logger.debug(f"📊 Возврат в меню реферала для {user_id}")
    
    username = callback.from_user.username or "Без username"
    bot_username = (await bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{partner_code}"
    stats = await db.get_partner_stats(partner_code)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Аналитика", callback_data=f"ref_stats_{partner_code}")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data=f"ref_users_{partner_code}")]
    ])
    
    menu_text = (
        f"👤 ЛИЧНЫЙ КАБИНЕТ РЕФЕРАЛА\n\n"
        f"{'='*40}\n"
        f"Username: @{username}\n"
        f"User ID: {user_id}\n"
        f"Код партнера: {partner_code}\n"
        f"{'='*40}\n\n"
        f"🔗 Реферальная ссылка:\n{ref_link}\n\n"
        f"{'='*40}\n"
        f"📊 СТАТИСТИКА:\n"
        f"{'='*40}\n"
        f"🆕 За сегодня: {stats['today']}\n"
        f"📅 За неделю: {stats['week']}\n"
        f"📊 За месяц: {stats['month']}\n"
        f"👥 За все время: {stats['total']}\n"
        f"{'='*40}"
    )
    
    await callback.message.edit_text(menu_text, reply_markup=keyboard)
    await callback.answer()


async def main():
    """Главная функция"""
    logger.info("="*60)
    logger.info("🚀 ЗАПУСК TELEGRAM БОТА")
    logger.info("="*60)
    
    try:
        # Инициализация БД
        logger.info("📊 Инициализация базы данных...")
        await db.init_db()
        logger.info("✅ База данных инициализирована успешно")
        
        # Получаем информацию о боте
        bot_info = await bot.get_me()
        logger.info(f"✅ Бот авторизован: @{bot_info.username} (ID: {bot_info.id})")
        
        # Запуск бота
        logger.info("📡 Запуск polling...")
        logger.info("="*60)
        logger.info("✅ БОТ ЗАПУЩЕН И ГОТОВ К РАБОТЕ")
        logger.info("="*60)
        
        await dp.start_polling(bot)
    
    except TelegramUnauthorizedError as e:
        logger.critical("❌ КРИТИЧЕСКАЯ ОШИБКА: Telegram server says - Unauthorized. Проверьте BOT_TOKEN в .env: токен неверный, отозван или имеет неверный формат. Убедитесь, что токен выглядит как <id>:<token> и не содержит префикса 'Bot '.", exc_info=True)
        raise
    except Exception as e:
        logger.critical(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        raise
    finally:
        logger.info("="*60)
        logger.info("🛑 БОТ ОСТАНОВЛЕН")
        logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())

