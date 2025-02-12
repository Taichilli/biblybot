import asyncio
import logging
import os
import asyncpg
import re
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from models import init_db
import pandas as pd  # Добавляем импорт pandas
from io import BytesIO  # Для отправки файла без сохранения на диск
from datetime import datetime, timedelta
import pytz  # Для работы с часовыми поясами
from timezonefinder import TimezoneFinder

tf = TimezoneFinder()

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME")
ADMIN_ID = int(os.getenv("ADMIN_ID"))  # ID администратора

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)
logging.basicConfig(level=logging.INFO)


# Подключение к базе данных
async def create_db_pool():
    """Создаёт пул подключений к БД с обработкой ошибок."""
    try:
        return await asyncpg.create_pool(DATABASE_URL)
    except Exception as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        return None





async def send_reminders():
    async with db_pool.acquire() as conn:
        schedule = await conn.fetchrow("SELECT * FROM schedule")
        if not schedule:
            return  # Если расписания нет, ничего не делаем

        schedule_timezone = pytz.timezone(schedule["timezone"])
        lesson_time_utc = datetime.strptime(schedule["time"], "%H:%M").time()
        lesson_datetime_utc = schedule_timezone.localize(datetime.combine(datetime.now().date(), lesson_time_utc))

        reminder_1h_utc = lesson_datetime_utc - timedelta(hours=1)
        reminder_1d_utc = lesson_datetime_utc - timedelta(days=1)

        # Русские дни переводим в английские
        DAYS_MAP = {
            "пн": "mon", "вт": "tue", "ср": "wed", "чт": "thu",
            "пт": "fri", "сб": "sat", "вс": "sun"
        }
        lesson_days = [DAYS_MAP[day.strip().lower()] for day in schedule["days"].split(",")]

        now_utc = datetime.now(pytz.utc)

        users = await conn.fetch("SELECT user_id, timezone FROM users")  # Получаем таймзоны пользователей

        for user in users:
            user_tz = pytz.timezone(user["timezone"]) if user["timezone"] else pytz.utc
            user_time = lesson_datetime_utc.astimezone(user_tz).strftime("%H:%M")  # Конвертируем в локальное время пользователя

            if now_utc.strftime("%a").lower() in lesson_days:
                if reminder_1h_utc <= now_utc < lesson_datetime_utc:
                    await send_reminder(user["user_id"],
                                        f"📢 Не забудьте! Сегодня в {user_time} (по вашему времени) начнётся занятие по курсу изучения Библии.")

                if reminder_1d_utc.date() == now_utc.date():
                    await send_reminder(user["user_id"],
                                        f"📢 Не забудьте! Завтра в {user_time} (по вашему времени) начнётся занятие по курсу изучения Библии.")



async def send_reminder(users, text):
    """Отправляет напоминание всем пользователям."""
    for user in users:
        try:
            await bot.send_message(user["user_id"], text)
        except Exception as e:
            logging.warning(f"Ошибка отправки напоминания {user['user_id']}: {e}")



async def notify_schedule_update():
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM users")

        for user in users:
            try:
                await bot.send_message(user["user_id"],
                                       "📢 Внимание! Расписание занятий изменилось. Проверьте новое расписание в боте. \n "
                                       "по кнопке 'информация о курсе'")
            except Exception as e:
                logging.warning(f"Ошибка отправки уведомления {user['user_id']}: {e}")


db_pool = None
scheduler = AsyncIOScheduler()


# Определение состояний
class Registration(StatesGroup):
    full_name = State()
    city = State()
    age = State()
    phone = State()
    telegram = State()


class EditSchedule(StatesGroup):
    text = State()
    days = State()
    time = State()
    timezone = State()


class Broadcast(StatesGroup):
    text = State()


class SearchUser(StatesGroup):
    query = State()


# Клавиатуры
after_registration_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="ℹ️ Информация о курсе")],
        [KeyboardButton(text="📞 Связь с оператором")]
    ],
    resize_keyboard=True
)
geo_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📍 Отправить местоположение", request_location=True)],
        [KeyboardButton(text="🚫 Не отправлять")]
    ],
    resize_keyboard=True
)
admin_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="✏️ Редактировать расписание")],
        [KeyboardButton(text="📢 Массовая рассылка")],
        [KeyboardButton(text="🔍 Поиск пользователя")],
        [KeyboardButton(text="📋 Показать учеников")]
    ],
    resize_keyboard=True
)


@router.message(F.text == "/add_test_users")
async def add_test_users(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    test_users = [
        (101, 'Иван Иванов', 'Москва', 25, '+79161234567', '@port_manager_mmvbrts'),
        (102, 'Мария Петрова', 'Санкт-Петербург', 30, '+79261234568', '@maria_pet'),
        (103, 'Александр Сидоров', 'Новосибирск', 27, '+79371234569', '@alex_s'),
        (104, 'Ольга Смирнова', 'Казань', 22, '+79481234560', '@olga_smir'),
        (105, 'Дмитрий Кузнецов', 'Екатеринбург', 35, '+79591234561', '@dmitry_k'),
    ]

    async with db_pool.acquire() as conn:
        for user in test_users:
            user_id = int(user[0])  # Явно преобразуем user_id в число
            await conn.execute(
                "INSERT INTO users (user_id, full_name, city, age, phone, telegram) "
                "VALUES ($1, $2, $3, $4, $5, $6) "
                "ON CONFLICT (user_id) DO NOTHING",
                user_id, *user[1:]
            )

    await message.answer("✅ Тестовые пользователи добавлены!")


@router.message(F.text == "/clear_db")
async def clear_database(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM users;")
        await conn.execute("DELETE FROM schedule;")
        await message.answer("✅ База данных очищена!")


@router.message(F.text == "/start")
async def start_command(message: types.Message, state: FSMContext):
    if message.from_user.id == ADMIN_ID:
        await message.answer("Добро пожаловать, Админ!", reply_markup=admin_keyboard)
        return

    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", message.from_user.id)

    if user:
        await message.answer("Вы уже зарегистрированы!", reply_markup=after_registration_keyboard)
    else:
        await message.answer(
            "Благословенного дня! 🙏\n"
            "Меня зовут Роман, я помогу вам зарегистрироваться на курс изучения Библии и получить информацию о занятиях.\n"
        )
        await message.answer("Введите ваше ФИО:")
        await state.set_state(Registration.full_name)


@router.message(Registration.full_name)
async def process_full_name(message: types.Message, state: FSMContext):
    await state.update_data(full_name=message.text)
    await message.answer("Введите ваш город или страну:")
    await state.set_state(Registration.city)


@router.message(Registration.city)
async def process_city(message: types.Message, state: FSMContext):
    await state.update_data(city=message.text)
    await message.answer("Введите ваш возраст:")
    await state.set_state(Registration.age)


@router.message(Registration.age)
async def process_age(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Возраст должен быть числом.")
        return

    await state.update_data(age=int(message.text))
    await message.answer("Введите ваш номер телефона:")
    await state.set_state(Registration.phone)


@router.message(Registration.phone)
async def process_phone(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.text)
    await message.answer("Введите ссылку на ваш Telegram:")
    await state.set_state(Registration.telegram)


@router.message(Registration.telegram)
async def process_telegram(message: types.Message, state: FSMContext):
    await state.update_data(telegram=message.text)
    data = await state.get_data()

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, full_name, city, age, phone, telegram, timezone)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, message.from_user.id, data['full_name'], data['city'], data['age'], data['phone'], data['telegram'], None)  # Часовой пояс пока None

    await message.answer(
        "Спасибо за регистрацию! 🎉\n"
        "Теперь отправьте ваше местоположение, чтобы мы могли правильно определить ваше время занятий.\n\n"
        "Если не хотите отправлять местоположение, нажмите кнопку '🚫 Не отправлять'.",
        reply_markup=geo_keyboard
    )

    await state.clear()



@router.message(F.text == "📢 Массовая рассылка")
async def start_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("Введите текст рассылки:")
    await state.set_state(Broadcast.text)


@router.message(Broadcast.text)
async def process_broadcast(message: types.Message, state: FSMContext):
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id, full_name FROM users")

    sent_users = []
    failed_users = []

    # Проверяем, есть ли медиа в сообщении
    caption = message.caption if message.caption else message.text
    media = None

    if message.photo:
        media = types.InputMediaPhoto(media=message.photo[-1].file_id, caption=caption)
    elif message.video:
        media = types.InputMediaVideo(media=message.video.file_id, caption=caption)
    elif message.document:
        media = types.InputMediaDocument(media=message.document.file_id, caption=caption)
    elif message.audio:
        media = types.InputMediaAudio(media=message.audio.file_id, caption=caption)
    elif message.voice:
        media = message.voice.file_id  # Голосовое сообщение
    elif message.animation:
        media = message.animation.file_id  # GIF-анимация

    for user in users:
        try:
            if media:
                if message.voice:
                    await bot.send_voice(user["user_id"], media, caption=caption)
                elif message.animation:
                    await bot.send_animation(user["user_id"], media, caption=caption)
                else:
                    await bot.send_media_group(user["user_id"], [media])
            else:
                await bot.send_message(user["user_id"], message.text)

            sent_users.append(user["full_name"])
            await asyncio.sleep(0.1)  # Пауза, чтобы избежать блокировки Telegram
        except Exception:
            failed_users.append(user["full_name"])

    # Формируем отчет
    report = "📢 **Рассылка завершена!**\n"
    if sent_users:
        report += "✅ **Сообщение получили:**\n" + "\n".join(sent_users) + "\n"
    if failed_users:
        report += "❌ **Не удалось отправить:**\n" + "\n".join(failed_users)

    await message.answer(report)
    await state.clear()


@router.message(F.text == "🔍 Поиск пользователя")
async def start_search(message: types.Message, state: FSMContext):
    await message.answer("Введите имя или город пользователя для поиска:")
    await state.set_state(SearchUser.query)


@router.message(F.text == "ℹ️ Информация о курсе")
async def course_info(message: types.Message):
    async with db_pool.acquire() as conn:
        schedule = await conn.fetchrow("SELECT * FROM schedule")

    if schedule:
        info_text = (f"📅 Расписание занятий:\n{schedule['text']}\n\n"
                     f"📅 Дни недели: {schedule['days']}\n"
                     f"⏰ Время: {schedule['time']} ({schedule['timezone']})")
        await message.answer(info_text)
        await message.answer(open("FAQ.txt", "rb").read())
    else:
        await message.answer("❌ Расписание ещё не добавлено.")


@router.message(F.location)
async def process_location(message: types.Message):
    """Сохраняет часовой пояс пользователя по геолокации."""
    latitude = message.location.latitude
    longitude = message.location.longitude

    timezone = tf.timezone_at(lat=latitude, lng=longitude)

    if timezone:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET timezone = $1 WHERE user_id = $2",
                timezone,
                message.from_user.id
            )
        await message.answer(f"✅ Ваш часовой пояс установлен: {timezone}", reply_markup=after_registration_keyboard)
    else:
        await message.answer("❌ Не удалось определить часовой пояс. Используйте стандартный (UTC+3).", reply_markup=after_registration_keyboard)



@router.message(F.text == "📞 Связь с оператором")
async def support_chat(message: types.Message):
    if SUPPORT_USERNAME:
        await message.answer(f"Свяжитесь с оператором:[Перейти в чат](https://t.me/{SUPPORT_USERNAME})",
                             parse_mode="Markdown")
    else:
        await message.answer("Контакт поддержки пока не настроен.")


@router.message(F.text == "✏️ Редактировать расписание")
async def edit_schedule(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("Введите новый текст расписания:")
    await state.set_state(EditSchedule.text)


@router.message(F.text == "📋 Показать учеников")
async def show_students(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    async with db_pool.acquire() as conn:
        students = await conn.fetch("SELECT full_name, city, age, phone, telegram FROM users")

    if not students:
        await message.answer("В базе данных пока нет зарегистрированных учеников.")
        return

    df = pd.DataFrame(students, columns=["ФИО", "Город", "Возраст", "Телефон", "Telegram"])

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Ученики")

    output.seek(0)
    await message.answer_document(types.BufferedInputFile(output.read(), filename="students.xlsx"))


@router.message(SearchUser.query)
async def process_search(message: types.Message, state: FSMContext):
    query = message.text.lower()
    async with db_pool.acquire() as conn:
        users = await conn.fetch("""
            SELECT * FROM users 
            WHERE LOWER(full_name) LIKE '%' || $1 || '%' 
            OR LOWER(city) LIKE '%' || $1 || '%'
        """, query)


        if not users:
            await message.answer("❌ Пользователи не найдены.")
        else:
            response = "📋 **Результаты поиска:**\n"
            for user in users:
                response += f"👤 {user['full_name']}, 🏙 {user['city']}, {user['age']} возраст\n📞 {user['phone']}, 📨 {user['telegram']}\n\n"
            await message.answer(response)


@router.message(EditSchedule.text)
async def schedule_text(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    await message.answer("Выберите дни недели (например: Пн, Ср, Пт):")
    await state.set_state(EditSchedule.days)


@router.message(EditSchedule.days)
async def schedule_days(message: types.Message, state: FSMContext):
    valid_days = {"пн", "вт", "ср", "чт", "пт", "сб", "вс"}
    input_days = {day.strip().lower() for day in message.text.split(",")}

    if not input_days.issubset(valid_days):
        await message.answer("Ошибка! Введите дни недели через запятую, например: Пн, Ср, Пт.")
        return

    await state.update_data(days=message.text)
    await message.answer("Введите время (формат: ЧЧ:ММ):")
    await state.set_state(EditSchedule.time)


@router.message(EditSchedule.time)
async def schedule_time(message: types.Message, state: FSMContext):
    if not re.match(r"^\d{2}:\d{2}$", message.text):
        await message.answer("Ошибка! Введите время в формате ЧЧ:ММ (например: 19:30).")
        return

    await state.update_data(time=message.text)
    await message.answer("Введите часовой пояс (например: UTC+3):")
    await state.set_state(EditSchedule.timezone)


@router.message(EditSchedule.timezone)
async def schedule_timezone(message: types.Message, state: FSMContext):
    if not re.match(r"^UTC[+-]\d+$", message.text):
        await message.answer("Ошибка! Введите корректный часовой пояс (например: UTC+3 или UTC-5).")
        return

    data = await state.get_data()
    async with db_pool.acquire() as conn:
        exists = await conn.fetchrow("SELECT id FROM schedule LIMIT 1")
        if exists:
            await conn.execute(
                "UPDATE schedule SET text = $1, days = $2, time = $3, timezone = $4 WHERE id = $5",
                data['text'], data['days'], data['time'], message.text, exists['id']
            )
        else:
            await conn.execute(
                "INSERT INTO schedule (text, days, time, timezone) VALUES ($1, $2, $3, $4)",
                data['text'], data['days'], data['time'], message.text
            )

        await message.answer("✅ Расписание обновлено!", reply_markup=admin_keyboard)
        await notify_schedule_update()

        await state.clear()


async def main():
    global db_pool
    db_pool = await create_db_pool()

    if db_pool is None:
        logging.error("Не удалось подключиться к базе данных. Бот завершает работу.")
        return  # Останавливаем бота, если БД недоступна

    await init_db()

    scheduler.start()
    scheduler.add_job(send_reminders, "cron", hour=9, minute=0)  # Запуск в 9:00 утра
    scheduler.add_job(send_reminders, "cron", hour=18, minute=30)  # Запуск в 18:30

    try:
        await dp.start_polling(bot)
    finally:
        await db_pool.close()  # Закрываем пул при завершении работ


if __name__ == "__main__":
    asyncio.run(main())
