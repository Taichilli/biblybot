import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


async def create_tables():
    """Создаёт таблицы в БД, если их нет."""
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            full_name TEXT NOT NULL,
            city TEXT NOT NULL,
            age INTEGER NOT NULL,
            phone TEXT,
            telegram TEXT,
            timezone VARCHAR DEFAULT 'UTC',
            registration_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            
        );

        CREATE TABLE IF NOT EXISTS schedule (
            id SERIAL PRIMARY KEY,
            text TEXT NOT NULL,
            days TEXT NOT NULL,
            time TEXT NOT NULL,
            timezone TEXT NOT NULL
        );

        INSERT INTO schedule (text, days, time, timezone)
        SELECT 'Курс проходит дважды в неделю.', 'Вт, Чт', '19:30', 'UTC+6'
        WHERE NOT EXISTS (SELECT 1 FROM schedule);
    """)
    await conn.close()


async def init_db():
    await create_tables()



