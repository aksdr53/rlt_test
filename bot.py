import asyncio
import json
import logging
from aiogram import Bot, Dispatcher, types, client, F
from aiogram.enums import ParseMode
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.strategy import FSMStrategy
from aiogram import Router
from aiogram.types import Message, BotCommand, BotCommandScopeDefault
from aiogram.filters import CommandStart

from aggregation import aggregate_salaries

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_TOKEN = '6878826878:AAHDmJa-G1Laqy0bwgJEQkSgqaqNckar4gg'

bot = Bot(token=API_TOKEN, session=AiohttpSession(), default=client.default.DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage, fsm_strategy=FSMStrategy.CHAT)
router = Router()

@router.message(CommandStart())
async def handle_start(message: Message):
    logger.info("Received start command from user %s", message.from_user.id)
    await message.answer(f"Hi {message.from_user.first_name} {message.from_user.last_name}")

@router.message(F.text.startswith('{'))
async def handle_message(message: Message):
    logger.info("Received JSON message from user %s: %s", message.from_user.id, message.text)
    try:
        data = json.loads(message.text)
        dt_from = data['dt_from']
        dt_upto = data['dt_upto']
        group_type = data['group_type']
        
        result = await aggregate_salaries(dt_from, dt_upto, group_type)
        response = json.dumps(result,)
        
        await message.answer(response)
        logger.info("Sent response to user %s: %s", message.from_user.id, response)
    except json.JSONDecodeError:
        error_message = "Invalid JSON format."
        logger.error("JSON decode error for user %s: %s", message.from_user.id, error_message)
        await message.answer(error_message)
    except ValueError as e:
        logger.error("Value error for user %s: %s", message.from_user.id, e)
        await message.answer(str(e))
    except Exception as e:
        logger.error("Unexpected error for user %s: %s", message.from_user.id, e)
        await message.answer(f"An error occurred: {e}")

async def main():
    dp.include_router(router)
    
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())