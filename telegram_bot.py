# telegram_bot.py
import asyncio
import json
import logging
import os
import random
import re
from pathlib import Path
from typing import Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
import generic_scraper  # Import the generic scraper module
from shared_scrapers_config import setup_logging # Import the unified logging setup

# --- Setup Unified Logging ---
setup_logging()
bot_logger = logging.getLogger(__name__)

# Suppress overly verbose logs from telegram library
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
# How often to run the full scrape cycle (e.g., every 5 minutes)
CHECK_INTERVAL_SECONDS = 60 * 15
# Delay between sending messages to avoid rate limits (in seconds)
MIN_MESSAGE_DELAY_SECONDS = 0.5
MAX_MESSAGE_DELAY_SECONDS = 1.0
# File to store subscribed chat IDs
MERGED_OUTPUT_FILE = generic_scraper.MERGED_OUTPUT_FILE
SUBSCRIBERS_FILE = Path("subscribers.json")

class TelegramBot:
    def __init__(self, token: str):
        self.application = Application.builder().token(token).build()
        self.subscribed_chats = self.load_subscribers()  # Store chat IDs that have subscribed
        self.known_apartments = {}
        self.load_known_apartments()

    def load_subscribers(self):
        """Load the list of subscribed chat IDs from file."""
        if SUBSCRIBERS_FILE.exists():
            try:
                with open(SUBSCRIBERS_FILE, 'r', encoding='utf-8') as f:
                    subscribers = set(json.load(f))
                bot_logger.info(f"Loaded {len(subscribers)} subscribers from {SUBSCRIBERS_FILE}")
                return subscribers
            except json.JSONDecodeError as e:
                bot_logger.error(f"Error decoding JSON from {SUBSCRIBERS_FILE}: {e}")
                return set()
            except FileNotFoundError:
                bot_logger.warning(f"Subscribers file {SUBSCRIBERS_FILE} does not exist yet.")
                return set()
            except Exception as e:
                bot_logger.error(f"Unexpected error loading {SUBSCRIBERS_FILE}: {e}")
                return set()
        else:
            bot_logger.warning(f"Subscribers file {SUBSCRIBERS_FILE} does not exist yet.")
            return set()

    def save_subscribers(self):
        """Save the list of subscribed chat IDs to file."""
        try:
            with open(SUBSCRIBERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(self.subscribed_chats), f, ensure_ascii=False, indent=2)
            bot_logger.info(f"Saved {len(self.subscribed_chats)} subscribers to {SUBSCRIBERS_FILE}")
        except Exception as e:
            bot_logger.error(f"Error saving subscribers to {SUBSCRIBERS_FILE}: {e}")

    def load_known_apartments(self):
        if MERGED_OUTPUT_FILE.exists():
            try:
                with open(MERGED_OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.known_apartments = {
                    md5: item for md5, item in data.items()}
                bot_logger.info(
                    f"Loaded {len(self.known_apartments)} apartments from {MERGED_OUTPUT_FILE}")
            except json.JSONDecodeError as e:
                bot_logger.error(
                    f"Error decoding JSON from {MERGED_OUTPUT_FILE}: {e}")
                self.known_apartments = {}
            except FileNotFoundError:
                bot_logger.warning(
                    f"Merged file {MERGED_OUTPUT_FILE} does not exist yet.")
                self.known_apartments = {}
            except Exception as e:
                bot_logger.error(
                    f"Unexpected error loading {MERGED_OUTPUT_FILE}: {e}")
                self.known_apartments = {}
        else:
            bot_logger.warning(
                f"Merged file {MERGED_OUTPUT_FILE} does not exist yet.")
            self.known_apartments = {}

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /start command."""
        try:
            chat_id = update.effective_message.chat_id if update.effective_message else None
            if chat_id:
                # Create inline keyboard with commands
                keyboard = [
                    [
                        InlineKeyboardButton("Subscribe", callback_data='subscribe'),
                        InlineKeyboardButton("Unsubscribe", callback_data='unsubscribe')
                    ],
                    [
                        InlineKeyboardButton("Dump All Apartments", callback_data='dumpall')
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                help_message = (
                    "🏠 **Apartment Finder Bot**\n"
                    "I can help you find new apartments on Yad2 and Facebook Marketplace!\n"
                    "Available commands:\n"
                    "/start - Show this help message\n"
                    "/help - Show this help message\n"
                    "/subscribe - Subscribe to receive updates about new apartments\n"
                    "/unsubscribe - Unsubscribe from updates\n"
                    "/dumpall - Get all apartments currently in the database\n"
                    "Use the buttons below to manage your subscription or view apartments."
                )
                await update.message.reply_text(
                    help_message,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                bot_logger.info(
                    f"Chat {chat_id} started bot and received help message.")
            else:
                bot_logger.warning(
                    f"Could not get chat ID from /start command update: {update}")
        except TelegramError as e:
            bot_logger.error(f"Telegram error in /start command: {e}")
        except Exception as e:
            bot_logger.error(f"Unexpected error in /start command: {e}")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /help command."""
        try:
            chat_id = update.effective_message.chat_id if update.effective_message else None
            if chat_id:
                # Create inline keyboard with commands
                keyboard = [
                    [
                        InlineKeyboardButton("Subscribe", callback_data='subscribe'),
                        InlineKeyboardButton("Unsubscribe", callback_data='unsubscribe')
                    ],
                    [
                        InlineKeyboardButton("Dump All Apartments", callback_data='dumpall')
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                help_message = (
                    "🏠 **Apartment Finder Bot**\n"
                    "I can help you find new apartments on Yad2 and Facebook Marketplace!\n"
                    "Available commands:\n"
                    "/start - Show this help message\n"
                    "/help - Show this help message\n"
                    "/subscribe - Subscribe to receive updates about new apartments\n"
                    "/unsubscribe - Unsubscribe from updates\n"
                    "/dumpall - Get all apartments currently in the database\n"
                    "Use the buttons below to manage your subscription or view apartments."
                )
                await update.message.reply_text(
                    help_message,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                bot_logger.info(
                    f"Chat {chat_id} requested help message.")
            else:
                bot_logger.warning(
                    f"Could not get chat ID from /help command update: {update}")
        except TelegramError as e:
            bot_logger.error(f"Telegram error in /help command: {e}")
        except Exception as e:
            bot_logger.error(f"Unexpected error in /help command: {e}")

    async def subscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /subscribe command."""
        try:
            chat_id = update.effective_message.chat_id if update.effective_message else None
            if chat_id:
                self.subscribed_chats.add(chat_id)
                self.save_subscribers()  # Save the updated list
                await update.message.reply_text(
                    "You are now subscribed to receive updates about new and updated apartments. Scraping will run periodically."
                )
                bot_logger.info(
                    f"Chat {chat_id} subscribed for updates via /subscribe.")
            else:
                bot_logger.warning(
                    f"Could not get chat ID from /subscribe command update: {update}")
        except TelegramError as e:
            bot_logger.error(f"Telegram error in /subscribe command: {e}")
        except Exception as e:
            bot_logger.error(f"Unexpected error in /subscribe command: {e}")

    async def unsubscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /unsubscribe command."""
        try:
            chat_id = update.effective_message.chat_id if update.effective_message else None
            if chat_id and chat_id in self.subscribed_chats:
                self.subscribed_chats.discard(chat_id)
                self.save_subscribers()  # Save the updated list
                await update.message.reply_text(
                    "You have been unsubscribed. You will no longer receive updates."
                )
                bot_logger.info(
                    f"Chat {chat_id} unsubscribed via /unsubscribe.")
            elif chat_id:
                await update.message.reply_text(
                    "You were not subscribed."
                )
            else:
                bot_logger.warning(
                    f"Could not get chat ID from /unsubscribe command update: {update}")
        except TelegramError as e:
            bot_logger.error(f"Telegram error in /unsubscribe command: {e}")
        except Exception as e:
            bot_logger.error(f"Unexpected error in /unsubscribe command: {e}")

    async def dumpall_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /dumpall command."""
        try:
            chat_id = update.effective_message.chat_id if update.effective_message else None
            if chat_id:
                if not MERGED_OUTPUT_FILE.exists():
                    await update.message.reply_text("No apartment data available yet.")
                    return

                with open(MERGED_OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    current_data = json.load(f)

                apartments = list(current_data.values())
                if not apartments:
                    await update.message.reply_text("No apartments found in the database.")
                    return

                # Send a message for each apartment (or batch them if there are many)
                total_apartments = len(apartments)
                await update.message.reply_text(f"Sending {total_apartments} apartments...")

                for i, apt in enumerate(apartments):
                    message = self.format_apartment_message(apt)
                    try:
                        await update.message.reply_text(message, parse_mode='HTML')
                        # Add a random delay to avoid hitting rate limits
                        delay = random.uniform(MIN_MESSAGE_DELAY_SECONDS, MAX_MESSAGE_DELAY_SECONDS)
                        await asyncio.sleep(delay)
                    except TelegramError as e:
                        bot_logger.error(f"Error sending apartment {i} to chat {chat_id}: {e}")
                        # Continue to next apartment even if one fails
                        continue
                    except Exception as e:
                        bot_logger.error(f"Unexpected error sending apartment {i} to chat {chat_id}: {e}")
                        continue

                bot_logger.info(
                    f"Chat {chat_id} requested /dumpall and received {total_apartments} apartments.")
            else:
                bot_logger.warning(
                    f"Could not get chat ID from /dumpall command update: {update}")
        except Exception as e:
            bot_logger.error(f"Unexpected error in /dumpall command: {e}")

    def format_apartment_message(self, apt: Dict[str, Any]) -> str:
        # Use the normalized fields
        title = apt.get('title', apt.get('id', 'N/A'))
        price = apt.get('price', 'N/A')
        location = apt.get('location', f"{apt.get('street', 'N/A')}, {apt.get('city', 'N/A')}")
        url = apt.get('apartment_page_url', 'N/A')
        description = apt.get('description', 'No description available')
        rooms = apt.get('rooms', 'N/A')
        size = apt.get('size', 'N/A')
        floor = apt.get('floor', 'N/A')
        type_ = apt.get('type', 'Unknown')
        tags = apt.get('tags', [])
        full_address = apt.get('full_address', 'N/A')
        unit_room_info = apt.get('unit_room_info', 'N/A')
        delivery_types = apt.get('delivery_types', 'N/A')
        comments_count = apt.get('comments_count', 'N/A')

        # Format price with currency symbol if it's a number
        formatted_price = f"₪{price:,}" if isinstance(price, (int, float)) else str(price)

        message = (
            f"<b>🏠 Apartment Found!</b>\n"
            f"<b>Type:</b> {type_}\n"
            f"<b>Title:</b> {title}\n"
            f"<b>Price:</b> {formatted_price}\n"
            f"<b>Location:</b> {location}\n"
            f"<b>Full Address:</b> {full_address}\n"
            f"<b>Rooms:</b> {rooms}\n"
            f"<b>Unit Info:</b> {unit_room_info}\n"
            f"<b>Size:</b> {size} sqm\n"
            f"<b>Floor:</b> {floor}\n"
            f"<b>Delivery Types:</b> {delivery_types}\n"
            f"<b>Comments Count:</b> {comments_count}\n"
            f"<b>Tags:</b> {', '.join(tags) if tags else 'N/A'}\n"
            f"<b>Description:</b> {description}\n"
            f"<b>URL:</b> <a href='{url}'>Link</a>"
        )
        return message

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button presses from inline keyboard."""
        query = update.callback_query
        await query.answer()

        chat_id = query.message.chat_id

        if query.data == 'subscribe':
            self.subscribed_chats.add(chat_id)
            self.save_subscribers()
            await query.edit_message_text(text="You are now subscribed! You will receive updates about new and updated apartments.")
            bot_logger.info(f"Chat {chat_id} subscribed via inline button.")
        elif query.data == 'unsubscribe':
            if chat_id in self.subscribed_chats:
                self.subscribed_chats.discard(chat_id)
                self.save_subscribers()
                await query.edit_message_text(text="You have been unsubscribed. You will no longer receive updates.")
                bot_logger.info(f"Chat {chat_id} unsubscribed via inline button.")
            else:
                await query.edit_message_text(text="You were not subscribed.")
        elif query.data == 'dumpall':
            if not MERGED_OUTPUT_FILE.exists():
                await query.edit_message_text(text="No apartment data available yet.")
                return

            with open(MERGED_OUTPUT_FILE, 'r', encoding='utf-8') as f:
                current_data = json.load(f)

            apartments = list(current_data.values())
            if not apartments:
                await query.edit_message_text(text="No apartments found in the database.")
                return

            # Send a message for each apartment
            total_apartments = len(apartments)
            await query.edit_message_text(f"Sending {total_apartments} apartments...")

            for i, apt in enumerate(apartments):
                message = self.format_apartment_message(apt)
                try:
                    await query.message.reply_text(message, parse_mode='HTML')
                    # Add a random delay to avoid hitting rate limits
                    delay = random.uniform(MIN_MESSAGE_DELAY_SECONDS, MAX_MESSAGE_DELAY_SECONDS)
                    await asyncio.sleep(delay)
                except TelegramError as e:
                    bot_logger.error(f"Error sending apartment {i} to chat {chat_id}: {e}")
                    continue
                except Exception as e:
                    bot_logger.error(f"Unexpected error sending apartment {i} to chat {chat_id}: {e}")
                    continue

            bot_logger.info(
                f"Chat {chat_id} requested dumpall via inline button and received {total_apartments} apartments.")

    async def send_message_to_chat(self, chat_id: int, message: str):
        """Helper function to send a message to a specific chat with error handling."""
        try:
            await self.application.bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
            bot_logger.info(f"Message sent successfully to chat {chat_id}")
        except TelegramError as e:
            bot_logger.error(
                f"Telegram error sending message to chat {chat_id}: {e}")
            # Check if it's a flood control error and parse the retry time
            if "Flood control exceeded" in str(e):
                # Extract seconds from the error message
                match = re.search(r"Retry in (\d+) seconds", str(e))
                if match:
                    wait_time = int(match.group(1))
                    # Add random time between 5 and 30 seconds
                    additional_wait = random.randint(5, 30)
                    total_wait = wait_time + additional_wait
                    bot_logger.info(f"Waiting {total_wait} seconds before retrying message to {chat_id}")
                    await asyncio.sleep(total_wait)
                    try:
                        await self.application.bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
                        bot_logger.info(f"Message sent successfully to chat {chat_id} after retry")
                    except TelegramError as retry_error:
                        bot_logger.error(f"Retry failed for chat {chat_id}: {retry_error}")
                else:
                    bot_logger.error(f"Could not parse wait time from flood control error: {e}")
            # Example: Check for specific errors like blocked user
            elif e.message == "Forbidden: bot was blocked by the user":
                bot_logger.info(
                    f"Bot was blocked by user {chat_id}, removing from subscribers.")
                self.subscribed_chats.discard(chat_id)
                self.save_subscribers()  # Save the updated list
            elif e.message == "Forbidden: chat not found":
                bot_logger.info(
                    f"Chat {chat_id} not found, removing from subscribers.")
                self.subscribed_chats.discard(chat_id)
                self.save_subscribers()  # Save the updated list
        except Exception as e:
            bot_logger.error(
                f"Unexpected error sending message to chat {chat_id}: {e}")

    async def run_scraping_cycle(self):
        """Run the generic scraper and check for new/updated apartments."""
        try:
            bot_logger.info("Starting scraping cycle...")
            # Run the generic scraper and capture its output
            await generic_scraper.run_generic_scraper()
            bot_logger.info("Scraping cycle completed.")

            # Reload the merged data after scraping
            if not MERGED_OUTPUT_FILE.exists():
                bot_logger.warning(
                    f"Merged file {MERGED_OUTPUT_FILE} does not exist after scraping.")
                return

            with open(MERGED_OUTPUT_FILE, 'r', encoding='utf-8') as f:
                current_data = json.load(f)

            current_by_md5 = {md5: item for md5, item in current_data.items()}
            new_apartments = []
            updated_apartments = []

            for md5, item in current_by_md5.items():
                if md5 not in self.known_apartments:
                    new_apartments.append(item)
                elif item.get('id') != self.known_apartments[md5].get('id'):
                    updated_apartments.append(item)

            # Notify subscribed chats about new apartments
            for apt in new_apartments:
                message = self.format_apartment_message(apt)
                for chat_id in self.subscribed_chats.copy():  # Use copy to avoid issues if set changes during iteration
                    await self.send_message_to_chat(chat_id, message)
                    # Add a random delay between sending messages to avoid rate limits
                    delay = random.uniform(MIN_MESSAGE_DELAY_SECONDS, MAX_MESSAGE_DELAY_SECONDS)
                    await asyncio.sleep(delay)

            # Notify subscribed chats about updated apartments
            for apt in updated_apartments:
                message = f"<b>🔄 Apartment Updated!</b>\nID changed for: {apt.get('id', 'N/A')}\nURL: <a href='{apt.get('apartment_page_url', 'N/A')}'>Link</a>"
                for chat_id in self.subscribed_chats.copy():
                    await self.send_message_to_chat(chat_id, message)
                    # Add a random delay between sending messages to avoid rate limits
                    delay = random.uniform(MIN_MESSAGE_DELAY_SECONDS, MAX_MESSAGE_DELAY_SECONDS)
                    await asyncio.sleep(delay)

            # Update the known apartments after processing
            self.known_apartments = current_by_md5
            bot_logger.info(
                f"Scraping cycle finished. Found {len(new_apartments)} new and {len(updated_apartments)} updated apartments.")

        except Exception as e:
            bot_logger.error(f"Unexpected error during scraping cycle: {e}")

    async def run_polling(self):
        try:
            # Set bot commands for the command menu
            await self.application.bot.set_my_commands([
                BotCommand("start", "Show help message and command options"),
                BotCommand("help", "Show help message and command options"),
                BotCommand("subscribe", "Subscribe to receive apartment updates"),
                BotCommand("unsubscribe", "Unsubscribe from apartment updates"),
                BotCommand("dumpall", "Get all apartments currently in the database")
            ])

            # Add command handlers
            self.application.add_handler(
                CommandHandler("start", self.start_command))
            self.application.add_handler(
                CommandHandler("help", self.help_command))
            self.application.add_handler(CommandHandler(
                "subscribe", self.subscribe_command))
            self.application.add_handler(CommandHandler(
                "unsubscribe", self.unsubscribe_command))
            self.application.add_handler(CommandHandler(
                "dumpall", self.dumpall_command))
            # Add callback query handler for inline buttons
            self.application.add_handler(CallbackQueryHandler(
                self.button_handler))

            # Start the bot
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()

            bot_logger.info(
                "Telegram Bot polling started, waiting for commands and running periodic scrapes...")

            # Main loop to run the scraping cycle periodically
            while True:
                try:
                    await self.run_scraping_cycle()
                except Exception as e:
                    bot_logger.error(
                        f"Unexpected error in main scraping loop: {e}")
                await asyncio.sleep(CHECK_INTERVAL_SECONDS)

        except Exception as e:
            bot_logger.error(
                f"Critical error running the bot polling loop: {e}")
        finally:
            # Graceful shutdown if the loop ever exits (shouldn't normally)
            try:
                await self.application.stop()
                await self.application.shutdown()
                bot_logger.info(
                    "Telegram Bot application shut down gracefully.")
            except Exception as e:
                bot_logger.error(f"Error during bot shutdown: {e}")

async def main():
    if not TELEGRAM_BOT_TOKEN:
        bot_logger.error("Telegram bot token not configured!")
        return

    bot = TelegramBot(TELEGRAM_BOT_TOKEN)
    await bot.run_polling()

if __name__ == '__main__':
    asyncio.run(main())