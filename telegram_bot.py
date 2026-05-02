# telegram_bot.py
import asyncio
import html
import json
import logging
import os
import random
import re
from pathlib import Path
from typing import Any, Dict, List

from telegram import (BotCommand, InlineKeyboardButton, InlineKeyboardMarkup,
                      Update)
from telegram.error import TelegramError
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes)

# Import the generic scraper module to access its function and MERGED_OUTPUT_FILE
import generic_scraper
from shared_scrapers_config import setup_logging

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
MIN_MESSAGE_DELAY_SECONDS = 2
MAX_MESSAGE_DELAY_SECONDS = 5
MANUAL_DUMP_MESSAGE_DELAY_SECONDS = 0.25
MAX_DESCRIPTION_CHARS = 700

MERGED_OUTPUT_FILE = Path.cwd() / Path("out/merged_apartments.json")

# File to store subscribed chat IDs
SUBSCRIBERS_FILE = Path.cwd() / Path("out/subscribers.json")


class TelegramBot:
    def __init__(self, token: str):
        self.application = Application.builder().token(token).build()
        # Store chat IDs that have subscribed
        self.subscribed_chats = self.load_subscribers()
        # This will store apartments by their ID for quick lookup and saving
        self.known_apartments_by_id = {}
        # Load the initial state of apartments from the merged file
        self.load_known_apartments()

    def load_subscribers(self):
        """Load the list of subscribed chat IDs from file."""
        if SUBSCRIBERS_FILE.exists():
            try:
                with open(SUBSCRIBERS_FILE, 'r', encoding='utf-8') as f:
                    subscribers = set(json.load(f))
                bot_logger.info(
                    f"Loaded {len(subscribers)} subscribers from {SUBSCRIBERS_FILE}")
                return subscribers
            except json.JSONDecodeError as e:
                bot_logger.error(
                    f"Error decoding JSON from {SUBSCRIBERS_FILE}: {e}")
                return set()
            except FileNotFoundError:
                bot_logger.warning(
                    f"Subscribers file {SUBSCRIBERS_FILE} does not exist yet.")
                return set()
            except Exception as e:
                bot_logger.error(
                    f"Unexpected error loading {SUBSCRIBERS_FILE}: {e}")
                return set()
        else:
            bot_logger.warning(
                f"Subscribers file {SUBSCRIBERS_FILE} does not exist yet.")
            return set()

    def save_subscribers(self):
        """Save the list of subscribed chat IDs to file."""
        try:
            with open(SUBSCRIBERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(self.subscribed_chats), f,
                          ensure_ascii=False, indent=2)
            bot_logger.info(
                f"Saved {len(self.subscribed_chats)} subscribers to {SUBSCRIBERS_FILE}")
        except Exception as e:
            bot_logger.error(
                f"Error saving subscribers to {SUBSCRIBERS_FILE}: {e}")

    def load_known_apartments(self):
        """Load the current state of apartments from the merged file into a dictionary keyed by ID."""
        if MERGED_OUTPUT_FILE.exists():
            try:
                with open(MERGED_OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Reconstruct the known apartments dictionary keyed by ID
                self.known_apartments_by_id = {
                    item['id']: item for item in data.values()}
                bot_logger.info(
                    f"Loaded {len(self.known_apartments_by_id)} apartments into known state from {MERGED_OUTPUT_FILE}")
            except json.JSONDecodeError as e:
                bot_logger.error(
                    f"Error decoding JSON from {MERGED_OUTPUT_FILE}: {e}")
                self.known_apartments_by_id = {}
            except FileNotFoundError:
                bot_logger.warning(
                    f"Merged file {MERGED_OUTPUT_FILE} does not exist yet.")
                self.known_apartments_by_id = {}
            except Exception as e:
                bot_logger.error(
                    f"Unexpected error loading {MERGED_OUTPUT_FILE}: {e}")
                self.known_apartments_by_id = {}
        else:
            bot_logger.warning(
                f"Merged file {MERGED_OUTPUT_FILE} does not exist yet.")
            self.known_apartments_by_id = {}

    def save_known_apartments(self):
        """Save the current state of known apartments to the merged file."""
        try:
            # Convert the dictionary keyed by ID back to the format expected by the JSON file (dict keyed by a unique key like ID)
            # The structure is now { "some_unique_id": {...apt_data...}, ... }
            # where the key "some_unique_id" is the value of apt_data["id"]
            with open(MERGED_OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.known_apartments_by_id, f,
                          ensure_ascii=False, indent=2)
            bot_logger.info(
                f"Saved {len(self.known_apartments_by_id)} apartments to {MERGED_OUTPUT_FILE}")
        except Exception as e:
            bot_logger.error(
                f"Error saving apartments to {MERGED_OUTPUT_FILE}: {e}")

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /start command."""
        try:
            chat_id = update.effective_message.chat_id if update.effective_message else None
            if chat_id:
                # Create inline keyboard with commands
                keyboard = [
                    [
                        InlineKeyboardButton(
                            "Subscribe", callback_data='subscribe'),
                        InlineKeyboardButton(
                            "Unsubscribe", callback_data='unsubscribe')
                    ],
                    [
                        InlineKeyboardButton(
                            "Dump All Apartments", callback_data='dumpall')
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                help_message = (
                    "🏠 **Apartment Finder Bot**\n"
                    "I can help you find new apartments on Yad2!\n"
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
                        InlineKeyboardButton(
                            "Subscribe", callback_data='subscribe'),
                        InlineKeyboardButton(
                            "Unsubscribe", callback_data='unsubscribe')
                    ],
                    [
                        InlineKeyboardButton(
                            "Dump All Apartments", callback_data='dumpall')
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                help_message = (
                    "🏠 **Apartment Finder Bot**\n"
                    "I can help you find new apartments on Yad2!\n"
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
                        bot_logger.info(
                            f"Sent dumpall apartment {i + 1}/{total_apartments} to chat {chat_id}.")
                        await asyncio.sleep(MANUAL_DUMP_MESSAGE_DELAY_SECONDS)
                    except TelegramError as e:
                        bot_logger.error(
                            f"Error sending apartment {i} to chat {chat_id}: {e}")
                        # Continue to next apartment even if one fails
                        continue
                    except Exception as e:
                        bot_logger.error(
                            f"Unexpected error sending apartment {i} to chat {chat_id}: {e}")
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
        price = apt.get('price', 'N/A')
        location = self._html_value(apt.get(
            'location', f"{apt.get('street', 'N/A')}, {apt.get('city', 'N/A')}"))
        url = html.escape(str(apt.get('apartment_page_url', 'N/A')), quote=True)
        description = self._html_value(
            self._trim_text(apt.get('description', 'No description available')))
        rooms = self._html_value(apt.get('rooms', 'N/A'))
        size = self._html_value(apt.get('size', 'N/A'))
        floor = self._html_value(apt.get('floor', 'N/A'))
        type_ = self._html_value(apt.get('type', 'Unknown'))
        tags = apt.get('tags', [])
        mamad = self._format_bool(apt.get('is_mamad', 'N/A'))
        elevator = self._format_bool(apt.get('is_elevator', 'N/A'))

        # Format price with currency symbol if it's a number
        formatted_price = f"₪{price:,}" if isinstance(
            price, (int, float)) else str(price)

        message = (
            f"<b>🏠 Apartment Found!</b>\n"
            f"<b>Type:</b> {type_}\n"
            f"<b>Price:</b> {formatted_price}\n"
            f"<b>Location:</b> {location}\n"
            f"<b>Rooms:</b> {rooms}\n"
            f"<b>Size:</b> {size} sqm\n"
            f"<b>Floor:</b> {floor}\n"
            f"<b>Mamad:</b> {mamad}\n"
            f"<b>Elevator:</b> {elevator}\n"
            f"<b>Tags:</b> {self._format_tags(tags)}\n"
            f"<b>Description:</b> {description}\n"
            f"<b>URL:</b> <a href='{url}'>Link</a>"
        )
        return message

    @staticmethod
    def _html_value(value: Any) -> str:
        return html.escape(str(value), quote=False)

    @staticmethod
    def _trim_text(value: Any) -> str:
        text = str(value)
        if len(text) <= MAX_DESCRIPTION_CHARS:
            return text
        return text[:MAX_DESCRIPTION_CHARS].rstrip() + "..."

    @staticmethod
    def _format_bool(value: Any) -> str:
        if value is True:
            return "Yes"
        if value is False:
            return "No"
        return html.escape(str(value), quote=False)

    def _format_tags(self, tags: Any) -> str:
        if not tags:
            return "N/A"
        if not isinstance(tags, list):
            return self._html_value(tags)
        return ', '.join(self._html_value(tag) for tag in tags)

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
                bot_logger.info(
                    f"Chat {chat_id} unsubscribed via inline button.")
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
                    delay = random.uniform(
                        MIN_MESSAGE_DELAY_SECONDS, MAX_MESSAGE_DELAY_SECONDS)
                    await asyncio.sleep(delay)
                except TelegramError as e:
                    bot_logger.error(
                        f"Error sending apartment {i} to chat {chat_id}: {e}")
                    continue
                except Exception as e:
                    bot_logger.error(
                        f"Unexpected error sending apartment {i} to chat {chat_id}: {e}")
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
                    bot_logger.info(
                        f"Waiting {total_wait} seconds before retrying message to {chat_id}")
                    await asyncio.sleep(total_wait)
                    try:
                        await self.application.bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
                        bot_logger.info(
                            f"Message sent successfully to chat {chat_id} after retry")
                    except TelegramError as retry_error:
                        bot_logger.error(
                            f"Retry failed for chat {chat_id}: {retry_error}")
                else:
                    bot_logger.error(
                        f"Could not parse wait time from flood control error: {e}")
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
        """Load old data, run generic scraper, compare, update internal state, save, and notify."""
        try:
            bot_logger.info("Starting scraping cycle...")

            # 1. Load the *current* state of apartments from the JSON file *before* scraping
            # This ensures the comparison is against the state *before* the new data arrives
            # Use a copy to compare against
            old_apartments_by_id = self.known_apartments_by_id.copy()

            # 2. Run the generic scraper to get the new list of apartments
            all_new_apartments = await generic_scraper.run_generic_scraper()

            # 3. Compare new apartments against old state to find new and updated
            new_items = []
            updated_items = []
            for apt in all_new_apartments:
                apt_id = apt['id']
                if apt_id in old_apartments_by_id:
                    # Apartment exists, potentially updated
                    old_apt = old_apartments_by_id[apt_id]
                    # Check if the price has changed (or any other field you care about)
                    if old_apt.get('price') != apt.get('price'):
                        updated_items.append(apt)
                        bot_logger.debug(
                            f"Price changed for existing apartment ID {apt_id}. Old: {old_apt.get('price')}, New: {apt.get('price')}")
                    else:
                        bot_logger.debug(
                            f"Existing apartment ID {apt_id} price unchanged ({apt.get('price')}).")
                    # Always update the internal state with the new data (even if only URL changed)
                    self.known_apartments_by_id[apt_id] = apt
                else:
                    # Apartment is new
                    new_items.append(apt)
                    self.known_apartments_by_id[apt_id] = apt
                    bot_logger.debug(f"New apartment found with ID {apt_id}.")
                    
            bot_logger.info(
                f"Scraping cycle completed. Received {len(new_items)} new apartments and {len(updated_items)} updated apartments from scrapers.")

            # 4. Save the updated state (new and updated apartments, plus any old ones not in the new list)
            # In this model, we keep *all* apartments found in the new scrape in the final state.
            # Apartments that disappeared from the scrape are removed from the state.
            # If you want to keep old apartments indefinitely, you'd need a different logic here.
            # For now, the state reflects the *current* scrape results.
            self.save_known_apartments()

            # 5. Notify subscribers about new and updated apartments
            # Notify about NEW apartments
            for apt in new_items:
                message = self.format_apartment_message(apt)
                for chat_id in self.subscribed_chats.copy():  # Use copy to avoid issues if set changes during iteration
                    await self.send_message_to_chat(chat_id, message)
                    bot_logger.info(
                        f"Sent new apartment notification to chat {chat_id}.")
                    await asyncio.sleep(MANUAL_DUMP_MESSAGE_DELAY_SECONDS)

            # Notify about UPDATED apartments (price changes only)
            for apt in updated_items:
                message = f"<b>🔄 Apartment Price Changed!</b>\nNew Price: ₪{apt.get('price', 'N/A'):,}\nURL: <a href='{apt.get('apartment_page_url', 'N/A')}'>Link</a>"
                for chat_id in self.subscribed_chats.copy():
                    await self.send_message_to_chat(chat_id, message)
                    # Add a random delay between sending messages to avoid rate limits
                    delay = random.uniform(
                        MIN_MESSAGE_DELAY_SECONDS, MAX_MESSAGE_DELAY_SECONDS)
                    await asyncio.sleep(delay)

            bot_logger.info(
                f"Scraping cycle finished. Notified about {len(new_items)} new and {len(updated_items)} price-changed apartments.")

        except Exception as e:
            bot_logger.error(f"Unexpected error during scraping cycle: {e}")

    async def run_polling(self):
        try:
            # Set bot commands for the command menu
            await self.application.bot.set_my_commands([
                BotCommand("start", "Show help message and command options"),
                BotCommand("help", "Show help message and command options"),
                BotCommand(
                    "subscribe", "Subscribe to receive apartment updates"),
                BotCommand("unsubscribe",
                           "Unsubscribe from apartment updates"),
                BotCommand(
                    "dumpall", "Get all apartments currently in the database")
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
