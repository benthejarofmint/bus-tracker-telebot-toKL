# all imports required for the bot to run
import json
import telebot
import os
from dotenv import load_dotenv
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import re
import gspread
from datetime import datetime
from zoneinfo import ZoneInfo
from google.oauth2 import service_account  # ✅ Correct import
import logging
import time
import functools

# ─── LOGGING SETUP ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ─── ENV + BOT INIT ───────────────────────────────────────────────────────────
# Replace 'YOUR_BOT_TOKEN' with your actual bot token
load_dotenv()
BOT_TOKEN = os.getenv('TELE_TOKEN')

bot = telebot.TeleBot(BOT_TOKEN)

def process_update_from_webhook(update_json):
    """Entry point called by main.py for webhook mode."""
    update = telebot.types.Update.de_json(json.loads(update_json))
    bot.process_new_updates([update])

# Load environment variables from .env file
# UNCOMMENT TO run locally to connect google sheets
# JSON_TOKEN = os.getenv('JSON_PATHNAME')
# gc = gspread.service_account(filename=JSON_TOKEN)
if os.getenv("GOOGLE_CREDS"):
    credentials = os.getenv("GOOGLE_CREDS")
    gc = gspread.service_account_from_dict(json.loads(credentials))
else:
    # Local fallback
    JSON_TOKEN = os.getenv('JSON_PATHNAME')
    gc = gspread.service_account(filename=JSON_TOKEN)



# # UNCOMMENT to run on cloud run, this loads the raw JSON string from the environment
# json_str = os.getenv("JSON_PATHNAME")  # or 'JSON_PATHNAME' if that’s what you're using

# if not json_str:
#     raise ValueError("Missing CREDENTIALS_JSON environment variable")

# Parse the JSON string into a Python dict
# info = json.loads(json_str)

# # Build credentials from the info
# scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)

# gc = gspread.authorize(credentials)

##DO NOT UNCOMMENT BEYOND THIS LINE HERE

GSHEET_NAME = os.getenv("GSHEET_NAME", "AL26 Bus Ops Tracking")
GSHEET_TAB = os.getenv("GSHEET_TAB", "D1")

sh = gc.open(GSHEET_NAME)

WEBHOOK_TOKEN = BOT_TOKEN  # use token in URL path
WEBHOOK_PATH = f"/{WEBHOOK_TOKEN}"
WEBHOOK_URL = os.getenv("WEBHOOK_URL") + WEBHOOK_PATH  # set this in your environment, e.g. https://your-app-name.onrender.com/<token>

# Store user sessions in memory (for a live bot, consider a DB)
user_sessions = {}
# Cache the headers for optimization
HEADER_CACHE = {}

# --- MAIN LOGIC OF THE BOT ---

# Define the sequential steps with button prompts
steps = [
    "left_star", "reached_sg_custom", "left_sg_custom",
    "reached_my_custom", "left_my_custom", "reached_rest_stop",
    "left_rest_stop", "at_30_min_mark", "reached_sunway"
]

# Human-readable prompts for each step
prompts = {
    "left_star": "Have you left Star?",
    "reached_sg_custom": "Have you reached SG Customs?",
    "left_sg_custom": "Have you left SG Customs?",
    "reached_my_custom": "Have you reached MY Customs?",
    "left_my_custom": "Have you left MY Customs?",
    "reached_rest_stop": "Have you reached the rest stop?",
    "left_rest_stop": "Have you left the rest stop?",
    "at_30_min_mark": "Are you at the toll with many tall yellow building 30mins away from Sunway?",
    "reached_sunway": "Have you reached Sunway? 🎉🚌"
}

# Doing this so that we can recover lost sessions by making it globally available
step_to_column = {
    "left_star": "Time departed from Star/PTC",
    "reached_sg_custom": "Time reach SG custom",
    "left_sg_custom": "Time leave SG custom",
    "reached_my_custom": "Time reach MY custom",
    "left_my_custom": "Time leave MY custom",
    "reached_rest_stop": "Time reach Rest Stop",
    "left_rest_stop": "Time leave Rest Stop",
    "at_30_min_mark": "Time reach 30 min mark",
    "reached_sunway": "Time bus reach sunway"
}


# ─── IMPROVEMENT 1: Retry Decorator ──────────────────────────────────────────

def retry_on_error(max_retries=3, delay=2):
    """Retries GSheet operations on API rate limit errors with exponential backoff."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (gspread.exceptions.APIError, gspread.exceptions.GSpreadException) as e:
                    logging.error(f"GSheet Error: {e}. Retrying {i+1}/{max_retries}...")
                    time.sleep(delay * (i + 1))
                except Exception as e:
                    logging.error(f"Unexpected error in GSheet op: {e}")
                    raise e
            logging.error("Max retries reached for GSheet operation.")
            return None
        return wrapper
    return decorator

# ─── COMMAND INTERCEPTOR ─────────────────────────────────────────────────────
def intercept_end_command(message, next_handler):
    text = message.text.strip().lower() if message.text else ""
    chat_id = message.chat.id

    if text == '/end':
        return end_bot(message)
    
    if text in ['/edit_pax', '/edit_plate']:
        if not user_sessions.get(chat_id, {}).get('details_confirmed', False):
            bot.send_message(chat_id,
                "⚠️ Please complete and *confirm* your bus details by filling the form first before "
                "using commands like /edit_plate or /edit_pax. Use /end and /start again to restart",
                parse_mode="Markdown")
            return
        else:
            # allow commands through normally after confirmation
            return bot.process_new_messages([message])

    return next_handler(message)

# Entry point
# ─── /start ───────────────────────────────────────────────────────────────────

@bot.message_handler(commands=['start'])
def handle_start(message):
    user_id = message.from_user.id
    if user_id in get_admin_ids():
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("📋 View All Buses", callback_data="admin_list_refresh"))
        bot.send_message(
            message.chat.id,
            "👮‍♂️ *Welcome, Admin!*\n\nYou are in Administrator Mode. You do not need to register.\n"
            "Click below to monitor the fleet.",
            reply_markup=markup,
            parse_mode="Markdown"
        )
    else:
        bot.send_message(message.chat.id,
            "🚌 Welcome! Please enter the *bus number* to begin or resume tracking:",
            parse_mode="Markdown")
        bot.register_next_step_handler(message,
            lambda msg: intercept_end_command(msg, ask_and_validate_bus_number))

# ─── REGISTRATION FLOW ────────────────────────────────────────────────────────

def is_valid_bus_number(text):
    return re.fullmatch(r"[A-Za-z]{1,2}[0-9]{1,2}", text.strip()) is not None

# @bot.message_handler(commands=['edit'])
def edit_details(message):
    chat_id = message.chat.id
    # user_sessions[chat_id] = {"step_index": 0}  # Reset session

    msg = bot.send_message(
        chat_id,
        "🔁 You’ve chosen to edit details.\nPlease re-enter the *bus number:*",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, 
        lambda msg1: intercept_end_command(msg1, ask_and_validate_bus_number))


def ask_and_validate_bus_number(message):
    chat_id = message.chat.id
    bus_number = message.text.strip()

    if not is_valid_bus_number(bus_number):
        bot.send_message(chat_id, "❌ Please enter a valid bus number (e.g., A1, B2).")
        return bot.register_next_step_handler(message, 
            lambda msg: intercept_end_command(msg, ask_and_validate_bus_number))
    
    if chat_id not in user_sessions:
        user_sessions[chat_id] = {}

    user_sessions[chat_id]['bus_number'] = bus_number

    # Try to recover session from sheet
    session = recover_session_from_sheet(chat_id, bus_number)

    if session:
        user_sessions[chat_id] = session
        bot.send_message(chat_id, 
            f"🔄 Resuming tracking for *Bus {bus_number}* from checkpoint {session['step_index'] + 1}.",
            parse_mode="Markdown")
        send_step_prompt(chat_id)
    else:
        user_sessions[chat_id] = {"step_index": 0, "bus_number": bus_number}
        bot.send_message(chat_id, 
            "🆕 New bus detected. Please enter the *Wave number* (1–5):",
            parse_mode="Markdown")
        bot.register_next_step_handler(message, handle_wave_number)

def handle_bus_recovery_check(message):
    chat_id = message.chat.id
    bus_number = message.text.strip()

    session = recover_session_from_sheet(chat_id, bus_number)

    if session:
        user_sessions[chat_id] = session
        bot.send_message(chat_id, f"🔄 Resuming tracking for *Bus {bus_number}* from checkpoint {session['step_index'] + 1}.", parse_mode="Markdown")
        send_step_prompt(chat_id)
    else:
        user_sessions[chat_id] = {"step_index": 0, "bus_number": bus_number}
        bot.send_message(chat_id, "🆕 New bus detected. Please enter the *Wave number* (1–5):", parse_mode="Markdown")
        bot.register_next_step_handler(message, lambda msg: intercept_end_command(msg,handle_wave_number))

# seems to be redundant methods ! ----------------------------------------------------

# def ask_wave_number(message):
#     user_sessions[message.chat.id] = {"step_index": 0}
#     bot.send_message(message.chat.id, "Please enter the *Wave number* (single digit):", parse_mode="Markdown")
#     bot.register_next_step_handler(message, lambda msg: intercept_end_command(msg,handle_wave_number))

# def ask_bus_number(message):
#     # user_sessions[message.chat.id] = {"step_index": 0}
#     bot.send_message(message.chat.id, "Please enter the bus number:")
#     #input data handling to sheets here
#     bot.register_next_step_handler(message, lambda msg: intercept_end_command(msg, ask_and_validate_bus_plate))

def handle_wave_number(message):
    chat_id = message.chat.id
    wave = message.text.strip()

    # ✅ Exit early if user sends /end
    if wave.lower() == "/end":
        return end_bot(message)

    if not wave.isdigit() or not (0 <= int(wave) <= 6):
        bot.send_message(chat_id, "❌ Please enter a valid Wave number (0–6).")
        return bot.register_next_step_handler(message,lambda msg: intercept_end_command(msg, handle_wave_number))

    user_sessions[chat_id]['wave'] = wave
    bot.send_message(chat_id, 
        "Please enter the *CGs' names* (comma-separated if more than one) Eg. NP1 NPD, NP1 NPG:", 
        parse_mode="Markdown")
    bot.register_next_step_handler(message,
        lambda msg: intercept_end_command(msg, handle_cgs_input))


def handle_cgs_input(message):
    chat_id = message.chat.id
    cgs = message.text.strip()

    if not cgs:
        bot.send_message(chat_id, "❌ Please enter valid CGs' names.")
        return bot.register_next_step_handler(message, 
            lambda msg: intercept_end_command(msg,handle_cgs_input))

    user_sessions[chat_id]['cgs'] = cgs
    bot.send_message(chat_id, "Please enter the *bus plate number*:", parse_mode="Markdown")
    bot.register_next_step_handler(message, 
        lambda msg: intercept_end_command(msg, ask_and_validate_bus_plate))



def ask_and_validate_bus_plate(message):
    chat_id = message.chat.id
    plate = message.text.strip().upper()

    if not re.fullmatch(r"(?=.*[A-Z])[A-Z0-9\- ]{3,15}", plate):
        bot.send_message(chat_id, 
            "❌ Please enter a valid bus plate number (e.g. 'ABC1234' or 'SGX-1234').")
        return bot.register_next_step_handler(message, 
            lambda msg: intercept_end_command(msg,ask_and_validate_bus_plate))

    user_sessions[chat_id]['bus_plate'] = plate
    bot.send_message(chat_id, "Please enter the Bus IC's name:")
    bot.register_next_step_handler(message, 
        lambda msg: intercept_end_command(msg,ask_bus_ic_name))

def ask_bus_plate_number(message):
    chat_id = message.chat.id
    plate = message.text.strip().upper()

    # Basic validation: alphanumeric + hyphens
    if not re.fullmatch(r"[A-Z0-9\- ]{3,15}", plate):
        bot.send_message(chat_id, "❌ Please enter a valid bus plate number (e.g. 'ABC1234' or 'SGX1234').")
        return bot.register_next_step_handler(message, lambda msg: intercept_end_command(msg,ask_bus_plate_number))

    user_sessions[chat_id]['bus_plate'] = plate
    bot.send_message(chat_id, "Please enter the Bus IC's name:")
    bot.register_next_step_handler(message, lambda msg: intercept_end_command(msg,ask_bus_ic_name))


def ask_bus_ic_name(message):
    chat_id = message.chat.id
    name = message.text.strip()

    if not is_valid_name(name):
        bot.send_message(chat_id, "❌ Please enter a valid name for the Bus IC (letters only).")
        return bot.register_next_step_handler(message, 
            lambda msg: intercept_end_command(msg, ask_bus_ic_name))

    user_sessions[chat_id]['bus_ic'] = name
    bot.send_message(chat_id, "Please enter the Bus 2IC's name:")
    bot.register_next_step_handler(message, 
        lambda msg: intercept_end_command(msg, ask_2ic))


def ask_2ic(message):
    chat_id = message.chat.id
    if not is_valid_name(message.text):
        bot.send_message(chat_id, "❌ Please enter a valid name for the Bus 2IC (letters only).")
        return bot.register_next_step_handler(message, 
            lambda msg: intercept_end_command(msg,ask_2ic))

    user_sessions[chat_id]['bus_2ic'] = message.text
    bot.send_message(chat_id, "Please enter the total number of people on board:")
    bot.register_next_step_handler(message, 
        lambda msg: intercept_end_command(msg,ask_passenger_count))


def ask_passenger_count(message):
    chat_id = message.chat.id
    passenger_count = message.text.strip()

    # Store first
    user_sessions[chat_id]['passenger_count'] = passenger_count

    # Then validate
    if not passenger_count.isdigit() or int(passenger_count) <= 0:
        bot.send_message(chat_id, "❌ Please enter a valid number for passenger count. E.g. 40")
        return bot.register_next_step_handler(message, 
            lambda msg: intercept_end_command(msg,ask_passenger_count))

    # If valid, proceed
    confirm_user_details(message)



def confirm_user_details(message):
    chat_id = message.chat.id
    session = user_sessions[chat_id]
    # user_sessions[chat_id]['passenger_count'] = message.text
    session['passenger_count'] = message.text.strip()

    # Assign row dynamically
    row = get_or_create_user_row(session['bus_number'])
    session['row'] = row  # Store for future logging

    session = user_sessions[chat_id]
    summary = (
    f"🚌 *Your entered details:*\n\n"
    f"*Bus Number:* {session['bus_number']}\n"
    f"*Wave:* {session['wave']}\n"
    f"*CGs:* {session['cgs']}\n"
    f"*Bus Plate:* {session['bus_plate']}\n"
    f"*Bus IC:* {session['bus_ic']}\n"
    f"*Bus 2IC:* {session['bus_2ic']}\n"
    f"*Passenger Count:* {session['passenger_count']}\n\n"
    f"✅ If everything is correct, click *Continue*.\n"
    f"🔁 If you'd like to change anything, click *Edit*."
    )


    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("✅ Continue", callback_data="confirm_details"),
        InlineKeyboardButton("🔁 Edit", callback_data="edit_details")
    )

    bot.send_message(chat_id, summary, reply_markup=markup, parse_mode="Markdown")

def start_checkpoint_flow(message):
    # user_sessions[message.chat.id]['passenger_count'] = message.text
    send_step_prompt(message.chat.id)

def send_step_prompt(chat_id):
    step_index = user_sessions[chat_id]["step_index"]
    if step_index >= len(steps):
        bot.send_message(chat_id,
            "🎉 Congratulations! You've successfully reached Star safely. "
            "Thank you for your effort 🙌\n\n"
            "A few final reminders to wrap up the journey smoothly:\n\n"
            "• Boys head down first to unload, followed by the girls 🚶‍♂️🚶‍♀️\n\n"
            "• Double-check that everyone has all their belongings 🎒📱\n\n"
            "• Don't forget to collect the bus IC packs, signages, tracker, and masks — and please pass them back to the FTS at Star 🎭📦\n\n"
            "• Lastly, make sure all trash is properly disposed of on your own! 🗑️\n\n"
            "Please send /end to terminate this bot. Great job, team!"
        )
        return
    step_key = steps[step_index]
    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton(text="⬅️ Back", callback_data="go_back"),
        InlineKeyboardButton(text="✅ Yes", callback_data=f"yes_{step_key}")
    )
    bot.send_message(chat_id, f"{prompts[step_key]} (Click only when confirmed)", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: True)
def handle_step_callback(call):
    chat_id = call.message.chat.id
    data    = call.data

    # ── Admin callbacks (no session required) ─────────────────────────────────
    if data in ("admin_list_refresh", "admin_back"):
        _send_admin_list(chat_id, message_id=call.message.message_id)
        return

    if data == "admin_report":
        _generate_fleet_report(chat_id, message_id=call.message.message_id)
        return

    if data.startswith("cb_"):
        _show_bus_detail(call)
        return
    session = user_sessions.get(chat_id)

    if not session:
        bot.send_message(chat_id, "Session not found. Please /start again.")
        return


    if data == "go_back":
        if session["step_index"] > 0:
            # clear_cell(chat_id)
            # IMPROVEMENT 4: dynamic clear using step_to_column (not hardcoded math)
            step_to_undo = steps[session["step_index"] - 1]
            clear_cell(chat_id, step_to_undo)
            session["step_index"] -= 1
            current_step = steps[session["step_index"]]
            logging.info(f"⬅️ User {chat_id} went back to step {session['step_index']} ({current_step})")
            bot.send_message(chat_id,
                f"⬅️ You have moved back to: *{prompts[current_step]}*",
                parse_mode="Markdown")
        else:
            logging.info(f"[INFO] ⬅️ User {chat_id} already at first step, can't go back further")
            bot.send_message(chat_id, "⚠️ You're already at the first checkpoint. Cannot go back further.")

        send_step_prompt(chat_id)


   
    elif data.startswith("yes_"):
        print(f"[CALLBACK] ✅ Button Pressed: {data}")  # ✅ log button press
        step_key = data[4:]
        expected_step = steps[session["step_index"]]
        logging.debug(f"[DEBUG] step_key: {step_key}, expected_step: {expected_step}, step_index: {session['step_index']}")

        if step_key == expected_step:
            # log_to_excel_placeholder(chat_id, step_key)
            session['awaiting_passenger_count_step'] = step_key

            # 🎯 Custom reminder after MY Customs
            if step_key == "left_sg_custom":
                bot.send_message(
                    chat_id,
                    "🔔 *Reminder for Bus IC:*\nPlease put back the event signages at the:\n"
                    "- 🪧 *Front*\n"
                    "- 🔲 *Left side*\n"
                    "- 🪧 *Rear* of the bus.",
                    parse_mode="Markdown"
                )

                bot.send_message(
                    chat_id,
                    "🔔 *Reminder for Bus IC:*\nPlease remember to do a passport check with everyone in the bus too!\n",
                    parse_mode="Markdown"
                )

            if step_key == "left_my_custom":
                bot.send_message(
                    chat_id,
                    "🔔 *Reminder for Bus IC:*\nPlease remember to do a passport check with everyone in the bus!\n",
                    parse_mode="Markdown"
                )
            if step_key == "left_my_custom":
                bot.send_message(
                    chat_id,
                    "🔔 *Reminder for Bus IC:*\nPlease bring down at the SG customs:\n"
                    "- 🪧 *3 Bus Signages (Front, Left, Rear)*\n"
                    "- 😷 *Surgical masks*\n"
                    "- 🎒 *ALL BELONGINGS*",
                    parse_mode="Markdown"
                )

            prompt_passenger_count(chat_id, step_key)
            # bot.register_next_step_handler(message, handle_passenger_count_after_step)
        else:
            logging.warning("[WARNING] Mismatch: button step vs current expected step")

            
    elif data == "confirm_details":
        user_sessions[chat_id]['details_confirmed'] = True
        chat_id = call.message.chat.id
        bot.send_message(chat_id, "⏳ Saving your details to Google Sheet...")

        try:
            log_initial_details_to_sheet(chat_id)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Failed to save details: {e}")
            return

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🟢 Okay", callback_data="begin_checklist"))
        bot.send_message(
                    chat_id,
                    "🔔 *Reminder for Bus IC:*\nPlease *put up* the bus signages at the:\n"
                    "- 🪧 *Front*\n"
                    "- 🔲 *Left side*\n"
                    "- 🪧 *Rear* of the bus.",
                    parse_mode="Markdown"
                )
        bot.send_message(chat_id, "Great! Please click the button below to begin the journey checklist.", reply_markup=markup)

    elif data == "begin_checklist":
       start_checkpoint_flow(call.message)

    elif data == "edit_details":
        # bot.send_message(call.message.chat.id, "Let’s start over. Please enter the bus number:")
        user_sessions[call.message.chat.id] = {"step_index": 0}
        edit_details(call.message) 

def prompt_passenger_count(chat_id, step_key):
    user_sessions[chat_id]['awaiting_passenger_count_step'] = step_key
    msg = bot.send_message(
        chat_id,
        f"👥 Please enter the *current passenger count* after '{prompts[step_key]}':",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, lambda msg1: intercept_end_command(msg1,handle_passenger_count_after_step))

    

def handle_passenger_count_after_step(message):
    chat_id = message.chat.id
    text = message.text.strip()
    passenger_count = message.text.strip()
    logging.info(f"[INPUT] 👥 Received passenger count: '{passenger_count}' from user {chat_id}")

    if text.startswith("/edit_pax"):
        return handle_edit_pax(message)

    if text.startswith("/edit_plate"):
        return handle_edit_plate(message)

    if text.startswith("/end"):
        return end_bot(message)

    if not passenger_count.isdigit():
        logging.error("[ERROR] ❌ Invalid passenger count input")
        bot.send_message(chat_id, "❌ Please enter a valid number for passenger count.")
        return bot.register_next_step_handler(message, lambda msg: intercept_end_command(msg,handle_passenger_count_after_step))

    step_key = user_sessions[chat_id].get('awaiting_passenger_count_step')
    if not step_key:
        logging.error("[ERROR] ❌ Missing step key during count logging")
        bot.send_message(chat_id, "⚠️ No step context found. Please try again.")
        return

    #handle if the passenger count does not match the original number
    current_pax = int(passenger_count)
    expected_pax = int(user_sessions[chat_id].get('passenger_count', current_pax))
    
    if 'passenger_log' not in user_sessions[chat_id]:
        user_sessions[chat_id]['passenger_log'] = []

    logging.debug(f"[DEBUG] expected: {expected_pax}, current: {current_pax}")

    if current_pax != expected_pax:
        user_sessions[chat_id]['pending_pax_mismatch'] = {
            'step_key': step_key,
            'actual_count': current_pax,
            'expected_count': expected_pax
        }
        msg = bot.send_message(
            chat_id,
            f"⚠️ Passenger count mismatch (Expected: {expected_pax}, Now: {current_pax}).\n"
            f"Please enter a reason to include in the Remarks column:"
        )
        return bot.register_next_step_handler(msg, lambda msg1: intercept_end_command(msg1,handle_mismatch_reason))

    user_sessions[chat_id]['passenger_log'].append({
        'step': step_key,
        'count': int(passenger_count)
    })

    logging.info(f"[LOG] ✅ Saved count: {passenger_count} for step: {step_key} (User: {chat_id})")
    logging.debug(f"[STATE] Full log for user {chat_id}: {user_sessions[chat_id]['passenger_log']}")

    # ✅ NEW: Log time + checkbox to Google Sheet
    # log_checkpoint_to_sheet(chat_id, step_key)
    bot.send_message(chat_id, "⏳ Uploading checkpoint to sheet...")

    try:
        log_checkpoint_to_sheet(chat_id, step_key)
        bot.send_message(chat_id, "✅ Checkpoint successfully saved.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Failed to save checkpoint: {e}")

    # threading.Thread(
    #   target=log_checkpoint_to_sheet,
    #   args=(chat_id, step_key)
    # ).start()
    # bot.send_message(chat_id, "✅ Passenger count recorded.")

    user_sessions[chat_id]['step_index'] += 1
    send_step_prompt(chat_id)

def handle_mismatch_reason(message):
    chat_id = message.chat.id
    reason = message.text.strip()
    text = message.text.strip()
    mismatch = user_sessions[chat_id].pop('pending_pax_mismatch', None)

    if text.startswith("/edit_pax"):
        return handle_edit_pax(message)
    if text.startswith("/edit_plate"):
        return handle_edit_plate(message)
    if text.startswith("/end"):
        return end_bot(message)

    if not mismatch:
        bot.send_message(chat_id, "⚠️ No mismatch context found. Please retry the step.")
        return


    # Ensure passenger_log exists
    if 'passenger_log' not in user_sessions[chat_id]:
        user_sessions[chat_id]['passenger_log'] = []

    # Log count
    user_sessions[chat_id]['passenger_log'].append({
        'step': mismatch['step_key'],
        'count': mismatch['actual_count']
    })

    # ✅ Now log to sheet, with red remark
    # log_checkpoint_to_sheet(
    #   chat_id,
    #    mismatch['step_key'],
    #    actual_pax=mismatch['actual_count'],
    #    expected_pax=mismatch['expected_count'],
    #    remark=reason
    # )

    bot.send_message(chat_id, "⏳ Uploading checkpoint and remarks to sheet...")

    try:
        log_checkpoint_to_sheet(
            chat_id,
            mismatch['step_key'],
            actual_pax=mismatch['actual_count'],
            expected_pax=mismatch['expected_count'],
            remark=reason
        )
        bot.send_message(chat_id, "✅ Checkpoint and remarks successfully saved.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Failed to save checkpoint with remark: {e}")

    # Update the expected pax count to the new actual count so future checkpoints
    # compare against the latest confirmed headcount, not the original registration number.
    user_sessions[chat_id]['passenger_count'] = str(mismatch['actual_count'])
    logging.info(f"[PAX] Updated expected pax for user {chat_id} to {mismatch['actual_count']}")
    
    # threading.Thread(
    #   target=log_checkpoint_to_sheet,
    #   args=(chat_id, mismatch['step_key']),
    #   kwargs={
    #       "actual_pax": mismatch['actual_count'],
    #       "expected_pax": mismatch['expected_count'],
    #       "remark": reason
    #   }
    # ).start()
    # bot.send_message(chat_id, "✅ Passenger count and remark recorded.")
    user_sessions[chat_id]['step_index'] += 1
    send_step_prompt(chat_id)




#validation helper function.
def is_valid_name(text):
    return re.fullmatch(r"[A-Za-z\s\-]+", text.strip()) is not None



    
@bot.message_handler(commands=['end'])
def end_bot(message):
    chat_id = message.chat.id
    # user_sessions.pop(chat_id, None)
    bot.send_message(chat_id, "✅ Your session has been terminated. You can restart anytime with /start.")
    user_sessions.pop(message.chat.id, None)


# it will check by bus number and see if the user has an existing code

def get_column_mapping(worksheet):
    title = worksheet.title  # e.g. 'D1'

    if title in HEADER_CACHE:
        return HEADER_CACHE[title]

    header_row = worksheet.row_values(1)
    column_map = {header.strip().lower(): idx + 1 for idx, header in enumerate(header_row)}

    HEADER_CACHE[title] = column_map
    return column_map
@retry_on_error()
def get_or_create_user_row(bus_number):
    """IMPROVEMENT 3: Find row by looking up the Bus # column header, not hardcoded col A."""
    worksheet   = sh.worksheet(GSHEET_TAB)
    columns     = get_column_mapping(worksheet)
    bus_col_idx = columns.get("bus #", 2)          # default to col 2 if header missing
    bus_numbers = worksheet.col_values(bus_col_idx)
    # worksheet = sh.worksheet(GSHEET_TAB)
    # bus_numbers = worksheet.col_values(1)  # Assuming column A has bus numbers

    for i, existing in enumerate(bus_numbers):
        if existing.strip().lower() == bus_number.strip().lower():
            return i + 1  # gspread uses 1-based indexing

    # If not found, append a new row
    new_row_index = len(bus_numbers) + 1
    # worksheet.update_cell(new_row_index, 1, bus_number)
    return new_row_index


def clear_cell(chat_id, step_key):
    session = user_sessions[chat_id]
    # step_index = session["step_index"]
    # row = session.get("row", 2)

    # col_time = 8 + (3 * step_index)
    # col_true = 9 + (3 * step_index)
    # worksheet = sh.worksheet(GSHEET_TAB)
    # worksheet.update_cell(row, col_time, '')
    # worksheet.update_cell(row, col_true, '')
    row       = session.get("row", 2)
    worksheet = sh.worksheet(GSHEET_TAB)
    columns   = get_column_mapping(worksheet)

    col_name  = step_to_column.get(step_key, "").strip().lower()
    if not col_name or col_name not in columns:
        logging.warning(f"[clear_cell] Column for step '{step_key}' not found, skipping.")
        return

    time_col = columns[col_name]
    tele_col = time_col + 1

    # IMPROVEMENT 2: batch_update instead of two individual update_cell calls
    worksheet.batch_update([
        {'range': gspread.utils.rowcol_to_a1(row, time_col), 'values': [['']]},
        {'range': gspread.utils.rowcol_to_a1(row, tele_col), 'values': [['']]},
    ])
    logging.info(f"[LOG] {chat_id} cleared step '{step_key}' at row {row}")

# this logs the bus number, bus plate, no. of pax, bus ic and bus 2ic down into the sheet.
@retry_on_error()
def log_initial_details_to_sheet(chat_id):
    session = user_sessions[chat_id]
    row = session['row']
    worksheet = sh.worksheet(GSHEET_TAB)
    col_map   = get_column_mapping(worksheet)

    try:
        # IMPROVEMENT 2: batch_update with column header lookup
        field_map = {
            'wave':       session['wave'],
            'bus #':      session['bus_number'],
            'bus plate':  session['bus_plate'],
            'no. of pax': session['passenger_count'],
            'bus ic':     session['bus_ic'],
            'bus 2ic':    session['bus_2ic'],
            'cgs':        session['cgs'],
            'chat id':    str(chat_id),
        }
        updates = [
            {'range': gspread.utils.rowcol_to_a1(row, col_map[h]), 'values': [[v]]}
            for h, v in field_map.items() if h in col_map
        ]
        if updates:
            worksheet.batch_update(updates)

    except KeyError as e:
        bot.send_message(chat_id, f"❌ Column header not found in sheet: {e}")
        logging.error(f"[ERROR] Column not found: {e}")
        return
    except Exception as e:
        bot.send_message(chat_id, f"❌ Failed to update Google Sheet: {e}")
        logging.error(f"[ERROR] Google Sheet update failed: {e}")
        return

    logging.info(f"[LOG] Initial bus info saved dynamically for user {chat_id} at row {row}")

# this is code to log each checkpoint.
@retry_on_error()
def log_checkpoint_to_sheet(chat_id, step_key, actual_pax=None, expected_pax=None, remark=None):
    session = user_sessions[chat_id]
    row = session['row']
    worksheet = sh.worksheet(GSHEET_TAB)
    columns = get_column_mapping(worksheet)

    # step_to_column is a global var
    if step_key not in step_to_column:
        logging.info(f"[INFO] No sheet mapping for step '{step_key}', skipping log.")
        return

    time_col_name = step_to_column[step_key].strip().lower()
    try:
        time_col_index = columns[time_col_name]
        tele_col_index = time_col_index + 1  # "Tele" column is always next
        remarks_col_index = tele_col_index + 1
        current_time = datetime.now(ZoneInfo("Asia/Singapore")).strftime("%H:%M")

        # IMPROVEMENT 2: batch_update for time + checkbox
        updates = [
            {'range': gspread.utils.rowcol_to_a1(row, time_col_index), 'values': [[current_time]]},
            {'range': gspread.utils.rowcol_to_a1(row, tele_col_index), 'values': [[True]]},
        ]

        if remark:
            updates.append({'range': gspread.utils.rowcol_to_a1(row, remarks_col_index),
                            'values': [[remark]]})
            worksheet.batch_update(updates)
            worksheet.format(gspread.utils.rowcol_to_a1(row, remarks_col_index),
                             {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}})
        else:
            updates.append({'range': gspread.utils.rowcol_to_a1(row, remarks_col_index),
                            'values': [['']]})
            worksheet.batch_update(updates)
            worksheet.format(gspread.utils.rowcol_to_a1(row, remarks_col_index),
                             {"backgroundColor": {"red": 1, "green": 1, "blue": 1}})


    except KeyError as e:
        print(f"[ERROR] Column header not found: {e}")
        return
    
    logging.info(f"[LOG] Logged step '{step_key}' at {current_time} for user {chat_id} in row {row}")

# if user filling halfway we recover the session.
@retry_on_error()
def recover_session_from_sheet(chat_id, bus_number):
    worksheet = sh.worksheet(GSHEET_TAB)

    columns = get_column_mapping(worksheet)
    bus_col_index = columns.get("bus #")  # Get index from header

    if not bus_col_index:
        logging.error("[ERROR] 'Bus #' column not found in header.")
        return None

    bus_numbers = worksheet.col_values(bus_col_index)
    # bus_numbers = worksheet.col_values(2)  # Column 2 = "Bus #" (1-indexed)

    for i, b in enumerate(bus_numbers):
        if b.strip().lower() == bus_number.strip().lower():
            row = i + 1
            values = worksheet.row_values(row)
            col_map = get_column_mapping(worksheet)

            # Helper to safely extract a value by header name
            def safe_get(col_name):
                idx = col_map.get(col_name.strip().lower())
                return values[idx - 1].strip() if idx and len(values) >= idx else ""

            # Extract fields
            wave = safe_get("wave")
            cgs = safe_get("cgs")
            bus_plate = safe_get("bus plate")
            pax = safe_get("no. of pax")
            bus_ic = safe_get("bus ic")
            bus_2ic = safe_get("bus 2ic")

            # Step recovery
            step_index = 0
            for step in steps:
                col_name = step_to_column.get(step)
                col_idx = col_map.get(col_name.strip().lower())
                if col_idx and len(values) >= col_idx and values[col_idx - 1].strip():
                    step_index += 1
                else:
                    break

            return {
                "step_index": step_index,
                "bus_number": bus_number,
                "row": row,
                "wave": wave,
                "cgs": cgs,
                "bus_plate": bus_plate,
                "passenger_count": pax,
                "bus_ic": bus_ic,
                "bus_2ic": bus_2ic,
                "details_confirmed": True
            }

    return None

@bot.message_handler(commands=['edit_plate'])
def handle_edit_plate(message):
    chat_id = message.chat.id

    if chat_id not in user_sessions:
        bot.send_message(chat_id, "⚠️ No active session found. Please register all details first. Use /end and /start again to restart the bot.")
        return
    
    if not user_sessions[chat_id].get('details_confirmed', False):
        bot.send_message(chat_id, "❌ You must confirm your bus details before editing. Please complete the setup first.")
        return
    
    bot.send_message(chat_id, "✏️ Please enter the *new bus plate number*:", parse_mode="Markdown")
    bot.register_next_step_handler(message, lambda msg: intercept_end_command(msg, update_plate_number))

def update_plate_number(message):
    chat_id = message.chat.id
    plate = message.text.strip().upper()

    if not re.fullmatch(r"(?=.*[A-Z])[A-Z0-9\- ]{3,15}", plate):
        bot.send_message(chat_id, "❌ Invalid format. Please enter a valid bus plate number (e.g. 'ABC1234').")
        return bot.register_next_step_handler(message, update_plate_number)

    # Update in-memory session
    user_sessions[chat_id]['bus_plate'] = plate

    # # Update Google Sheet in thread
    # threading.Thread(
    #     target=_update_plate_number_sync,
    #     args=(chat_id, plate)
    # ).start()

    bot.send_message(chat_id, f"🔄 Updating Google Sheet with new plate *{plate}*...", parse_mode="Markdown")
    _update_plate_number_sync(chat_id, plate)

def _update_plate_number_sync(chat_id, plate):
    if 'row' not in user_sessions[chat_id]:
        print(f"[INFO] No row assigned yet for chat_id {chat_id}")
        return

    try:
        row = user_sessions[chat_id]['row']
        worksheet = sh.worksheet(GSHEET_TAB)
        columns = get_column_mapping(worksheet)

        col_index = columns.get("bus plate")
        if col_index:
            worksheet.update_cell(row, col_index, plate)
            worksheet.format(gspread.utils.rowcol_to_a1(row, col_index), {
            "backgroundColor": {"red": 0.8, "green": 1.0, "blue": 0.8}  # Light yellow
            })
            
            bot.send_message(chat_id, f"✅ Bus plate updated to *{plate}* in Google Sheet.", parse_mode="Markdown")
            send_step_prompt(chat_id)
        else:
            bot.send_message(chat_id, "⚠️ 'Bus Plate' column not found in sheet.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Failed to update Google Sheet: {e}")
        logging.error(f"[ERROR] Updating plate failed for {chat_id}: {e}")

@bot.message_handler(commands=['edit_pax'])
def handle_edit_pax(message):
    chat_id = message.chat.id

    if chat_id not in user_sessions:
        bot.send_message(chat_id, "⚠️ No active session found. Please register all details first. Use /end and /start again to restart the bot.")
        return

    if not user_sessions[chat_id].get('details_confirmed'):
        bot.send_message(chat_id, "❌ You must confirm your bus details before editing. Please complete the setup first.")
        return

    bot.send_message(chat_id, "✏️ Please enter the *new passenger count*:", parse_mode="Markdown")
    bot.register_next_step_handler(message, lambda msg: intercept_end_command(msg, update_pax))

def update_pax(message):
    chat_id = message.chat.id
    try:
        pax = int(message.text.strip())

        if pax < 1 or pax > 100:  # Adjust based on your limit
            bot.send_message(chat_id, "❌ Invalid input. Please enter a valid number of passengers (1-100).")
            return bot.register_next_step_handler(message, update_pax)

        # Update in-memory session
        user_sessions[chat_id]['passenger_count'] = str(pax)

        # Update Google Sheet in a separate thread
        # threading.Thread(
        #     target=_update_pax_sync,
        #     args=(chat_id, pax)
        # ).start()

        bot.send_message(chat_id, f"🔄 Updating Google Sheet with new passenger count *{pax}*...", parse_mode="Markdown")
        _update_pax_sync(chat_id, pax)
    
    except ValueError:
        bot.send_message(chat_id, "❌ Invalid input. Please enter a valid number for passengers.")
        return bot.register_next_step_handler(message, update_pax)

def _update_pax_sync(chat_id, pax):
    if 'row' not in user_sessions[chat_id]:
        logging.info(f"[INFO] No row assigned yet for chat_id {chat_id}")
        return

    try:
        row = user_sessions[chat_id]['row']
        worksheet = sh.worksheet(GSHEET_TAB)
        columns = get_column_mapping(worksheet)

        col_index = columns.get("no. of pax")
        if col_index:
            worksheet.update_cell(row, col_index, pax)

            worksheet.format(gspread.utils.rowcol_to_a1(row, col_index), {
            "backgroundColor": {"red": 0.8, "green": 1.0, "blue": 0.8}  # Light yellow
            })

            bot.send_message(chat_id, f"✅ Passenger count updated to *{pax}* in Google Sheet.", parse_mode="Markdown")
            send_step_prompt(chat_id)
        else:
            bot.send_message(chat_id, "⚠️ 'Passenger Count' column not found in sheet.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Failed to update Google Sheet: {e}")
        logging.error(f"[ERROR] Updating pax failed for {chat_id}: {e}")

# ─── ADMIN: HELPERS ───────────────────────────────────────────────────────────

def get_admin_ids():
    raw_ids = os.getenv("ADMIN_IDS", "")
    return [int(x.strip()) for x in raw_ids.split(",") if x.strip().isdigit()]

def create_progress_bar(steps_done, total_steps):
    if total_steps == 0:
        return "⬜" * 10 + " 0%"
    percent = int((steps_done / total_steps) * 100)
    filled  = int(steps_done * 10 // total_steps)
    empty   = 10 - filled
    return "🟩" * filled + "⬜" * empty + f" {percent}%"

# ─── ADMIN: /list ─────────────────────────────────────────────────────────────

@bot.message_handler(commands=['list'])
def admin_list_buses(message):
    if message.from_user.id not in get_admin_ids():
        return  # silently ignore non-admins
    _send_admin_list(message.chat.id)

def _send_admin_list(chat_id, message_id=None):
    """Send (or edit) the admin bus-list panel with a 📊 Generate Report button."""
    try:
        worksheet = sh.worksheet(GSHEET_TAB)
        raw_data  = worksheet.get_all_values()

        if not raw_data or len(raw_data) < 2:
            bot.send_message(chat_id, "No data found in sheet.")
            return

        headers_lower = [h.strip().lower() for h in raw_data[0]]
        try:
            bus_col_idx = headers_lower.index('bus #')
        except ValueError:
            bot.send_message(chat_id, "⚠️ Error: 'Bus #' column not found in headers.")
            return

        markup  = InlineKeyboardMarkup(row_width=3)
        buttons = []
        for i, row in enumerate(raw_data[1:]):
            bus_no = row[bus_col_idx].strip() if bus_col_idx < len(row) else ""
            if bus_no:
                buttons.append(InlineKeyboardButton(f"🚍 {bus_no}", callback_data=f"cb_{i}"))

        if buttons:
            markup.add(*buttons)
        markup.row(InlineKeyboardButton("📊 Generate Report", callback_data="admin_report"))

        panel_text = "📋 *Admin Panel: Select a Bus*"
        if message_id:
            bot.edit_message_text(
                chat_id=chat_id, message_id=message_id,
                text=panel_text, reply_markup=markup, parse_mode="Markdown")
        else:
            bot.send_message(chat_id, panel_text, reply_markup=markup, parse_mode="Markdown")

    except Exception as e:
        logging.error(f"Error fetching admin bus list: {e}")
        bot.send_message(chat_id, f"⚠️ Error: {str(e)}")

def _show_bus_detail(call):
    """Show individual bus checkpoint status for admin."""
    chat_id = call.message.chat.id
    try:
        data_row_index = int(call.data.split("_")[1])
        worksheet      = sh.worksheet(GSHEET_TAB)
        raw_data       = worksheet.get_all_values()
        headers_lower  = [h.strip().lower() for h in raw_data[0]]
        actual_row     = raw_data[data_row_index + 1]

        try:
            bus_col_idx = headers_lower.index('bus #')
            bus_num     = actual_row[bus_col_idx].strip()
        except (ValueError, IndexError):
            bus_num = "??"

        def safe_col(header):
            try:
                idx = headers_lower.index(header)
                return actual_row[idx].strip() if idx < len(actual_row) else ""
            except ValueError:
                return ""

        bus_ic      = safe_col('bus ic')
        bus_2ic     = safe_col('bus 2ic')
        pax         = safe_col('no. of pax')
        chat_id_val = safe_col('chat id')

        # Make the Bus IC name a deep link if we have their chat ID
        ic_display = f"[{bus_ic}](tg://user?id={chat_id_val})" if chat_id_val else bus_ic

        steps_done   = 0
        status_lines = []
        for step_key in steps:
            col_name = step_to_column[step_key].strip().lower()
            try:
                col_idx  = headers_lower.index(col_name)
                time_val = actual_row[col_idx].strip() if col_idx < len(actual_row) else ""
            except ValueError:
                time_val = ""

            if time_val:
                steps_done += 1
                status_lines.append(f"✅ {prompts[step_key]}: {time_val}")
            else:
                status_lines.append(f"⏳ {prompts[step_key]}: _Pending_")

        prog_bar    = create_progress_bar(steps_done, len(steps))
        status_text = "\n".join(status_lines)

        msg = (
            f"🚍 *Bus {bus_num} Report*\n"
            f"👤 Bus IC: {ic_display} | 2IC: {bus_2ic}\n"
            f"👥 Pax on board: {pax}\n"
            f"{prog_bar}\n"
            f"{'─' * 30}\n"
            f"{status_text}"
        )

        back_markup = InlineKeyboardMarkup()
        back_markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_back"))
        bot.edit_message_text(
            chat_id=chat_id, message_id=call.message.message_id,
            text=msg, parse_mode="Markdown", reply_markup=back_markup)

    except Exception as e:
        logging.error(f"Error showing bus detail: {e}")
        bot.answer_callback_query(call.id, "Error loading details.")

def _generate_fleet_report(chat_id, message_id):
    """Generate and display the fleet-wide checkpoint count report."""
    try:
        worksheet = sh.worksheet(GSHEET_TAB)
        raw_data  = worksheet.get_all_values()

        if not raw_data or len(raw_data) < 2:
            bot.send_message(chat_id, "No data found in sheet.")
            return

        headers_lower = [h.strip().lower() for h in raw_data[0]]
        data_rows     = raw_data[1:]
        now           = datetime.now(ZoneInfo("Asia/Singapore")).strftime("%H:%M:%S")

        lines = ["🚌 *ARROW BUS REPORT*\n"]
        for step_key in steps:
            col_name  = step_to_column[step_key].strip().lower()
            col_idx_0 = next((i for i, h in enumerate(headers_lower) if h == col_name), None)
            count = 0
            if col_idx_0 is not None:
                for row_vals in data_rows:
                    if col_idx_0 < len(row_vals) and row_vals[col_idx_0].strip():
                        count += 1
            lines.append(f"*{prompts[step_key]}*: {count} bus(es)")

        lines.append(f"\n_as of {now}_")
        report_text = "\n".join(lines)

        back_markup = InlineKeyboardMarkup()
        back_markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_list_refresh"))
        bot.edit_message_text(
            chat_id=chat_id, message_id=message_id,
            text=report_text, parse_mode="Markdown", reply_markup=back_markup)

    except Exception as e:
        logging.error(f"Error generating fleet report: {e}")
        bot.send_message(chat_id, f"❌ Failed to generate report: {e}")
# testing !!
# ─── POLLING MODE (for local testing) ────────────────────────────────────────
# Run this file directly to test with polling.
# For production (Cloud Run + webhook), run main.py instead.

if __name__ == "__main__":
    logging.info("🚌 Bot is running in POLLING mode (local testing)...")
    logging.info("   → For webhook/Cloud Run mode, run main.py instead.")
    bot.infinity_polling(timeout=10, long_polling_timeout=5)