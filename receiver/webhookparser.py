import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse
import json
from shapely.geometry import Point, Polygon
import requests
import os
from datetime import datetime, timedelta
import asyncio
from processor.celery_app import celery
from config.app_config import app_config
from processor.tasks import insert_data_task, generate_unique_id, insert_quest_data_task, insert_raid_data_task, insert_invasion_data_task
from threading import Lock, Thread
import time
from cachetools import TTLCache
import httpx
import backoff

# Setup the FastAPI app
webhook_processor = FastAPI()

# Caching geofences
geofence_cache = TTLCache(maxsize=app_config.max_size_geofence, ttl=app_config.cache_geofences)

# Data processing queue Pokémon
is_processing_queue = False
data_queue = []
data_queue_lock = asyncio.Lock()

# Quests processing queue
is_quests_processing_queue = False
quests_data_queue = []
quests_data_queue_lock = asyncio.Lock()

# Raids processing queue
is_raids_processing_queue = False
raids_data_queue = []
raids_data_queue_lock = asyncio.Lock()

# Invasions processing queue
is_invasions_processing_queue = False
invasions_data_queue = []
invasions_data_queue_lock = asyncio.Lock()

# Time processors
last_quests_processing_time = datetime.now()
last_raids_processing_time = datetime.now()
last_invasions_processing_time = datetime.now()

# Configuration values
console_log_level_str = app_config.webhook_console_log_level.upper()
log_level_str = app_config.webhook_log_level.upper()
log_file = app_config.webhook_log_file
max_bytes = app_config.webhook_log_max_bytes
backup_count = app_config.webhook_max_log_files

# Console Logger
console_logger = logging.getLogger("webhook_console_logger")
if console_log_level_str == "OFF":
    console_logger.disabled = True
else:
    console_log_level = getattr(logging, console_log_level_str, logging.INFO)
    console_logger.setLevel(console_log_level)
    # Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)
    # Formatter
    console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    console_logger.addHandler(console_handler)

# File logger
file_logger = logging.getLogger("webhook_file_logger")
if log_level_str == "OFF":
    log_level = logging.NOTSET
else:
    log_level = getattr(logging, log_level_str, logging.INFO)

file_logger.setLevel(log_level)
if not os.path.exists(os.path.dirname(log_file)):
    os.makedirs(os.path.dirname(log_file))

#handler
file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
file_handler.setLevel(log_level)
# Formatter
file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
file_handler.setFormatter(file_formatter)
file_logger.addHandler(file_handler)

refresh_task = None

@webhook_processor.on_event("startup")
async def startup_event():
    global geofence_cache, refresh_task
    try:
        geofences = await fetch_geofences()
        geofence_cache['geofences'] = geofences
        console_logger.info(f"Sucessfully obtained {len(geofences)} geofences.")
        file_logger.info(f"Sucessfully obtained {len(geofences)} geofences.")
        # Cancel existing refresh tasks before creating a new one
        if refresh_task:
            refresh_task.cancel()
        refresh_task = asyncio.create_task(refresh_geofences())
    except httpx.HTTPError as e:
        console_logger.error(f"Failed to fetch geofences: {e}")
        file_Logger.error(f"Failed to fetch geofences: {e}")


@backoff.on_exception(backoff.expo, httpx.HTTPError, max_tries=app_config.max_tries_geofences, jitter=None, factor=app_config.retry_delay_mult_geofences)
async def fetch_geofences():
    async with httpx.AsyncClient() as client:
        headers = {"Authorization": f"Bearer {app_config.bearer_token}"}
        response = await client.get(app_config.geofence_api_url, headers=headers)
        if response.status_code == 200:
            return response.json().get("data", {}).get("features", [])
        else:
            console_logger.error(f"Failed to fetch geofences. Status Code: {response.status_code}")
            file_logger.error(f"Failed to fetch geofences. Status Code: {response.status_code}")
            raise httpx.HTTPError(f"Failed to fetch geofences. Status Code: {response.status_code}")

async def refresh_geofences():
    while True:
        try:
            geofences = await fetch_geofences()
            geofence_cache['geofences'] = geofences
            console_logger.info(f"Successfully refreshed {len(geofences)} geofences.")
            file_logger.info(f"Successfully refreshed {len(geofences)} geofences.")
        except httpx.HTTPError as e:
            console_logger.error(f"Failed to refresh geofences: {e}")
            file_logger.error(f"Failed to refresh geofences: {e}")
        await asyncio.sleep(app_config.refresh_geofences)


def is_inside_geofence(lat, lon, geofences):
    point = Point(lon, lat)
    for geofence in geofences:
        polygon = geofence["geometry"]["coordinates"][0]
        if point.within(Polygon(polygon)):
            return True, geofence.get("properties", {}).get("name", "Unknown")
    return False, None

def calculate_despawn_time(disappear_time, first_seen):
    if disappear_time is None or first_seen is None:
        return None
    time_diff = disappear_time - first_seen
    total_seconds = time_diff // 1
    return total_seconds

async def validate_remote_addr(request: Request):
    if not app_config.allow_webhook_host:
        return
    if request.client.host != app_config.allow_webhook_host:
        raise HTTPException(status_code=403, detail="Access denied")

@webhook_processor.post("/")
def root_post_redirect():
    return RedirectResponse(url="/webhook", status_code=307)

@webhook_processor.post("/webhook")
async def receive_data(request: Request):
    console_logger.debug(f"Received request on path: {request.url.path}")
    file_logger.debug(f"Received request on path: {request.url.path}")
    global data_queue, is_processing_queue, geofence_cache, quests_data_queue, is_quests_processing_queue, last_quests_processing_time, raids_data_queue, is_raids_processing_queue, last_raids_processing_time, invasions_data_queue, is_invasions_processing_queue, last_invasions_processing_time
    await validate_remote_addr(request)
    console_logger.debug(f"Queue size before processing: {len(data_queue)}")
    file_logger.debug(f"Queue size before processing: {len(data_queue)}")
    async with data_queue_lock: 
        data = await request.json()
 
    geofences = geofence_cache.get('geofences', [])
    if not geofences:
        console_logger.info("No geofences matched.")
        file_logger.info("No geofences matched.")
        return {"status": "info", "message": "No geofences available"}

    # Raid filters
    def raid_filter_criteria(message):
        raid_required_fields = [
            'gym_id', 'ex_raid_eligible', 'is_exclusive', 'level', 'pokemon_id', 'form', 'costume', 'latitude', 'longitude'
        ]
        return all(message.get(raid_field) is not None for raid_field in raid_required_fields)

    # Invasion filters
    def invasion_filter_criteria(message):
        invasion_required_fields = [
            'display_type', 'character', 'confirmed', 'pokestop_id', 'latitude', 'longitude'
        ]
        return all(message.get(invasion_field) is not None for invasion_field in invasion_required_fields)

    # Quest filters
    def quest_filter_criteria(message):
        # Check for mandatory fields: type, with_ar, latitude, and longitude
        basic_checks = all(key in message for key in ['type', 'with_ar', 'latitude', 'longitude'])
    
        # Initialize rewards_check to False to ensure it must pass checks to turn True
        rewards_check = False
        if 'rewards' in message and isinstance(message['rewards'], list) and len(message['rewards']) > 0:
            for reward in message['rewards']:
                # Each reward must have a type and an info dictionary
                if 'type' in reward and 'info' in reward:
                    info = reward['info']
                    # Each info must have either (pokemon_id and form_id) or (item_id and amount)
                    if ('pokemon_id' in info and 'form_id' in info) or ('item_id' in info and 'amount' in info):
                        rewards_check = True
                    else:
                        # If any reward does not meet the criteria, fail the check and stop looping
                        rewards_check = False
                        break
                else:
                    # If any reward does not have the correct structure, fail the check and stop looping
                    rewards_check = False
                    break

        return basic_checks and rewards_check

    # Extract quest rewards information
    def extract_quest_rewards(quest_rewards):
        extracted_rewards = []  # Use a list to store information about each reward

        for reward in quest_rewards:
            reward_info = reward.get('info', {})
            reward_data = {
                'reward_type': reward.get('type'),  # Always extract the reward type
                'pokemon_id': reward_info.get('pokemon_id'),
                'form_id': reward_info.get('form_id'),
                'item_id': reward_info.get('item_id'),
                'amount': reward_info.get('amount')
            }

            # Keep None values, as they will be stored as NULL in the database
            extracted_rewards.append(reward_data)

        return extracted_rewards

    # Pokémon filters
    def filter_criteria(message):
        required_fields = [
            'pokemon_id', 'form', 'latitude', 'longitude',
            'individual_attack', 'individual_defense', 'individual_stamina'
        ]
        return all(message.get(field) is not None for field in required_fields)

    def extract_pvp_ranks(pvp_data):
        ranks = {f'pvp_{category}_rank': None for category in ['great', 'little', 'ultra']}
        if pvp_data:
            for category in ['great', 'little', 'ultra']:
                category_data = pvp_data.get(category, [])
                ranks[f'pvp_{category}_rank'] = 1 if any(entry.get('rank') == 1 for entry in category_data) else None
        return ranks

    # Webhook logic
    if isinstance(data, list):
        for item in data:
            # Pokémon Logic
            if item.get('type') == 'pokemon':
                message = item.get('message', {})
                if filter_criteria(message):
                    lat, lon = message.get('latitude'), message.get('longitude')
                    inside, geofence_name = is_inside_geofence(lat, lon, geofences)
                    if inside:
                        ind_attack = message['individual_attack']
                        ind_defense = message['individual_defense']
                        ind_stamina = message['individual_stamina']

                        if ind_attack == ind_defense == ind_stamina == 15:
                            iv_value = 100
                        elif ind_attack == ind_defense == ind_stamina == 0:
                            iv_value = 0
                        else:
                            iv_value = None

                        despawn_time = calculate_despawn_time(
                            message.get('disappear_time'),
                            message.get('first_seen')
                        )
                        filtered_data = {
                            'pokemon_id': message['pokemon_id'],
                            'form': message['form'],
                            'latitude': lat,
                            'longitude': lon,
                            'iv': iv_value,
                            **extract_pvp_ranks(message.get('pvp', {})),
                            'shiny':message['shiny'],
                            'area_name': geofence_name,
                            'despawn_time': despawn_time
                        }

                        item_unique_id = generate_unique_id(filtered_data)
                        data_queue.append((filtered_data, item_unique_id))

                        if len(data_queue) >= app_config.max_queue_size and not is_processing_queue:
                            is_processing_queue = True
                            process_full_queue()
                else:
                    console_logger.debug("Pokemon Data did not meet filter criteria")
                    file_logger.debug("Pokemon Data did not meet filter criteria")

            # Quest Logic
            elif item.get('type') == 'quest':
                message = item.get('message', {})
                if quest_filter_criteria(message):
                    lat, lon = message.get('latitude'), message.get('longitude')
                    inside, geofence_name = is_inside_geofence(lat, lon, geofences)
                    if inside:
                        rewards_extracted = extract_quest_rewards(message.get('rewards, []'))
                        quest_data_to_store = {
                            'pokestop_id': message.get('pokestop_id'),
                            'area_name': geofence_name,
                            # Initialize as None by default
                            'ar_type': None,
                            'normal_type': None,
                            'reward_ar_type': None,
                            'reward_normal_type': None,
                            'reward_ar_item_id': None,
                            'reward_ar_item_amount': None,
                            'reward_normal_item_id': None,
                            'reward_normal_item_amount': None,
                            'reward_ar_poke_id': None,
                            'reward_ar_poke_form': None,
                            'reward_normal_poke_id': None,
                            'reward_normal_poke_form': None,
                        }
                        # Determine AR or Normal
                        quest_type_field = 'ar_type' if message.get('with_ar') else 'normal_type'
                        quest_data_to_store[quest_type_field] = message.get('type')

                        # Process rewards
                        for reward in rewards_extracted:
                            reward_prefix = 'reward_ar_' if message.get('with_ar') else 'reward_normal_'
                            if 'pokemon_id' in reward:
                                quest_data_to_store[f'{reward_prefix}poke_id'] = reward.get('pokemon_id')
                                quest_data_to_store[f'{reward_prefix}poke_form'] = reward.get('form_id')
                            elif 'item_id' in reward:
                                quest_data_to_store[f'{reward_prefix}item_id'] = reward.get('item_id')
                                quest_data_to_store[f'{reward_prefix}item_amount'] = reward.get('amount')
                            # Break if theres more then one reward per quest
                            break

                        # Generate unique ID for quests
                        quest_unique_id = generate_unique_id(quest_data_to_store)
                        quests_data_queue.append((quest_data_to_store, quest_unique_id))

                        # Calculate time checker for time based task
                        now = datetime.now()
                        time_since_last_process = (now - last_quests_processing_time).total_seconds()

                        # Logic for Queue or Time Based Queue
                        if len(quests_data_queue) >= app_config.max_quest_queue_size and not is_quests_processing_queue:
                            is_quests_processing_queue = True
                            quest_process_full_queue()
                            last_quests_processing_time = now
                            console_logger.info(f"Processing Quest queue because it reached the maximum size of {app_config.max_quest_queue_size}.")
                            file_logger.info(f"Processing Quest queue because it reached the maximum size of {app_config.max_quest_queue_size}.")
                        elif time_since_last_process >= 1800 and len(quests_data_queue) > 0 and not is_quests_processing_queue:
                            is_quests_processing_queue = True
                            quest_process_full_queue()
                            last_quests_processing_time = now
                            console_logger.info(f"Processing quest queue due to time condition met. Queue size: {len(quests_data_queue)}. Time since last process: {time_since_last_process} seconds.")
                            file_logger.info(f"Processing quest queue due to time condition met. Queue size: {len(quests_data_queue)}. Time since last process: {time_since_last_process} seconds.")
                        elif time_since_last_process >= 1800 and len(quests_data_queue) == 0:
                            console_logger.info(f"Time condition for processing quest queue was met, but the queue is empty. No action taken.")
                            file_logger.info(f"Time condition for processing quest queue was met, but the queue is empty. No action taken.")                     

                else:
                    console_logger.debug("Quest Data did not meet filter criteria")
                    file_logger.debug("Quest Data did not meet filter criteira")

            # Raid logic
            elif item.get('type') == 'raid':
                message = item.get('message', {})
                if raid_filter_criteria(message):
                    lat, lon = message.get('latitude'), message.get('longitude')
                    inside, geofence_name = is_inside_geofence(lat, lon, geofences)
                    if inside:
                        raid_data_to_store = {
                            'gym_id': message['gym_id'],
                            'ex_raid_eligible': message['ex_raid_eligible'],
                            'is_exclusive': message['is_exclusive'],
                            'level': message['level'],
                            'pokemon_id': message['pokemon_id'],
                            'form': message['form'],
                            'costume': message['costume'],
                            'area_name': geofence_name,
                        }

                        # Generate unique ID for Raids
                        raid_unique_id = generate_unique_id(raid_data_to_store)
                        raids_data_queue.append((raid_data_to_store, raid_unique_id))

                        # Calculate time checker for time based task
                        now = datetime.now()
                        time_since_last_process = (now - last_raids_processing_time).total_seconds()

                        # Logic for Queue or Time Based Queue
                        if len(raids_data_queue) >= app_config.max_raid_queue_size and not is_raids_processing_queue:
                            is_raids_processing_queue = True
                            raid_process_full_queue()
                            last_raids_processing_time = now
                            console_logger.info(f"Processing Raid queue because it reached the maximum size of {app_config.max_raid_queue_size}.")
                            file_logger.info(f"Processing Raid queue because it reached the maximum size of {app_config.max_raid_queue_size}.")
                        elif time_since_last_process >= 1800 and len(raids_data_queue) > 0 and not is_raids_processing_queue:
                            is_raids_processing_queue = True
                            raid_process_full_queue()
                            last_raids_processing_time = now
                            console_logger.info(f"Processing Raid queue due to time condition met. Queue size: {len(raids_data_queue)}. Time since last process: {time_since_last_process} seconds.")
                            file_logger.info(f"Processing Raid queue due to time condition met. Queue size: {len(raids_data_queue)}. Time since last process: {time_since_last_process} seconds.")
                        elif time_since_last_process >= 1800 and len(raids_data_queue) == 0:
                            console_logger.info(f"Time condition for processing Raid queue was met, but the queue is empty. No action taken.")
                            file_logger.info(f"Time condition for processing Raid queue was met, but the queue is empty. No action taken.")   
                else:
                    console_logger.debug("Raid Data did not meet filter criteria")
                    file_logger.debug("Raid Data did not meet filter criteira")

            # Invasion logic
            elif item.get('type') == 'invasion':
                message = item.get('message', {})
                if invasion_filter_criteria(message):
                    lat, lon = message.get('latitude'), message.get('longitude')
                    inside, geofence_name = is_inside_geofence(lat, lon, geofences)
                    if inside:
                        invasion_data_to_store = {
                            'pokestop_id': message['pokestop_id'],
                            'display_type': message['display_type'],
                            'character': message['character'],
                            'confirmed': message['confirmed'],
                            'area_name': geofence_name,
                        }

                        # Generate unique ID for Invasions
                        invasion_unique_id = generate_unique_id(invasion_data_to_store)
                        invasions_data_queue.append((invasion_data_to_store, invasion_unique_id))

                        # Calculate time checker for time based task
                        now = datetime.now()
                        time_since_last_process = (now - last_invasions_processing_time).total_seconds()

                        # Logic for Queue or Time Based Queue
                        if len(invasions_data_queue) >= app_config.max_invasion_queue_size and not is_invasions_processing_queue:
                            is_invasions_processing_queue = True
                            invasion_process_full_queue()
                            last_invasions_processing_time = now
                            console_logger.info(f"Processing Invasion queue because it reached the maximum size of {app_config.max_invasion_queue_size}.")
                            file_logger.info(f"Processing Invasion queue because it reached the maximum size of {app_config.max_invasion_queue_size}.")
                        elif time_since_last_process >= 1800 and len(invasions_data_queue) > 0 and not is_invasions_processing_queue:
                            is_invasions_processing_queue = True
                            invasion_process_full_queue()
                            last_invasions_processing_time = now
                            console_logger.info(f"Processing Invasion queue due to time condition met. Queue size: {len(invasions_data_queue)}. Time since last process: {time_since_last_process} seconds.")
                            file_logger.info(f"Processing Invasion queue due to time condition met. Queue size: {len(invasions_data_queue)}. Time since last process: {time_since_last_process} seconds.")
                        elif time_since_last_process >= 1800 and len(invasions_data_queue) == 0:
                            console_logger.info(f"Time condition for processing Invasion queue was met, but the queue is empty. No action taken.")
                            file_logger.info(f"Time condition for processing Invasion queue was met, but the queue is empty. No action taken.")   

                    else:
                        console_logger.debug("Invasion Data did not meet filter criteria")
                        file_logger.debug("Invasion Data did not meet filter criteira")

            else:
                console_logger.debug(f"Unsupported data type found in payload: {item.get('type')}")
                file_logger.debug(f"Unsupported data type found in payload: {item.get('type')}")
    else:
        console_logger.error("Received data is not in list format")
        file_logger.error("Received data is not in list format")

    # Pokemon Queue Info
    console_logger.info(f"Pokemon Queue size AFTER processing: {len(data_queue)}")
    file_logger.info(f"Pokemon Queue size AFTER processing: {len(data_queue)}")

    # Quest Queue Info
    console_logger.info(f"Quest Queue size AFTER processing: {len(quests_data_queue)}")
    file_logger.info(f"Quest Queue size AFTER processing: {len(quests_data_queue)}")

    # Raid Queue Info
    console_logger.info(f"Raids Queue size AFTER processing: {len(raids_data_queue)}")
    file_logger.info(f"Raids Queue size AFTER processing: {len(raids_data_queue)}")

    # Invasion Queue Info
    console_logger.info(f"Invasions Queue size AFTER processing: {len(invasions_data_queue)}")
    file_logger.info(f"Invasions Queue size AFTER processing: {len(invasions_data_queue)}")

    return {"status": "success"}

# Invasion processing queue
def invasion_process_full_queue():
    global invasions_data_queue, is_invasions_processing_queue
    console_logger.info(f"Starting Invasion full queue processing. Current Invasion queue size: {len(invasions_data_queue)}")
    file_logger.info(f"Starting Invasion full queue processing. Current Invasion queue size: {len(invasions_data_queue)}")

    retry_count = 0
    while retry_count <= app_config.max_retries:
        try:
            current_invasion_batch_data = [item[0] for item in invasions_data_queue[:app_config.max_invasion_queue_size]]
            current_invasion_batch_ids = [item[1] for item in invasions_data_queue[:app_config.max_invasion_queue_size]]
            invasion_batch_unique_id = generate_unique_id(current_invasion_batch_ids)

            insert_invasion_data_task.delay(current_invasion_batch_data, invasion_batch_unique_id)
            console_logger.debug(f"Processed Invasion full queue with unique_id: {invasion_batch_unique_id}")
            file_logger.debug(f"Processed Invasion full queue with unique_id: {invasion_batch_unique_id}")
            invasions_data_queue = invasions_data_queue[app_config.max_invasion_queue_size:]
            break
        except Exception as e:
            retry_count += 1
            console_logger.error(f"Error processing Invasion queue on attempt {retry_count}: {e}")
            file_logger.error(f"Error processing Invasion queue on attempt {retry_count}: {e}")
            time.sleep(app_confg.retry_delay)

    if retry_count > app_config.max_retries:
        console_logger.error("Maximum retry attempts reached. Unable to process Invasion queue")
        file_logger.error("Maximum retry attempts reached. Unable to process Invasion queue")
    is_invasions_processing_queue = False
    console_logger.info(f"Finished Invasion queue processing. Updated Invasion queue size: {len(invasions_data_queue)}")
    file_logger.info(f"Finished Invasion queue processing. Updated Invasion queue size: {len(invasions_data_queue)}")


# Raid processing queue
def raid_process_full_queue():
    global raids_data_queue, is_raids_processing_queue
    console_logger.info(f"Starting Raid full queue processing. Current Raid queue size: {len(raids_data_queue)}")
    file_logger.info(f"Starting Raid full queue processing. Current Raid queue size: {len(raids_data_queue)}")

    retry_count = 0
    while retry_count <= app_config.max_retries:
        try:
            current_raid_batch_data = [item[0] for item in raids_data_queue[:app_config.max_raid_queue_size]]
            current_raid_batch_ids = [item[1] for item in raids_data_queue[:app_config.max_raid_queue_size]]
            raid_batch_unique_id = generate_unique_id(current_raid_batch_ids)

            insert_raid_data_task.delay(current_raid_batch_data, raid_batch_unique_id)
            console_logger.debug(f"Processed Raid full queue with unique_id: {raid_batch_unique_id}")
            file_logger.debug(f"Processed Raid full queue with unique_id: {raid_batch_unique_id}")
            raids_data_queue = raids_data_queue[app_config.max_raid_queue_size:]
            break
        except Exception as e:
            retry_count += 1
            console_logger.error(f"Error processing Raid queue on attempt {retry_count}: {e}")
            file_logger.error(f"Error processing Raid queue on attempt {retry_count}: {e}")
            time.sleep(app_confg.retry_delay)

    if retry_count > app_config.max_retries:
        console_logger.error("Maximum retry attempts reached. Unable to process Raid queue")
        file_logger.error("Maximum retry attempts reached. Unable to process Raid queue")
    is_raids_processing_queue = False
    console_logger.info(f"Finished Raid queue processing. Updated Raid queue size: {len(raids_data_queue)}")
    file_logger.info(f"Finished Raid queue processing. Updated Raid queue size: {len(raids_data_queue)}")

# Quest processing queue
def quest_process_full_queue():
    global quests_data_queue, is_quests_processing_queue
    console_logger.info(f"Starting Quest full queue processing. Current Quest queue size: {len(quests_data_queue)}")
    file_logger.info(f"Starting Quest full queue processing. Current Quest queue size: {len(quests_data_queue)}")

    retry_count = 0
    while retry_count <= app_config.max_retries:
        try:
            current_quest_batch_data = [item[0] for item in quests_data_queue[:app_config.max_quest_queue_size]]
            current_quest_batch_ids = [item[1] for item in quests_data_queue[:app_config.max_quest_queue_size]]
            quest_batch_unique_id = generate_unique_id(current_quest_batch_ids)

            insert_quest_data_task.delay(current_quest_batch_data, quest_batch_unique_id)
            console_logger.debug(f"Processed Quests full queue with unique_id: {quest_batch_unique_id}")
            file_logger.debug(f"Processed Quests full queue with unique_id: {quest_batch_unique_id}")
            quests_data_queue = quests_data_queue[app_config.max_quest_queue_size:]
            break
        except Exception as e:
            retry_count += 1
            console_logger.error(f"Error processing Quests queue on attempt {retry_count}: {e}")
            file_logger.error(f"Error processing Quests queue on attempt {retry_count}: {e}")
            time.sleep(app_confg.retry_delay)

    if retry_count > app_config.max_retries:
        console_logger.error("Maximum retry attempts reached. Unable to process Quests queue")
        file_logger.error("Maximum retry attempts reached. Unable to process Quests queue")
    is_quests_processing_queue = False
    console_logger.info(f"Finished Quests queue processing. Updated Quests queue size: {len(quests_data_queue)}")
    file_logger.info(f"Finished Quests queue processing. Updated Quests queue size: {len(quests_data_queue)}")

# Pokemon processing queue
def process_full_queue():
    global data_queue, is_processing_queue
    console_logger.info(f"Starting Pokemon full queue processing. Current queue size: {len(data_queue)}")
    file_logger.info(f"Starting Pokemon full queue processing. Current queue size: {len(data_queue)}")

    retry_count = 0
    while retry_count <= app_config.max_retries:
        try:
            current_batch_data = [item[0] for item in data_queue[:app_config.max_queue_size]]
            current_batch_ids = [item[1] for item in data_queue[:app_config.max_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_data_task.delay(current_batch_data, batch_unique_id)
            console_logger.debug(f"Processed Pokemon full queue with unique_id: {batch_unique_id}")
            file_logger.debug(f"Processed Pokemon full queue with unique_id: {batch_unique_id}")
            data_queue = data_queue[app_config.max_queue_size:]
            break
        except Exception as e:
            retry_count += 1
            console_logger.error(f"Error processing Pokemon queue on attempt {retry_count}: {e}")
            file_logger.error(f"Error processing Pokemon queue on attempt {retry_count}: {e}")
            time.sleep(app_config.retry_delay)
    
    if retry_count > app_config.max_retries:
        console_logger.error("Maximum retry attempts reached. Unable to process Pokemon queue")
        file_logger.error("Maximum retry attempts reached. Unable to process Pokemon queue")
    is_processing_queue = False
    console_logger.info(f"Finished Pokemon queue processing. Updated Pokemon queue size: {len(data_queue)}")
    file_logger.info(f"Finished Pokemon queue processing. Updated Pokemon queue size: {len(data_queue)}")

# Pokemon processing queue on shutdown
async def process_remaining_queue_on_shutdown():
    global data_queue
    console_logger.debug("Processing remaining items in Pokemon queue before shutdown.")
    file_logger.debug("Processing remaining items in Pokemon queue before shutdown.")
    while data_queue:
        try:
            current_batch_data = [item[0] for item in data_queue[:app_config.max_queue_size]]
            current_batch_ids = [item[1] for item in data_queue[:app_config.max_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_data_task.delay(current_batch_data, batch_unique_id)
            console_logger.debug(f"Processed Pokemon batch with unique_id: {batch_unique_id}")
            file_logger.debug(f"Processed Pokemon batch with unique_id: {batch_unique_id}")

            # Remove processed items from the queue
            data_queue = data_queue[app_config.max_queue_size:]
            console_logger.info(f"Processed and cleaned Pokemon queue -- Goodbye")
            file_logger.info(f"Processed and cleaned Pokemon queue -- Goodbye")

        except Exception as e:
            console_logger.error(f"Error processing Pokemon batch during shutdown: {e}")
            file_logger.error(f"Error processing Pokemon batch during shutdown: {e}")

# Quest processing queue on shutdown
async def process_remaining_quest_queue_on_shutdown():
    global quests_data_queue
    console_logger.debug("Processing remaining items in Quest queue before shutdown.")
    file_logger.debug("Processing remaining items in Quest queue before shutdown.")
    while quests_data_queue:
        try:
            current_batch_data = [item[0] for item in quests_data_queue[:app_config.max_quest_queue_size]]
            current_batch_ids = [item[1] for item in quests_data_queue[:app_config.max_quest_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_quest_data_task.delay(current_batch_data, batch_unique_id)
            console_logger.debug(f"Processed Quest batch with unique_id: {batch_unique_id}")
            file_logger.debug(f"Processed Quest batch with unique_id: {batch_unique_id}")

            # Remove processed items from the queue
            quests_data_queue = quests_data_queue[app_config.max_quest_queue_size:]
            console_logger.info(f"Processed and cleaned Quest queue -- Goodbye")
            file_logger.info(f"Processed and cleaned Quest queue -- Goodbye")

        except Exception as e:
            console_logger.error(f"Error processing Quest batch during shutdown: {e}")
            file_logger.error(f"Error processing Quest batch during shutdown: {e}")

# Raid processing queue on shutdown
async def process_remaining_raid_queue_on_shutdown():
    global raids_data_queue
    console_logger.debug("Processing remaining items in Raid queue before shutdown.")
    file_logger.debug("Processing remaining items in Raid queue before shutdown.")
    while raids_data_queue:
        try:
            current_batch_data = [item[0] for item in raids_data_queue[:app_config.max_raid_queue_size]]
            current_batch_ids = [item[1] for item in raids_data_queue[:app_config.max_raid_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_raid_data_task.delay(current_batch_data, batch_unique_id)
            console_logger.debug(f"Processed Raid batch with unique_id: {batch_unique_id}")
            file_logger.debug(f"Processed Raid batch with unique_id: {batch_unique_id}")

            # Remove processed items from the queue
            raids_data_queue = raids_data_queue[app_config.max_raid_queue_size:]
            console_logger.info(f"Processed and cleaned Raid queue -- Goodbye")
            file_logger.info(f"Processed and cleaned Raid queue -- Goodbye")

        except Exception as e:
            console_logger.error(f"Error processing Raid batch during shutdown: {e}")
            file_logger.error(f"Error processing Raid batch during shutdown: {e}")

# Invasion processing queue on shutdown
async def process_remaining_invasion_queue_on_shutdown():
    global invasions_data_queue
    console_logger.debug("Processing remaining items in Invasion queue before shutdown.")
    file_logger.debug("Processing remaining items in Invasion queue before shutdown.")
    while invasions_data_queue:
        try:
            current_batch_data = [item[0] for item in invasions_data_queue[:app_config.max_invasion_queue_size]]
            current_batch_ids = [item[1] for item in invasions_data_queue[:app_config.max_invasion_queue_size]]
            batch_unique_id = generate_unique_id(current_batch_ids)

            insert_invasion_data_task.delay(current_batch_data, batch_unique_id)
            console_logger.debug(f"Processed Invasion batch with unique_id: {batch_unique_id}")
            file_logger.debug(f"Processed Invasion batch with unique_id: {batch_unique_id}")

            # Remove processed items from the queue
            invasions_data_queue = invasions_data_queue[app_config.max_invasion_queue_size:]
            console_logger.info(f"Processed and cleaned Invasion queue -- Goodbye")
            file_logger.info(f"Processed and cleaned Invasion queue -- Goodbye")

        except Exception as e:
            console_logger.error(f"Error processing Invasion batch during shutdown: {e}")
            file_logger.error(f"Error processing Invasion batch during shutdown: {e}")

@webhook_processor.on_event("shutdown")
async def shutdown_event():
    global refresh_task
    console_logger.info("Application shutdown initiated.")
    file_logger.info("Application shutdown initiated.")
    # Cancel refresh task during shutdown
    if refresh_task:
        refresh_task.cancel()
    # Ensure all remaining items in the queue are processed
    async with data_queue_lock:
        await process_remaining_queue_on_shutdown()
    async with quests_data_queue_lock:
        await process_remaining_quest_queue_on_shutdown()
    async with raids_data_queue_lock:
        await process_remaining_raid_queue_on_shutdown()
    async with invasions_data_queue_lock:
        await process_remaining_invasion_queue_on_shutdown()