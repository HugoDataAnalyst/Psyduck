## Psyduck

Welcome to **PsyyyPsyPsyyyyyduckk** - a multifaceted project designed to streamline data handling and analysis.

This 3-in-1 solution encompasses:
- A **WebhookReceiver** for efficient data collection with advanced filtering capabilities.
- Robust **DataCube Database** storage, ensuring secure and scalable data management.
- A high-speed **API**, facilitating quick and easy access to detailed statistics.

**PsyyyPsyPsyyyyyduckk** is built for performance and reliability, mirroring the unexpected power of Psyduck from the Pokémon universe. Just as Psyduck releases formidable psychic powers when its headaches peak, this project tackles the "headaches" of data analysis by efficiently processing and interpreting large volumes of information. 

It's the perfect tool for anyone looking to burst through data-heavy challenges with speed and precision. Dive in and experience a new wave of data management and analysis!

![Psyduck Flex](Image/psyduck-flex.gif)

## Special Thanks:
- [Fabio](https://github.com/Fabio1988)

	Gave the guidance to use webhooks to process the raw data.

- [Root@tchavei](https://github.com/tchavei)

	Developed all the Dockerfiles.

	Helped a lot with testing and gave suggestions for security measures and new features such as VictoriaMetrics.

## API paths:

### Pokemon:

- /api/daily-area-pokemon-stats
- /api/weekly-area-pokemon-stats
- /api/monthly-area-pokemon-stats
- /api/hourly-total-pokemon-stats
- /api/daily-total-pokemon-stats
- /api/total-pokemon-stats
- /api/surge-daily-stats
- /api/surge-weekly-stats
- /api/surge-monthly-stats

### Quests:

- /api/daily-quest-grouped-stats
- /api/weekly-quest-grouped-stats
- /api/monthly-quest-grouped-stats
- /api/daily-quest-total-stats
- /api/total-quest-total-stats

### Raids:

- /api/daily-raid-grouped-stats
- /api/weekly-raid-grouped-stats
- /api/monthly-raid-grouped-stats
- /api/hourly-raid-total-stats
- /api/daily-raid-total-stats
- /api/total-raid-total-stats

### Invasions:

- /api/daily-invasion-grouped-stats
- /api/weekly-invasion-grouped-stats
- /api/monthly-invasion-grouped-stats
- /api/hourly-invasion-total-stats
- /api/daily-invasion-total-stats
- /api/total-invasion-total-stats

### Pokemon TTH:

- /api/hourly-pokemon-tth-stats
- /api/daily-pokemon-tth-stats

### Metrics Pokemon:

- /metrics/daily-area-pokemon
- /metrics/weekly-area-pokemon
- /metrics/monthly-area-pokemon
- /metrics/total-hourly-pokemon
- /metrics/total-daily-pokemon
- /metrics/total-pokemon
- /metrics/surge-daily-stats
- /metrics/surge-weekly-stats
- /metrics/surge-monthly-stats

### Metrics Quests:

- /metrics/daily-quest-grouped-stats
- /metrics/weekly-quest-grouped-stats
- /metrics/monthly-quest-grouped-stats
- /metrics/daily-quest-total-stats
- /metrics/total-quest-total-stats

### Metrics Raids:

- /metrics/daily-raid-grouped-stats
- /metrics/weekly-raid-grouped-stats
- /metrics/monthly-raid-grouped-stats
- /metrics/hourly-raid-total-stats
- /metrics/daily-raid-total-stats
- /metrics/total-raid-total-stats

### Metrics Invasions:

- /metrics/daily-invasion-grouped-stats
- /metrics/weekly-invasion-grouped-stats
- /metrics/monthly-invasion-grouped-stats
- /metrics/hourly-invasion-total-stats
- /metrics/daily-invasion-total-stats
- /metrics/total-invasion-total-stats

### Metrics Pokemon TTH:

- /metrics/hourly-pokemon-tth-stats
- /metrics/daily-pokemon-tth-stats

Metrics/ is only for Prometheus/VictoriaMetrics types (plaintext).

## Usage of APIs:

Currently I've setup dynamic caching TTL which means you don't need to ask for the API data more then the setup cache timers per hour/day/weekly/month.

### **Timers for each type:**

- Hourly: After minute 1 of each hour;
- Daily: After 02:00:00 of each day;
- Weekly: After 02:00:00 of each Monday;
- Monthly: After 03:00:00 of the first day of each Month;
- Total: uses Daily timers.

### **Quest specific:**

- Daily: After 16:00:00 of each day;
- Weekly: After 16:00:00 of each Monday;
- Monthly: After 16:00:00 of the first day of each Month;
- Total: users Daily Timers.

**Quest also has a parameter called Scanned.**

It's current logic works for scans based on normal quest timer (00-01 and 10-11):

- Quests obtained from 21:00:00 to 5:00:00 = Scanned 1
- Quests obtained from 07:00:00 to 14:00:00 = Scanned 2

You can change the procedure **store_quest_total_stats** logic to add more or tighten its timers.

## Requirements:

- MySQL Database 8.0.0+;
- MariaDB 11.2.2;
- Koji;
- Golbat;
- Python 3.10.13, untested on previous/higher versions;
- Redis Server 5.0.7.

Python3 Libraries:

- fastapi;
- uvicorn;
- fastapi-cache2;
- redis;
- aioredis;
- celery;
- pymysql;
- shapely;
- requests;
- cachetools;
- httpx;
- starlette.

## Installation:

```python3.10 -m pip install -r requirements.txt```

```sudo apt install redis-server```

## Configuration:

### Redis:

#### You can ignore these steps if you intend to use redis on the same server since the installation by default is protected.

Use bind to allow only designated ips example:

- bind 127.0.0.1 192.168.1.100 192.168.1.101

Disable protected-mode:

- protected-mode no

Properly set a very strong password for redis, because bruteforce can try 150k passwords per second:

- requirepass your_very_strong_password_here

Change the port if you want, default is 6379:

- port 6379

### Config:

```cd config/ && cp config.example.json config.json```

#### Key considerations:

#### **"redis"**:

- "CLEAN" Set to true in order to clean the redis db cache on API start up.

#### **"koji"**:

- "MAX_SIZE_GEOFENCE" If you have say 10 geofences, set to 15. Ensuring you have room for more without having to tweak this value often.

- "CACHE_GEOFENCES" Time in seconds that it will cache the Geofences.

- "REFRESH_GEOFENCES" Time in seconds that it will refresh the cache for the Geofences, should be the same as the above.

- "MAX_TRIES_GEOFENCES" Number of attempts on failure.

- "RETRY_DELAY_MULT_GEOFENCES" Number of retries multipled on failure. Example with 5 Max Tries and 2 Retry Delay: 1 sec * 2, 2 * 2, so on.

#### **"logs for any section"**:

- "LOG_LEVEL" INFO/DEBUG/WARNING/ERROR/CRITICAL/OFF choose one.

- "CONSOLE_LOG_LEVEL" INFO/DEBUG/WARNING/ERROR/CRITICAL/OFF choose one.

- "LOG_FILE" the path inside the repository you want your logs and their names.

- "LOG_MAX_BYTES" the max size, currently set to 1 MegaByte by default.

- "MAX_LOG_FILES" how many log files it should create (rotating).

#### **"receiver" Section:**

- "ALLOW_WEBHOOK_HOST" your golbat IP.

- "MAX_QUEUE_SIZE" when to flush the Pokémon data to the database, you could edit this value to be higher to create less stress in the database. Example: 2000.

- "MAX_QUEST_QUEUE_SIZE" when to flush the Quest data to the database, you could edit this value to be higher to create less stress in the database. Example: 500.

- "MAX_RAID_QUEUE_SIZE" when to flush the Raid data to the database, you could edit this value to be higher to create less stress in the database. Example: 500.

- "MAX_INVASION_QUEUE_SIZE" when to flush the Invasion data to the database, you could edit this value to be higher to create less stress in the database. Example: 500.

- "WORKERS" is set to 1, which by nature is more then enough to process up to 10 Million Raw Data a day.

- "MAX_RETRIES" number of attempts on failure.

- "RETRY_DELAY" time between each attempt on failure.

#### **"database" Section:**

- "CLEAN" is set to true, only set to false if you know what you're doing. Will only run once to create it.

#### **"celery" Section:**

- "WORKERS" is set to 1, only ever touch this if you know what you're doing otherwise it might lead to unexpected consumption of resources.

#### **"api" Section:**

- "HEADER_NAME" It's up to you if you want to change the Header Name, but I recommend you do.

- "SECRET_KEY" Make sure to set your own proper secret key.

- "SECRET_HEADER_KEY" Make sure to set your own proper secret header key.

- "WORKERS" is set to 1, only ever touch this if you know what you're doing otherwise it might lead to unexpected consumption of resources.

- "IP_RESTRICTION" Highly recommended if you don't know what your doing. Make sure to fill in "ALLOWED_IPS" "ip1, ip2, ip3" as in the example.

- "ALLOWED_IPS" Requires IP_RESTRICTION = true, set the IPs where you intend to request the API.

- "PATH_RESTRICTION" Restricts access to only the API available paths, fully blocking any other attempts.


### Please fill in **carefully** your config.json

### Database:

Take in mind you should allow for the creation of events/procedures in the Database.

```cd SQL/ && python3.10 create_database_schema.py```

### Celery:

Run Celery the task executioner:

```python3.10 celery_worker.py```

**Always run celery first to check for Database Migrations.**

After applying them, you can now populate your **total_pokestops** table, which is used for stats in the quests, by doing the following:

**Important Note: This is a standalone script that should be run once or whenever you want to update your total PokéStops numbers per area.**

```cd SQL && cp example_obtain_total_stops.py obtain_total_stops.py```

Fill in the details for the databases access.

```python3.10 obtain_total_stops.py```

### Webhook Receiver:

Run the start_webhookparser.py:

```python3.10 start_webhookparser.py```


### API:

Run the API:

```python3.10 start_api.py```


### Using PM2: -- Not working

```npm install pm2@latest -g```

Edit ecosystem.config.js as you prefer and run:

```pm2 start ecosystem.config.js```

## Docker:
```cd Psyduck ```

```sudo cp example.database.Dockerfile database.Dockerfile```

```sudo cp example.celery.Dockerfile celery.Dockerfile```

```sudo cp  example.webhook.Dockerfile webhook.Dockerfile```

```sudo cp example.api.Dockerfile api.Dockerfile```

```sudo cp example.docker-compose.yml docker-compose.yml``` 

```sudo docker-compose up```

```
#Psyduck stack
services:
  psyduck-schema-setup:
    build:
      context: .
      dockerfile: database.Dockerfile
    depends_on:
      - psyduck-db
```
Remove this section from the docker-compose.yml after running it the first time.

```sudo docker-compose up -d```