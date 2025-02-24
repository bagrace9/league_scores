import discord
import configparser
import asyncio
from league_scores import scrape_and_upload_league_scores, load_leagues, LeagueConfig

# Read configuration
config = configparser.ConfigParser()
config.read('config.ini')
DISCORD_TOKEN = config['discord']['DISCORD_TOKEN']
LEAGUES_FILE = config['files']['LEAGUES_FILE']
ENABLE_DISCORD_NOTIFICATIONS = config['discord'].getboolean('ENABLE_DISCORD_NOTIFICATIONS')

# Read league configurations from JSON file
leagues = load_leagues(LEAGUES_FILE)

# Set up Discord client with necessary intents
intents = discord.Intents.default()
intents.messages = True  # Ensure the bot can read messages
intents.message_content = True  # Ensure the bot can read the content of messages
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    pass

@client.event
async def on_message(message):
    # Event handler for when a message is received
    if message.author == client.user:
        return

    if message.content.startswith('!scores'):
        for league in leagues:
            if league['discord_channel_id'] == str(message.channel.id):
                await message.channel.send("Grabbing scores meow...")
                league_config = LeagueConfig(
                    url=league['url'],
                    file_name=league['file_name'],
                    channel_id=league['discord_channel_id'],
                    handicap_enabled=league['handicap_enabled'],
                    discord_token=DISCORD_TOKEN,
                    enable_discord_notifications=ENABLE_DISCORD_NOTIFICATIONS
                )
                await asyncio.to_thread(scrape_and_upload_league_scores, league_config)
                await message.channel.send("Done.")

if __name__ == "__main__":
    # Run the Discord bot
    client.run(DISCORD_TOKEN)
