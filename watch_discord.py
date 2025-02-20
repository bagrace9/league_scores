import discord
import configparser
import asyncio
from main import scrape_and_upload_league_scores, load_leagues

# read configuration
config = configparser.ConfigParser()
config.read('config.ini')
DISCORD_TOKEN = config['discord']['DISCORD_TOKEN']

# read league configurations from JSON file
leagues = load_leagues('leagues.json')

intents = discord.Intents.default()
intents.messages = True  # Ensure the bot can read messages
intents.message_content = True  # Ensure the bot can read the content of messages
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    pass

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('!scores'):
        for league in leagues:
            if league['discord_channel_id'] == str(message.channel.id):
                await message.channel.send("Grabbing scores meow...")
                await asyncio.to_thread(scrape_and_upload_league_scores, league['url'], league['file_name'], league['discord_channel_id'], league['handicap_enabled'])
                await message.channel.send("Done.")

if __name__ == "__main__":
    client.run(DISCORD_TOKEN)
