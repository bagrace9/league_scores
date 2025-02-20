import discord
import configparser

# read configuration
config = configparser.ConfigParser()
config.read('config.ini')
DISCORD_TOKEN = config['discord']['DISCORD_TOKEN']

def upload_scores_to_discord(file_name, channel_id):
    TOKEN = DISCORD_TOKEN

    intents = discord.Intents.default()
    intents.messages = True  # Ensure the bot can read messages
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        channel = client.get_channel(int(channel_id))
        
        if channel:
            file = discord.File(file_name)
            await channel.send("Here are the scores:", file=file)

        await client.close()

    client.run(TOKEN)
