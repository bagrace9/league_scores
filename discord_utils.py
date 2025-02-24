import discord
import configparser

# Read configuration
config = configparser.ConfigParser()
config.read('config.ini')

def upload_scores_to_discord(self,file_name, channel_id):
    # Upload scores to a Discord channel
    TOKEN = self.DISCORD_TOKEN

    intents = discord.Intents.default()
    intents.messages = True  # Ensure the bot can read messages
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        # Event handler for when the bot is ready
        channel = client.get_channel(int(channel_id))
        
        if channel:
            file = discord.File(file_name)
            await channel.send("Here are the scores:", file=file)

        await client.close()

    # Run the Discord bot
    client.run(TOKEN)
