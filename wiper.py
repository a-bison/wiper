import discord
from discord.ext import commands

from datetime import datetime, timedelta
from pathlib import Path
import asyncio
import json
import logging
import typing

import core.config
import core.job
import core.util
import core.wrapper
from core.exception import NotAdministrator
from core.util import ack

logging.basicConfig(level=logging.INFO)

# Invite: https://discord.com/oauth2/authorize?scope=applications.commands%20bot&permissions=120259160128&client_id=895670115515904000

MAX_BULK_DELETE_MESSAGES = 100
DELETE_DELAY = 1
MAX_JOBS = 4

DEFAULT_CONFIG_DIR = Path("configdb")
DEFAULT_SECRET = "REPLACE WITH API TOKEN"

STARTUP_CONFIG_LOCATION = Path("startup.json")
STARTUP_CONFIG_TEMPLATE = {
    "secret": "REPLACE WITH API TOKEN",
    "config_dir": str(DEFAULT_CONFIG_DIR)
}

# TODO: Add backups of the config directory to protect against errors in
# config writing code.

class Wiper:
    def __init__(self, secret, config_path):
        self.intents = discord.Intents.default()
        self.intents.members = True

        self.secret = secret
        self.bot = commands.Bot(command_prefix="w!",
                                case_insensitive=True,
                                intents=self.intents,
                                description=(
                                    "Automatically wipe old messages "
                                    "from discord servers."))

        self.cfgtemplate = {
            "default_post_age": 7 # days
        }
        self.common_cfgtemplate = {}

        # Bot core initialization
        self.core = core.wrapper.CoreWrapper(
            self.bot,
            Path(config_path),
            self.cfgtemplate,
            self.common_cfgtemplate
        )
        self.core.task(BlockerTask)
        self.core.task(MessageTask)

        # Discord bot init
        self.bot.event(self.on_command_error)
        self.bot.add_cog(Debug(self.core))
        self.bot.add_cog(core.wrapper.JobManagement(self.core))
        self.bot.add_cog(core.wrapper.JobDebug())

    def run(self):
        self.core.run(self.secret)

    async def on_command_error(self, ctx, error):
        # If it's a generic command invoke error, use the cause exception.
        if isinstance(error, commands.errors.CommandInvokeError):
            if error.__cause__:
                error = error.__cause__

        if isinstance(error, commands.errors.CommandNotFound):
            await ctx.send("Unknown command.")
        elif isinstance(error, NotAdministrator):
            msg = "You must have the Administrator role to {}."
            await ctx.send(msg.format(error.task.lower()))
        else:
            msg = "Unexpected error in command:"
            logging.error(msg)
            raise error


# Delete everything in the given channel before date_time
async def wipe(channel, date_time):
    msgcount = 0

    async for message in channel.history(limit=None, before=date_time):
        msgcount += 1
        await asyncio.sleep(DELETE_DELAY)
        await message.delete()

    return msgcount


def older_than(**args):
    return datetime.now() - timedelta(**args)


class Wiping(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.cdb = bot.config_db

    async def get_default_limit(self):
        cfg = await self.cdb.get_config(ctx.guild)
        return older_than(days=await cfg.aget("default_post_age"))

    @commands.command()
    @commands.is_owner()
    async def wipechannel(self, ctx):
        msgcount = await wipe(ctx.channel, await self.get_default_limit())
        await ctx.reply("Done, deleted {} messages.".format(msgcount))

    @commands.command()
    @commands.is_owner()
    async def wipeserver(self, ctx):
        limit = await self.get_default_limit()

        channels = ctx.guild.text_channels
        await ctx.reply("Wiping {} channels...".format(len(channels)))

        msgcount = 0
        for channel in channels:
            msgcount += await wipe(channel, limit)

        await ctx.reply("Done, deleted {} messages across {} channels.".format(msgcount, len(channels)))


class BlockerTask(core.job.JobTask):
    # Ignore any arguments passed in to retain compatibility with all job
    # factories.
    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def task_type(cls):
        return "blocker"

    @classmethod
    def property_default(cls, properties):
        return {
            "time": 60 # seconds. if None, loops forever.
        }

    async def run(self, header):
        p = header.properties
        time = p["time"]

        if time is None:
            while True: 
                await asyncio.sleep(1)
        else:
            counter = time

            while counter > 0:
                await asyncio.sleep(1)
                counter -= 1

    def display(self, header):
        return ""


class MessageTask(core.job.JobTask):
    MAX_MSG_DISPLAY_LEN = 15

    def __init__(self, bot, guild):
        self.bot = bot
        self.guild = guild

    async def run(self, header):
        p = header.properties
        channel = self.guild.get_channel(p["channel"])

        for _ in range(p["post_number"]):
            await channel.send(p["message"])
            await asyncio.sleep(p["post_interval"])

    @classmethod
    def task_type(cls):
        return "message"

    @classmethod
    def property_default(cls, properties):
        return {
            "message": "hello",
            "channel": 0,
            "post_interval": 1, # SECONDS
            "post_number": 1
        }

    def display(self, header):
        p = header.properties
        msg = p["message"]

        if len(msg) > MessageTask.MAX_MSG_DISPLAY_LEN:
            msg = msg[0:MessageTask.MAX_MSG_DISPLAY_LEN] + "..."

        fmt = "message=\"{}\" post_interval={} post_number={}"

        return fmt.format(msg, p["post_interval"], p["post_number"])


class WipeTask(core.job.JobTask):
    # Properties:
    #
    # {
    #     "wipetype": "channel" | "server"
    #     "olderthan": DAYS
    #     "member" (optional): MEMBER_ID
    #     "channel" (optional): CHANNEL
    # }
    async def run(self, header):
        p = header.properties
        wt = p["wipetype"]
        if wt == "channel":
            msgcount = await wipe(ctx.channel, await self.get_default_limit())

        pass

    @classmethod
    def task_type(cls):
        return "wipe"


class Config(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.cdb = bot.config_db

    @commands.command(name="default-post-age")
    @commands.is_owner()
    async def default_post_age(self, ctx, age: typing.Optional[int]):
        cfg = await self.cdb.get_config(ctx.guild)

        if age is None:
            age = await cfg.aget("default_post_age")
            await ctx.reply("Default post age is {}.".format(age))
            return

        await cfg.aset("default_post_age", age)
        await ack(ctx)


class Debug(commands.Cog):
    def __init__(self, core):
        self.core = core

    @commands.command()
    async def messagejob(self, ctx, message: str, repeat: int, interval: int):
        if interval < 5:
            await ctx.send("Interval may not be less than 5 secs.")
            return

        properties = {
            "message": message,
            "channel": ctx.channel.id,
            "post_interval": interval,
            "post_number": repeat
        }

        logging.info("MessageJob: " + str(properties))
        await self.core.start_job(ctx, MessageTask, properties)
        await ack(ctx)

    @commands.command()
    async def blockerjob(self, ctx):
        await self.core.start_job(ctx, BlockerTask, {})
        await ack(ctx)

    @commands.command()
    async def testerror(self, ctx, error, *rest):
        """Raise a given error to test error handling.

        Valid errors:
        notadmin - NotAdministrator(task: str)"""
        if error == "notadmin":
            if len(rest) < 1:
                await ctx.send("Not enough arguments for 'notadmin' error.")
                return

            raise NotAdministrator(rest[0])

        else:
            await ctx.send("Unknown error '{}'.".format(error))


def main():
    startup_cfg = core.config.JsonConfig(STARTUP_CONFIG_LOCATION,
                                         template=STARTUP_CONFIG_TEMPLATE)

    if startup_cfg.opts["secret"] == DEFAULT_SECRET:
        logging.error("Please open {} and replace the secret with a proper API token.".format(STARTUP_CONFIG_LOCATION))
        logging.error("For more information, see https://www.writebots.com/discord-bot-token/")
        return

    wiper = Wiper(secret=startup_cfg.opts["secret"],
                  config_path=startup_cfg.opts["config_dir"])
    wiper.run()


if __name__ == "__main__":
    main()
