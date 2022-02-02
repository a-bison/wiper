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
import core.util as util
import core.wrapper
from core.exception import NotAdministrator
from core.util import ack

logging.basicConfig(level=logging.INFO)

# Invite: https://discord.com/oauth2/authorize?scope=applications.commands%20bot&permissions=120259160128&client_id=895670115515904000

DELETE_DELAY = 1.5

DEFAULT_CONFIG_DIR = Path("configdb")
DEFAULT_SECRET = "REPLACE WITH API TOKEN"

STARTUP_CONFIG_LOCATION = Path("startup.json")
STARTUP_CONFIG_TEMPLATE = {
    "secret": "REPLACE WITH API TOKEN",
    "config_dir": str(DEFAULT_CONFIG_DIR)
}

ENABLE_DEBUG_COMMANDS = False

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
        self.core.task(core.job.BlockerTask)
        self.core.task(core.wrapper.MessageTask)
        self.core.task(WipeTask)

        # Discord bot init
        self.bot.event(self.on_command_error)
        self.bot.add_cog(core.wrapper.JobManagement(self.core))
        self.bot.add_cog(Wiping(self.core))

        if ENABLE_DEBUG_COMMANDS:
            self.bot.add_cog(core.wrapper.JobDebug())
            self.bot.add_cog(Debug(self.core))

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
    def __init__(self, core):
        self.core = core

    async def parse_argument_list(self, ctx, arglist, converter, argname):
        arg_strs = [a.strip() for a in arglist.split(",") if a]
        invalid_strs = []
        args = []
        except_args = []

        for astr in arg_strs:
            try:
                if astr.startswith("-"):
                    a = await converter.convert(ctx, astr[1:])
                    except_args.append(a)
                elif astr.lower() == "all":
                    args.append("all")
                else:
                    a = await converter.convert(ctx, astr)
                    args.append(a)
            except commands.BadArgument:
                invalid_strs.append(astr)

        if invalid_strs:
            await ctx.send("The following {}s are invalid: {}".format(
                argname,
                ", ".join(invalid_strs)
            ))
            return None

        # If "all" appeared in arglist, replace args with "all"
        if "all" in args:
            args = "all"
        else:
            # If an argument appears in both args and except_args, remove it
            # from both.
            for e in list(except_args):
                if e in args:
                    except_args.remove(e)
                    args.remove(e)

        if not args:
            await ctx.send("No {}s specified.".format(argname))
            return None
    
        return args, except_args

    async def parse_channel_list(self, ctx, arglist):
        return await self.parse_argument_list(
            ctx,
            arglist,
            commands.TextChannelConverter(),
            "channel"
        )

    async def parse_member_list(self, ctx, arglist):
        return await self.parse_argument_list(
            ctx,
            arglist,
            commands.MemberConverter(),
            "member"
        )

    def args_to_ids(self, args):
        if args != "all":
            return [a.id for a in args]
        else:
            return args

    # Parse wipe args into a wipe properties dict.
    # Also, check permissions.
    async def parse_wipe_args(self, ctx, channels, older_than, members):
        # Channel
        if channels == None:
            channels_tuple = [ctx.channel], []
        else:
            channels_tuple = await self.parse_channel_list(ctx, channels)
            if channels_tuple is None:
                return None

        wipe_channels, except_channels = channels_tuple

        # Members
        if members == None:
            members_tuple = [ctx.author], []
        else:
            members_tuple = await self.parse_member_list(ctx, members)
            if members_tuple is None:
                return None

        wipe_members, except_members = members_tuple

        # Post age
        if older_than is None:
            cfg = await self.core.cfg(ctx)
            older_than = await cfg.aget("default_post_age")

        wipe_channels = self.args_to_ids(wipe_channels)
        except_channels = self.args_to_ids(except_channels)
        wipe_members = self.args_to_ids(wipe_members)
        except_members = self.args_to_ids(except_members)

        wipe_properties = {
            "channels": wipe_channels,
            "except_channels": except_channels,
            "olderthan": older_than,
            "members": wipe_members,
            "except_members": except_members
        }

        return wipe_properties

    # Check whether a user has permission to start a wipe job in the given
    # channel.
    async def check_channel_perms(self, channel, owner, wipe_properties):
        perms = channel.permissions_for(owner)
        members = wipe_properties["members"]

        # Silently ignore channels that can't be read.
        if not perms.read_messages:
            logging.error("test")
            return False

        if (not isinstance(members, str) and
            len(members) == 1 and
            members[0] == owner.id):
            # If members specifies a single user id that matches the owner,
            # allow deletion if the user can read this channel.
            return True
        else:
            # Otherwise, other members are specified, so check for manage
            # messages permission. In this case, raise an exception
            if not perms.manage_messages:
                raise PermissionException(
                    "Cannot delete other members' messages without the " +
                    "\"manage messages\" permission."
                )

        return True

    # Check if the given member has permission to start a wipe job with the
    # given properties. Also, delete channels that fail soft permission
    # checks. If a channel fails a hard permission check, sends an error
    # message and return False.
    async def check_permissions(self, ctx, owner, wipe_properties):
        guild = ctx.guild
        channels = wipe_properties["channels"]
        c = None

        if channels == "all":
            channels = self.args_to_ids(guild.channels)
        else:
            channels = list(channels)

        try:
            for c_id in channels:
                c = guild.get_channel(c_id)
 
                channel_ok = await self.check_channel_perms(c, owner, wipe_properties)

                if not channel_ok and wipe_properties["channels"] != "all":
                    logging.error("Permission check failure")
                    # If false, we failed the perms.read_messages check, so
                    # remove c_id
                    wipe_properties["channels"].remove(c_id)

        except PermissionException as e:
            await ctx.send("Error for channel {}: {}".format(
                c.name, str(e)
            ))
            return False
        
        if wipe_properties["channels"] == []:
            await ctx.send("No channels specified.")
            return False

        return True

    @commands.command(enabled=ENABLE_DEBUG_COMMANDS)
    @commands.is_owner()
    async def wipepermissiontest(self, ctx,
            channels: typing.Optional[str],
            older_than: typing.Optional[int],
            members: typing.Optional[str],
            owner: typing.Optional[discord.Member]):
        properties = await self.parse_wipe_args(
            ctx, channels, older_than, members
        )

        if properties is None:
            return

        if owner is None:
            owner = ctx.author

        await ctx.send(util.codejson(properties))

        if await self.check_permissions(ctx, owner, properties):
            await ctx.send("Permission check successful.")

        await ctx.send(util.codejson(properties))

    @commands.command(enabled=ENABLE_DEBUG_COMMANDS)
    @commands.is_owner()
    async def wipeparsetest(self, ctx,
            channels: typing.Optional[str],
            older_than: typing.Optional[int],
            members: typing.Optional[str]):
        """Test parsing of wipe command arguments"""
        properties = await self.parse_wipe_args(
            ctx, channels, older_than, members
        )

        await ctx.send(util.codejson(properties))

    @commands.command()
    async def wipenow(self, ctx,
            channels: typing.Optional[str],
            older_than: typing.Optional[int],
            members: typing.Optional[str]):
        """Start a wipe job immediately.

        channels - Channel specifier. Can be "all", or a list of channels
                   to wipe. Examples:

                   "general,memes" - wipe the general and memes channel
                   "all,-general" - wipe everything but general
                   "all" - wipe everything

                   In general, a channel may be deleted by specifying its name
                   in the comma-separated list. If a channel has a - in front
                   of it, it will be added as an exception and will not be
                   wiped.

        older_than - Wipe messages older than this number, in days. If not
                     specified, this will be taken from the default_post_age
                     config.

        members - Member specifier. Can be "all", or a list of members to wipe.
                  Syntax is the same as channels, except with members.

        """
        properties = await self.parse_wipe_args(
            ctx, channels, older_than, members
        )

        if properties is None:
            return

        if await self.check_permissions(ctx, ctx.author, properties):
            job = await self.core.start_job(ctx, WipeTask, properties)
            await ack(ctx)
            await ctx.send("Wipe job started. ID is {}.".format(job.header.id))

    @commands.command()
    @commands.is_owner()
    async def wipelater(self, ctx,
            schedule: str,
            channels: typing.Optional[str],
            older_than: typing.Optional[int],
            members: typing.Optional[str]):
        """Schedule a wipe job to occur at a regular interval.
        
        This command is temporarily owner only.

        schedule - A cron-format string that specifies the schedule.

        channels - Channel specifier. Can be "all", or a list of channels
                   to wipe. Examples:

                   "general,memes" - wipe the general and memes channel
                   "all,-general" - wipe everything but general
                   "all" - wipe everything

                   In general, a channel may be deleted by specifying its name
                   in the comma-separated list. If a channel has a - in front
                   of it, it will be added as an exception and will not be
                   wiped.

        older_than - Wipe messages older than this number, in days. If not
                     specified, this will be taken from the default_post_age
                     config.

        members - Member specifier. Can be "all", or a list of members to wipe.
                  Syntax is the same as channels, except with members.
        """
        properties = await self.parse_wipe_args(
            ctx, channels, older_than, members
        )

        if properties is None:
            return

        try:
            core.job.cron_parse(schedule)
        except core.job.ScheduleParseException as e:
            await ctx.send("Could not parse cron str: " + str(e))
            return

        if not await self.check_permissions(ctx, ctx.author, properties):
            return

        sh = await self.core.schedule_job(ctx, WipeTask, properties, schedule)
        await ack(ctx)
        await ctx.send("Wipe job scheduled. Schedule ID is {}.".format(sh.id))


class PermissionException(Exception):
    pass


class WipeTask(core.job.JobTask):
    def __init__(self, bot, guild):
        self.bot = bot
        self.guild = guild

    @classmethod
    def property_default(cls, properties):
        return {
            # list of ints, or the string "all".
            # if list of ints, only delete the messages in the given channels.
            # Note that "all" will only cover channels the user that started
            # this job will have access to.
            "channels": [],

            # list of ints. do not delete the channel ids supplied here.
            "except_channels": [],

            # delete messages older than N days. This is picked up from a
            # cfg option, this is just here as a placeholder
            "olderthan": 7,

            # list of member ids, or the string "all".
            # if list of ints, only delete the messages belonging to
            # the given list.
            "members": [],

            # list of ints. do not delete messages belonging to the member ids
            # supplied here.
            "except_members": []
        }

    @classmethod
    def task_type(cls):
        return "wipe"

    # Get channels to delete from the properties dict.
    def get_delete_channels(self, p):
        if p["channels"] == "all":
            channels = {channel.id: channel for channel in self.guild.channels}
            print(channels)
        else:
            channels = {id: self.guild.get_channel(id) for id
                        in p["channels"]}
            print(channels)

        if p["except_channels"]:
            for id in p["except_channels"]:
                del channels[id]

        return channels

    def include_msg(self, p, msg):
        if p["members"] == "all":
            return True
        else:
            return msg.author.id in p["members"]

    def exclude_msg(self, p, msg):
        return msg.author.id in p["except_members"]

    # Determine whether a message should be deleted based on msg and properties
    # dict.
    def match_msg(self, p, msg):
        return include_msg(p, msg) and not exclude_msg(p, msg)

    # Delete history of a single channel. Returns number of messages deleted.
    async def wipe_channel(self, p, channel, datetime):
        msgcount = 0

        async for message in channel.history(limit=None, before=datetime):
            await asyncio.sleep(DELETE_DELAY)
            await message.delete()
            msgcount += 1

            if msgcount % 10 == 0:
                logging.info("wipe_channel: wiped {}".format(msgcount))

        logging.info("wipe_channel: wiped {} msg(s) in channel {}".format(
            msgcount, channel.name
        ))

        return msgcount

    # Perform one wipe cycle. Go through all channels and delete as many
    # messages as the discord API will allow. If we try to delete messages
    # in a channel, and there's nothing to do, mark that channel as complete.
    # The wipe job is complete when there is nothing to delete across all
    # channels.
    async def wipe_cycle(self, header, channels, datetime):
        p = header.properties
        complete_channels = {}

        for channel in channels.values():
            msgs_deleted = await self.wipe_channel(p, channel, datetime)
            if msgs_deleted == 0:
                complete_channels[channel.id] = channel

        return complete_channels

    # Test if there's more to delete based on completed_channels and the
    # output of self.get_delete_channels()
    def more_to_delete(self, completed_channels, delete_channels):
        return not all([(c in completed_channels) for c in delete_channels])

    async def run(self, header):
        p = header.properties
        date_time = older_than(days=p["olderthan"])
        completed_channels = {}

        channels = self.get_delete_channels(p)
        while self.more_to_delete(completed_channels, channels):
            complete = await self.wipe_cycle(header, channels, date_time)
            completed_channels.update(complete)
            channels = self.get_delete_channels(p)

        finishtime = datetime.now()
        starttime = datetime.fromtimestamp(header.start_time)

        owner = self.bot.get_user(header.owner_id)
        delta = finishtime - starttime
        hours = round(delta.total_seconds() / (60*60), 3)
        msg = "Wipe job in {} completed in {} hours.".format(
            self.bot.get_guild(header.guild_id).name,
            hours
        )
    
        await owner.send(msg)


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
