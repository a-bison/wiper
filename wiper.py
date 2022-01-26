import discord
from discord.ext import commands

from datetime import datetime, timedelta
from pathlib import Path
import asyncio
import logging
import typing

import core.config
import core.job

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

def check_administrator():
    async def predicate(ctx):
        return ctx.author.guild_permissions.administrator

    return commands.check(predicate)


class NotAdministrator(discord.DiscordException):
    def __init__(self, task):
        self.task = task


# Workaround until I read the docs better
async def process_user_optional(ctx, member, rest):
    too_many_args = "Too many arguments. Try \"{}\"."
    nosuchuser = "Could not find user {}. Try exact case or a mention."

    if member and rest:
        correct = " ".join([member.name, *rest])
        await ctx.send(too_many_args.format(correct))
        return None

    if not member and rest:
        # the member conversion failed.
        if len(rest) > 1:
            await ctx.send(too_many_args.format(" ".join(rest)))
            return None

        await ctx.send(nosuchuser.format(rest[0]))
        return None

    if not member:
        return ctx.author

    return member

# React with a check mark for confirmation
async def ack(ctx):
    await ctx.message.add_reaction("\U00002705")


class Wiper:
    def __init__(self, secret, config_path):
        self.intents = discord.Intents.default()
        self.intents.members = True

        self.bot = commands.Bot(command_prefix="w!",
                                case_insensitive=True,
                                intents=self.intents,
                                description=(
                                    "Automatically wipe old messages "
                                    "from discord servers."))

        self.cfgtemplate = {
            "jobs": {},
            "cron": {},
            "default_post_age": 7 # days
        }

        self.common_cfgtemplate = {
            # save the last schedule id so we don't overlap new schedules
            # with old ones
            "last_schedule_id": 0
        }

        self.config_db = core.config.JsonConfigDB(
            Path(config_path),
            template=self.cfgtemplate,
            main_template=self.common_cfgtemplate
        )
        self.secret = secret

        # Available tasks
        self.task_registry = core.job.TaskRegistry()
        self.task_registry.register(MessageTask)
        self.task_registry.register(BlockerTask)

        # Job executer/consumer component
        self.jobqueue = core.job.JobQueue(self.bot.loop)
        self.jobqueue.on_job_submit(self._cfg_job_create)
        self.jobqueue.on_job_stop(self._cfg_job_delete)
        self.jobqueue.on_job_cancel(self._cfg_job_delete)
        self.jobfactory = core.job.DiscordJobFactory(self.task_registry, self)
        self.jobtask = None

        # Job scheduler component
        self.jobcron = core.job.JobCron(self.jobqueue, self.jobfactory)
        self.jobcron.on_create_schedule(self._cfg_sched_create)
        self.jobcron.on_delete_schedule(self._cfg_sched_delete)
        self.cronfactory = core.job.DiscordCronFactory(
            self.config_db.get_common_config().opts["last_schedule_id"] + 1
        )
        self.crontask = None

        # Register discord events
        # TODO On guild leave
        self.bot.event(self.on_guild_join)
        self.bot.event(self.on_ready)
        self.bot.event(self.on_command_error)

        self.bot.add_cog(Debug(self))
        self.bot.add_cog(JobManagement(self))

        self.jobs_resumed = False

    def run(self):
        loop = self.bot.loop

        try:
            # Create job consumer and scheduler
            self.jobtask = loop.create_task(self.jobqueue.run())
            self.crontask = loop.create_task(self.jobcron.run())

            # Perform initialization and log in
            loop.run_until_complete(self.bot.login(self.secret))
            loop.run_until_complete(self.join_guilds_offline())
            loop.run_until_complete(self.bot.connect())

        except KeyboardInterrupt:
            print("Keyboard Interrupt!")
        finally:
            loop.close()

    # Resume all jobs that never properly finished from the last run.
    # Called from on_ready() to ensure that all discord state is init'd
    # properly
    async def resume_jobs(self):
        for guild_id, cfg in self.config_db.db.items():
            jobs = await cfg.sub_get_all_and_clear("jobs")

            for job_id, job_header in jobs.items():
                await self.resume_job(job_header)

            msg = "Resumed {} unfinished job(s) in guild {}"
            logging.info(msg.format(len(jobs), guild_id))

    async def reschedule_cron(self, header_dict):
        header = await self.cronfactory.create_cronheader_from_dict(header_dict)
        await self.jobcron.create_schedule(header)

    # Reschedule all cron entries from cfg
    async def reschedule_all_cron(self):
        for guild_id, cfg in self.config_db.db.items():
            crons = await cfg.sub_get_all_and_clear("cron")

            for sched_id, sched_header in crons.items():
                await self.reschedule_cron(sched_header)

            msg = "Loaded {} schedule(s) in guild {}"
            logging.info(msg.format(len(crons), guild_id))

    # Get the config object for a given job/cron header.
    async def get_cfg_for_header(self, header):
        guild = self.bot.get_guild(header.guild_id)
        cfg = await self.config_db.get_config(guild)

        return cfg

    # When a job is submitted, create an entry in the config DB.
    async def _cfg_job_create(self, header):
        cfg = await self.get_cfg_for_header(header)
        await cfg.sub_set("jobs", str(header.id), header.as_dict())

    # Once a job is done, delete it from the config db.
    async def _cfg_job_delete(self, header):
        cfg = await self.get_cfg_for_header(header)
        await cfg.sub_delete("jobs", str(header.id))

    # Add created schedules to the config DB, and increase the
    # last_schedule_id parameter.
    async def _cfg_sched_create(self, header):
        cfg = await self.get_cfg_for_header(header)
        await cfg.sub_set("cron", str(header.id), header.as_dict())

        common_cfg = self.config_db.get_common_config()
        await common_cfg.get_and_set(
            "last_schedule_id",
            lambda val: max(val, header.id)
        )

    # Remove deleted schedules from the config DB.
    async def _cfg_sched_delete(self, header):
        cfg = await self.get_cfg_for_header(header)
        await cfg.sub_delete("cron", str(header.id))

    def get_guild(self, id):
        return self.bot.get_guild(id)

    # Resume job from a loaded job header dict.
    async def resume_job(self, header):
        job = await self.jobfactory.create_job_from_dict(header)
        await self.jobqueue.submit_job(job)

    # Enqueue a new job
    async def start_job(self, ctx, task_type, properties):
        job = await self.jobfactory.create_job(ctx, task_type, properties)
        await self.jobqueue.submit_job(job)

    # Create configs for any guilds we were added to while offline
    async def join_guilds_offline(self):
        async for guild in self.bot.fetch_guilds():
            logging.info("In guilds: {}({})".format(guild.name, guild.id))
            _ = await self.config_db.get_config(guild)

        await self.config_db.write_db()

    async def on_ready(self):
        if not self.jobs_resumed:
            await self.reschedule_all_cron()
            await self.resume_jobs()
            self.jobs_resumed = True

        logging.info("Wiper ready.")

    # Add a new config slice if we join a new guild.
    async def on_guild_join(self, guild):
        logging.info("Joined new guild: {}({})".format(guild.name, guild.id))
        await self.config_db.create_config(guild)

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
        return older_than(days=await cfg.get("default_post_age"))

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


class JobManagement(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.jq = bot.jobqueue
        self.jc = bot.jobcron
    
    def pretty_print_job(self, guild, job):
        h = job.header
        owner = self.bot.bot.get_user(h.owner_id).name
        s = "{}: owner={} type={}".format(h.id, owner, h.task_type)
        
        if h.schedule_id is not None:
            s += " sched=" + str(h.schedule_id)

        task_str = job.task.display(job.header)

        if task_str:
            s += " " + task_str

        return s

    # Get jobs valid for a specific guild.
    def get_guild_jobs(self, guild):
        return {id: j for id, j in self.jq.jobs.items()
                if j.header.guild_id == guild.id}

    # List jobs for a given guild.
    @commands.command()
    async def joblist(self, ctx):
        """List enqueued jobs."""
        jobs = self.get_guild_jobs(ctx.guild)
        joblines = [self.pretty_print_job(ctx.guild, j) for j in jobs.values()]
        
        if joblines:
            msg = "```\n{}\n```".format("\n".join(joblines))
            await ctx.send(msg)
        else:
            await ctx.send("No jobs.")

    @commands.command()
    async def jobcancel(self, ctx, id: int):
        """Cancel a job listed by ?joblist.

        Members may only cancel jobs they started. Users with the Administrator
        permission may cancel any job.
        """
        jobs = self.get_guild_jobs(ctx.guild)
        
        if id in jobs:
            if (not ctx.author.guild_permissions.administrator and
                jobs[id].header.owner_id != ctx.member.id):
                raise NotAdministrator("cancel jobs started by other users")

            await self.jq.canceljob(id)
            await ack(ctx)
        else:
            await ctx.send("Job {} does not exist.".format(id))

    @commands.command()
    async def jobcanceluser(self, ctx, user: typing.Optional[discord.Member], *rest):
        """Cancel all jobs started by a specific user.

        Unless you're administrator, you'll only be able to cancel your
        own jobs.
        """
        corrected_user = await process_user_optional(ctx, user, rest)

        if corrected_user is None:
            return

        if (not ctx.author.guild_permissions.administrator and
            corrected_user.id != ctx.member.id):
            raise NotAdministrator("cancel jobs started by other users")
            return

        jobs = [j for _, j in self.get_guild_jobs(ctx.guild).items()
                if j.header.owner_id == corrected_user.id]

        if not jobs:
            await ctx.send("No jobs to delete.")
            return

        for j in reversed(jobs):
            await self.jq.canceljob(j.header.id)

        await ack(ctx)


    @commands.command()
    @check_administrator()
    async def jobcancelall(self, ctx):
        """Cancel all jobs. Only Administrators may use this command.
        """
        jobs = self.get_guild_jobs(ctx.guild)
        
        for id, jobs in reversed(jobs.items()):
            await self.jq.canceljob(id)

        await ack(ctx)


    @commands.command()
    @commands.is_owner()
    async def jobflush(self, ctx):
        """Cancel all jobs currently scheduled, across all servers.

        You must be the owner of the bot to use this command.
        """
        for id, job in reversed(self.jq.jobs.items()):
            await self.jq.canceljob(id)

        await ack(ctx)

    def pretty_print_cron(self, cron):
        owner = self.bot.bot.get_user(cron.owner_id).name

        s = "{}: owner={} type={} sched=\"{}\" params={} nextrun=\"{}\""
        s = s.format(
            cron.id,
            owner,
            cron.task_type,
            cron.schedule,
            str(cron.properties),
            cron.next.strftime("%c") if cron.next is not None else "null"
        )

        return s

    # Get schedule for a given guild.
    def get_guild_sched(self, guild):
        return {id: c for id, c in self.jc.schedule.items()
                if c.guild_id == guild.id}

    @commands.command()
    async def cronlist(self, ctx):
        """List scheduled jobs."""
        crons = self.get_guild_sched(ctx.guild)
        cronlines = [self.pretty_print_cron(cron) for cron in crons.values()]

        if cronlines:
            msg = "```\n{}\n```".format("\n".join(cronlines))
            await ctx.send(msg)
        else:
            await ctx.send("Nothing scheduled.")
        
    @commands.command()
    @commands.is_owner()
    async def croncreate(self, ctx, task_type: str, cronstr: str, *, params_json):
        """Create a schedule for an arbitrary job. Bot owner only."""
        if task_type not in self.bot.task_registry:
            await ctx.send("Task type \"{}\" is not available.".format(
                task_type    
            ))
            return

        params_json = params_json.strip()

        if params_json:
            params_dict = json.loads(params_json)
        else:
            params_dict = {}

        try: 
            chdr = await self.bot.cronfactory.create_cronheader(
                ctx,
                params_dict,
                task_type,
                cronstr
            )

            await self.jc.create_schedule(chdr)
            await ack(ctx)

        except ScheduleParseException as e:
            msg = "Could not parse cron str \"{}\": {}"
            await ctx.send(msg.format(cronstr, str(e)))

    @commands.command()
    async def crondelete(self, ctx, id: int):
        """Delete a schedule."""
        crons = self.get_guild_sched(ctx.guild)

        if id in crons:
            if (not ctx.author.guild_permissions.administrator and
                crons[id].owner_id != ctx.member.id):
                raise NotAdministrator("delete schedules created by other users")

            await self.jc.delete_schedule(id)
            await ack(ctx)
        else:
            await ctx.send("Schedule {} does not exist.".format(id))


class BlockerTask(core.job.JobTask):
    def __init__(self, bot, guild):
        self.bot = bot
        self.guild = guild

    @classmethod
    def task_type(cls):
        return "blocker"

    async def run(self, header):
        while True:
            await asyncio.sleep(1)

    def display(self, header):
        return ""


class MessageTask(core.job.JobTask):
    MAX_MSG_DISPLAY_LEN = 15

    def __init__(self, bot, guild):
        self.bot = bot
        self.guild = guild
    
    # Properties:
    #
    # {
    #     "message": MESSAGE
    #     "channel": CHANNEL_ID
    #     "post_interval": NUMBER, SECONDS
    #     "post_number": NUMBER
    # }
    async def run(self, header):
        p = header.properties
        channel = self.guild.get_channel(p["channel"])

        for _ in range(p["post_number"]):
            await channel.send(p["message"])
            await asyncio.sleep(p["post_interval"])

    @classmethod
    def task_type(cls):
        return "message"

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
            age = await cfg.get("default_post_age")
            await ctx.reply("Default post age is {}.".format(age))
            return

        await cfg.set("default_post_age", age)
        await ack(ctx)


class Debug(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.cdb = bot.config_db

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
        await self.bot.start_job(ctx, MessageTask, properties)
        await ack(ctx)

    @commands.command()
    async def blockerjob(self, ctx):
        await self.bot.start_job(ctx, BlockerTask, {})
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

    @commands.command()
    async def testcronparse(self, ctx, cron: str):
        """Test parsing of cron strings."""
        try:
            s_dict = core.job.cron_parse(cron)
        except ScheduleParseException as e:
            await ctx.send("Could not parse cron str: " + str(e))
            return

        s = json.dumps(s_dict, indent=4)
        await ctx.send("```\n{}\n```".format(s))

    @commands.command()
    async def testcronmatch(self, ctx, cron:str, date_time:str):
        """Test matching a cron string to a date.

        Date must be provided in ISO format:
        YYYY-MM-DDThh:mm:ss
        """
        try:
            if core.job.cron_match(cron, datetime.fromisoformat(date_time)):
                await ctx.send("Schedule match.")
            else:
                await ctx.send("No match.")

        except ScheduleParseException as e:
            await ctx.send("Could not parse cron str: " + str(e))
            return
        except ValueError as e:
            await ctx.send(str(e))
            return


    @commands.command()
    async def testcronnext(self, ctx, cron:str, date_time: typing.Optional[str]):
        """Test functionality to predict next run of a cron schedule.

        Date must be provided in ISO format:
        YYYY-MM-DDThh:mm:ss

        If date_time is not given, the current date will be used instead.
        """
        try:
            if date_time is None:
                date_time = datetime.now()
            else:
                date_time = datetime.fromisoformat(date_time)

            s = core.job.cron_parse(cron)
        except ScheduleParseException as e:
            await ctx.send("Could not parse cron str: " + str(e))
            return
        except ValueError as e:
            await ctx.send(str(e))
            return

        next_date_time = core.job.cron_next_date_as_datetime(s, date_time)

        message  = "```\nFrom {}\n"
        message += "{} will next run\n"
        message += "     {}\n```"

        await ctx.send(message.format(
            date_time.strftime("%c"),
            cron,
            next_date_time.strftime("%c")
        ))


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
