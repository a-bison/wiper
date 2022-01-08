import discord
from discord.ext import commands

import asyncio
import collections
import json
from pathlib import Path
import logging
from datetime import datetime, timedelta
import time
import typing

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


def check_administrator():
    async def predicate(ctx):
        return ctx.author.guild_permissions.administrator

    return commands.check(predicate)


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


class AsyncAtomicCounter:
    def __init__(self):
        self.count = 0
        self.lock = asyncio.Lock()

    async def get_and_increment(self):
        async with self.lock:
            return self.get_and_increment_unsafe()

    def get_and_increment_unsafe(self):
        n = self.count
        self.count += 1
        return n


# A single job queue. Can run one job at a time.
class JobQueue:
    def __init__(self, eventloop=None):
        if eventloop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = eventloop

        self.active_job = None
        self.active_task = None
        self.job_queue = asyncio.Queue()

        # Job dict used for display purposes, because asyncio.Queue doesn't
        # support peeking
        self.jobs = collections.OrderedDict()

        self.job_start_callback = None
        self.job_stop_callback = None
        self.job_cancel_callback = None

    async def submit_job(self, job):
        await self.job_queue.put(job)

        self.jobs[job.header.id] = job

    def on_job_start(self, callback):
        self.job_start_callback = callback

    def on_job_stop(self, callback):
        self.job_stop_callback = callback

    def on_job_cancel(self, callback):
        self.job_cancel_callback = callback

    def _rm_job(self, job):
        if job is None:
            return

        if job.header.id in self.jobs:
            del self.jobs[job.header.id]

        self.active_job = None
        self.active_task = None

    async def run(self):
        logging.info("Starting job queue...")

        try:
            while True:
                await self.mainloop()
        except:
            logging.exception("Job queue stopped unexpectedly!")
        finally:
            logging.info("Job queue stoppped.")

    async def mainloop(self):
        j = await self.job_queue.get()

        if j.header.cancel:
            logging.info("Skipping cancelled job " + str(j.header.id))
            self._rm_job(self.active_job)
            return

        logging.info("Start new job " + str(j.header.as_dict()))
        self.active_job = j

        # Schedule task
        coro = self.active_job.task.run(self.active_job.header)
        task = self.loop.create_task(coro)
        self.active_task = task

        if self.job_start_callback:
            await self.job_start_callback(j.header)

        try:
            await task
        except asyncio.CancelledError:
            logging.warning("Uncaught CancelledError in job " + str(j.header.id))
        except:
            logging.exception("Got exception while running job")
        finally:
            self._rm_job(self.active_job)

        if self.job_stop_callback:
            await self.job_stop_callback(j.header)


    async def canceljob(self, job):
        if isinstance(job, JobTask):
            job = job.header.id

        self.jobs[job].header.cancel = True

        if self.active_job.header.id == job and self.active_task:
            self.active_task.cancel()

        if self.job_cancel_callback:
            await self.job_cancel_callback(self.jobs[job].header)

        del self.jobs[job]


class DiscordJobFactory:
    def __init__(self, bot):
        self.id_counter = AsyncAtomicCounter()
        self.bot = bot

        self.task_types = {}
    
    def register_task_type(self, cls):
        if not hasattr(cls, "task_type"):
            raise Exception("Task class must have task_type attr")

        self.task_types[cls.task_type] = cls

    # create a new job header
    async def create_jobheader(self, ctx, properties, task_type, schedule_id):
        header = JobHeader(
            await self.id_counter.get_and_increment(),
            task_type,
            properties,
            ctx.message.author.id,
            ctx.guild.id,
            int(time.time()),
            schedule_id
        )

        return header

    async def create_jobheader_from_dict(self, header):
        return JobHeader.from_dict(
            await self.id_counter.get_and_increment(),
            header
        )

    async def create_job(self, ctx, task_type, properties, schedule_id=None):
        if not isinstance(task_type, str):
            task_type = task_type.task_type

        header = await self.create_jobheader(ctx, properties, task_type, schedule_id)

        task_cls = self.task_types[task_type]
        task = task_cls(self.bot, ctx.guild)
        j = Job(header, task)

        return j

    # Create a job from an existing dictionary (typically loaded from cfg)
    async def create_job_from_dict(self, header, guild):
        header = await self.create_jobheader_from_dict(header)

        task_cls = self.task_types[header.task_type]
        task = task_cls(self.bot, guild)
        j = Job(header, task)

        return j


class Job:
    def __init__(self, header, task):
        self.header = header
        self.task = task


# Metadata for tracking a job, for scheduling and persistence purposes.
class JobHeader:
    @classmethod
    def from_dict(self, id, d):
        return JobHeader(
            id, # ignore loaded id
            d["task_type"],
            d["properties"],
            d["owner_id"],
            d["guild_id"],
            d["start_time"],
            d["schedule_id"]
        )

    def __init__(self, id, task_type, properties, owner_id, guild_id, start_time, schedule_id=None):
        self.id = id

        # Arguments to the job.
        self.properties = properties

        # Integer ID of the schedule that spawned this job. If no schedule ID,
        # then this will be None/null.
        self.schedule_id = schedule_id

        # Member ID of the discord user that started this job.
        # If this job was started by a schedule, this will reflect the
        self.owner_id = owner_id

        # Guild ID of the guild this job was started in
        self.guild_id = guild_id

        # The date the job was started, as a unix timestamp, in UTC time.
        self.start_time = start_time

        # The task type string.
        self.task_type = task_type

        # Is the job cancelled?
        self.cancel = False

    def as_dict(self):
        return {
            "id": self.id,
            "properties": self.properties,
            "schedule_id": self.schedule_id,
            "owner_id": self.owner_id,
            "guild_id": self.guild_id,
            "start_time": self.start_time,
            "task_type": self.task_type
        }


class JobTask:
    task_type = "NONE"

    async def run(self, header):
        raise NotImplemented("Subclass JobTask to implement specific tasks.")

    # Pretty print info about this task. This should only return information
    # included in the header.properties dictionary. Higher level information
    # is the responsibility of the caller.
    def display(self, header):
        msg = "display() not implemented for task {}"
        logging.warning(msg.format(header.task_type))

        return ""


# Very simple config database consisting of json files on disk.
# Saves a different version of the config depending on the guild.
#
# On disk structure:
# config_root_dir \ _ <guild_id_1>.json
#                  |_ <guild_id_2>.json
#
class JsonConfigDB:
    def __init__(self, path, template=None):
        self.path = path
        self.create_lock = asyncio.Lock()
        self.db = {}
        self.template = template

        if path.is_dir():
            self.load_db()
        elif path.exists():
            msg = "config {} is not a directory"
            raise FileExistsError(msg.format(str(path)))
        else: # No file or dir, so create new
            self.create_new_db()
        
    # Creates a new config DB
    def create_new_db(self):
        try:
            self.path.mkdir()
        except FileNotFoundError:
            logging.error("Parent directories of config not found.")
            raise

    def cfg_loc(self, guild_id):
        return self.path / (str(guild_id) + ".json")

    # Loads the entire DB from a directory on disk.
    # Note that this will override any configuration currently loaded in
    # memory.
    def load_db(self):
        self.db = {}

        for child in self.path.iterdir():
            try:
                guild_id = int(child.stem)
            except ValueError:
                continue

            self.db[guild_id] = JsonConfig(self.cfg_loc(guild_id),
                                           self.template)
            logging.info("Load config: guild id {}".format(guild_id))

    async def write_db(self):
        for cfg in self.db.values():
            await cfg.write()

    # Gets the config for a single guild. If the config for a guild doesn't
    # exist, create it.
    async def get_config(self, guild):
        if guild.id not in self.db:
            await self.create_config(guild)

        return self.db[guild.id]

    async def create_config(self, guild):
        async with self.create_lock:
            self.db[guild.id] = JsonConfig(self.cfg_loc(guild.id), self.template)


# Simple on-disk persistent configuration for one guild (or anything else that
# only needs one file)
class JsonConfig:
    def __init__(self, path, template=None):
        self.opts = {}
        self.path = path
        self.lock = asyncio.Lock() # Lock for config writing
        self.template = template
        self.init()

    def init(self):
        if self.path.exists():
            self.load()
        else:
            self.create()

    def load(self):
        template = self.template

        with open(self.path, 'r') as f:
            self.opts = dict(json.load(f))

        if template is not None:
            for key, value in self.template.items():
                if key not in self.opts:
                    self.opts[key] = template[key]

    def create(self):
        if self.template is not None:
            self.opts = dict(self.template)

        self.unsafe_write()
   
    def get_lock(self):
        return self.lock

    async def set(self, key, value):
        async with self.lock:
            self.opts[key] = value
            self.unsafe_write()

    async def get(self, key):
        async with self.lock:
            return self.opts[key]

    def unsafe_write(self):
        with open(self.path, 'w') as f:
            json.dump(self.opts, f, indent=4)

    async def write(self):
        async with self.lock:
            self.unsafe_write()


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
            "default_post_age": 7 # days
        }

        self.config_db = JsonConfigDB(Path(config_path), template=self.cfgtemplate)
        self.secret = secret

        self.jobqueue = JobQueue(self.bot.loop)
        # make sure to remove the cfg entry for jobs that are cancelled or
        # stopped
        self.jobqueue.on_job_stop(self._cfg_job_delete)
        self.jobqueue.on_job_cancel(self._cfg_job_delete)
        self.jobtask = None

        self.jobfactory = DiscordJobFactory(self)
        self.jobfactory.register_task_type(MessageTask)
        self.jobfactory.register_task_type(BlockerTask)

        self.bot.event(self.on_guild_join)
        self.bot.event(self.on_ready)

        #self.bot.add_cog(Wiping(self))
        #self.bot.add_cog(Config(self))
        self.bot.add_cog(Debug(self))
        self.bot.add_cog(JobManagement(self))

        self.jobs_resumed = False

    def run(self):
        loop = self.bot.loop

        try:
            # Create job consumer
            self.jobtask = loop.create_task(self.jobqueue.run())

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
            async with cfg.get_lock():
                jobs = dict(cfg.opts["jobs"])
                cfg.opts["jobs"] = {}
                cfg.unsafe_write()

            for job_id, job_header in jobs.items():
                logging.info("resume")
                await self.resume_job(job_header)

            msg = "Resumed {} unfinished job(s) in guild {}"
            logging.info(msg.format(len(jobs), guild_id))

    # Enqueue a job. Users should not call this.
    async def _start_job(self, job, guild):
        cfg = await self.config_db.get_config(guild)

        # Record each started job in the config DB.
        async with cfg.get_lock():
            cfg.opts["jobs"][str(job.header.id)] = job.header.as_dict()
            cfg.unsafe_write()

        await self.jobqueue.submit_job(job)

    # Resume job from a loaded job header dict.
    async def resume_job(self, header):
        guild = self.bot.get_guild(header["guild_id"])

        job = await self.jobfactory.create_job_from_dict(header, guild)
        await self._start_job(job, guild)

    # Enqueue a new job
    async def start_job(self, ctx, task_type, properties):
        job = await self.jobfactory.create_job(ctx, task_type, properties)
        await self._start_job(job, ctx.guild)

    # Once a job is done, delete it from the config db.
    async def _cfg_job_delete(self, header):
        guild = self.bot.get_guild(header.guild_id)
        cfg = await self.config_db.get_config(guild)

        async with cfg.get_lock():
            id = str(header.id)

            # only delete if it's actually there. otherwise ignore
            if id in cfg.opts["jobs"]:
                del cfg.opts["jobs"][id]
                cfg.unsafe_write()

    # Create configs for any guilds we were added to while offline
    async def join_guilds_offline(self):
        async for guild in self.bot.fetch_guilds():
            logging.info("In guilds: {}({})".format(guild.name, guild.id))
            _ = await self.config_db.get_config(guild)

        await self.config_db.write_db()

    async def on_ready(self):
        if not self.jobs_resumed:
            await self.resume_jobs()
            self.jobs_resumed = True

        logging.info("Wiper ready.")

    # Add a new config slice if we join a new guild.
    async def on_guild_join(self, guild):
        logging.info("Joined new guild: {}({})".format(guild.name, guild.id))
        await self.config_db.create_config(guild)


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
    
    def pretty_print_job(self, guild, job):
        h = job.header
        print(h.owner_id)
        owner = self.bot.bot.get_user(h.owner_id).name
        s = "{}: owner={} type={}".format(h.id, owner, h.task_type)
        
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
                await ctx.send("You can only delete jobs you started.")
                return

            await self.jq.canceljob(id)
            await ack(ctx)

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
            await ctx.send("Cannot cancel other users' jobs.")
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


class BlockerTask(JobTask):
    task_type = "blocker"

    def __init__(self, bot, guild):
        self.bot = bot
        self.guild = guild

    async def run(self, header):
        while True:
            await asyncio.sleep(1)

    def display(self, header):
        return ""


class MessageTask(JobTask):
    MAX_MSG_DISPLAY_LEN = 15
    task_type = "message"

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

    def display(self, header):
        p = header.properties
        msg = p["message"]

        if len(msg) > MessageTask.MAX_MSG_DISPLAY_LEN:
            msg = msg[0:MessageTask.MAX_MSG_DISPLAY_LEN] + "..."

        fmt = "message=\"{}\" post_interval={} post_number={}"

        return fmt.format(msg, p["post_interval"], p["post_number"])


class WipeTask(JobTask):
    task_type = "wipe"

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


def main():
    startup_cfg = JsonConfig(STARTUP_CONFIG_LOCATION,
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
