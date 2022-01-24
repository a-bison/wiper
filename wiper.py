import discord
from discord.ext import commands

import asyncio
import calendar
import collections
import copy
import functools
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


class AsyncAtomicCounter:
    def __init__(self, start_count=0):
        self.count = start_count
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

        self.job_submit_callback = None
        self.job_start_callback = None
        self.job_stop_callback = None
        self.job_cancel_callback = None

    async def submit_job(self, job):
        if self.job_submit_callback is not None:
            await self.job_submit_callback(job.header)

        await self.job_queue.put(job)
        self.jobs[job.header.id] = job

    def on_job_submit(self, callback):
        self.job_submit_callback = callback

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

    async def create_jobheader_from_cron(self, cron):
        return cron.as_jobheader(
            await self.id_counter.get_and_increment(),
            int(time.time())
        )

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

    # Create a job using just the header.
    def _create_job_from_jobheader(self, header):
        guild = self.bot.get_guild(header.guild_id)
        print(header.guild_id)
        task_cls = self.task_types[header.task_type]
        task = task_cls(self.bot, guild)
        j = Job(header, task)

        return j

    # Create a job from an existing dictionary (typically loaded from cfg)
    async def create_job_from_dict(self, header):
        header = await self.create_jobheader_from_dict(header)
        return self._create_job_from_jobheader(header)

    # Create a job from a schedule entry
    async def create_job_from_cron(self, cron):
        header = await self.create_jobheader_from_cron(cron)
        return self._create_job_from_jobheader(header)


class DiscordCronFactory:
    def __init__(self, bot, start_id=0):
        self.id_counter = AsyncAtomicCounter(start_id)
        self.bot = bot
        self.task_types = {}

    def register_task_type(self, cls):
        if not hasattr(cls, "task_type"):
            raise Exception("Task class must have task_type attr")

        self.task_types[cls.task_type] = cls

    async def create_cronheader(self, ctx, properties, task_type, cron_str):
        header = CronHeader(
            await self.id_counter.get_and_increment(),
            task_type,
            properties,
            ctx.message.author.id,
            ctx.guild.id,
            cron_str
        )

        return header

    async def create_cronheader_from_dict(self, header_dict):
        return CronHeader.from_dict(header_dict)


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
        # If this job was started by a schedule, this will reflect the owner of
        # the schedule.
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


class CronHeader:
    @classmethod
    def from_dict(self, d):
        return CronHeader(**d)

    def __init__(self, id, task_type, properties, owner_id, guild_id, schedule):
        # ID of this schedule. NOTE: Unlike Jobs, whose ID count resets after
        # every startup, schedules always have the same IDs.
        self.id = id

        # Arguments to the jobs created by this schedule.
        self.properties = properties

        # The task type string.
        self.task_type = task_type

        # Member ID of the discord user that owns this schedule.
        self.owner_id = owner_id

        # Guild ID of the guild this schedule was created in.
        self.guild_id = guild_id

        # Schedule string for this schedule. Determines the time that this
        # job will run at. The header stores the schedule string in the exact
        # same format as the unix cron utility.
        #
        # min hour day_of_month month day_of_week
        # note: day_of_week runs from 0-6, Sunday-Saturday.
        #
        # supported operators:
        # * - Signifies all possible values in a field.
        # 
        # example:
        # 1 4 * * 0 - run the job at 4:01 am every Sunday.
        self.schedule = schedule

        # RUNTIME VALUES
        # These values are generated at runtime and are never saved.

        # A python datetime object representing the next time this schedule
        # will run. Used by a schedule dispatcher to avoid missing a job fire.
        self.next = None

    def as_dict(self):
        return {
            "id": self.id,
            "properties": self.properties,
            "task_type": self.task_type,
            "owner_id": self.owner_id,
            "guild_id": self.guild_id,
            "schedule": self.schedule
        }

    def as_jobheader(self, id, start_time):
        return JobHeader(
            id,
            self.task_type,
            self.properties,
            self.owner_id,
            self.guild_id,
            start_time,
            self.id
        )

    # Updates self.next to the next run after current datetime.
    def update_next(self):
        sched_obj = schedule_parse(self.schedule)

        # Make sure to avoid multiple schedule firings, so make carry=1.
        # See cron_next_date() for more details.
        self.next = cron_next_date_as_datetime(sched_obj, carry=1)
            

class ScheduleParseException(Exception): 
    pass


SCHED_PARSE_POSITIONS = [
    "minute",
    "hour",
    "dayofmonth",
    "month",
    "dayofweek"
]
SCHED_PARSE_LIMITS = [
    (0, 59),
    (0, 23),
    (1, 31),
    (1, 12),
    (0, 6)
]
SCHED_LIMITS = {k: v for k, v in zip(SCHED_PARSE_POSITIONS, SCHED_PARSE_LIMITS)}
SCHED_WD_NAMES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6
}

SCHED_MACROS = {
    "!weekly": "* * SUN",
    "!monthly": "1 * *",
    "!yearly": "1 1 *",
    "!daily": "* * *"
}

# Parse a schedule string into a dictionary.
@functools.cache
def schedule_parse(schedule_str):
    schedule_str = schedule_str.lower()
    
    # Parse macros first
    for macro, repl in SCHED_MACROS.items():
        schedule_str = schedule_str.replace(macro, repl)

    s_split = schedule_str.lower().split()
    s_dict = {}

    if len(s_split) < 5:
        raise ScheduleParseException("less than 5 elements")
    elif len(s_split) > 5:
        raise ScheduleParseException("more than 5 elements")

    for limit, name, elem, i in zip(SCHED_PARSE_LIMITS, SCHED_PARSE_POSITIONS, s_split, range(5)):
        lower, upper = limit

        if elem == "*":
            s_dict[name] = None
            continue
       
        try:
            result = int(elem)
        except ValueError:
            if name == "dayofweek" and elem.lower() in SCHED_WD_NAMES:
                result = SCHED_WD_NAMES[elem.lower()]
            else:
                msg = "position {}({}): {} is not an integer"
                raise ScheduleParseException(msg.format(i, name, elem))


        s_dict[name] = result

    return s_dict


def wd_cron_to_python(wd):
    return (wd + 6) % 7


def wd_python_to_cron(wd):
    return (wd + 1) % 7


# Test whether a schedule should run, based on a timedate object.
def schedule_match(schedule_str, timedate_obj):
    sd = schedule_parse(schedule_str)

    for name, elem in sd.items():
        # Skip *'s
        if elem is None:
            continue

        # If we encounter a field that doesn't match, stop and return False.
        # Therefore, if all specified fields match, we will return True.
        if ((name == "minute"     and elem != timedate_obj.minute) or
            (name == "hour"       and elem != timedate_obj.hour) or
            (name == "dayofmonth" and elem != timedate_obj.day) or
            (name == "month"      and elem != timedate_obj.month) or
            (name == "dayofweek"  and elem != wd_python_to_cron(timedate_obj.weekday()))):
            return False

    return True


# Calculate the upper and lower bounds for a given element.
def limit_elem(elem_name, t):
    if elem_name == "dayofmonth":
        upper = calendar.monthrange(t["year"], t["month"])[1]
        return 1, upper

    elif elem_name == "year":
        return datetime.MIN_YEAR, datetime.MAX_YEAR

    else:
        return SCHED_LIMITS[elem_name]


# Calculate the next element
def next_elem(elem_name, elem, carry, t, schedule):
    sched_elem = schedule[elem_name]
    lower, upper = limit_elem(elem_name, t)

    new_elem = elem + carry
    new_carry = 0

    logging.info("next_elem(): {}: {}({}) -> {}".format(
        elem_name,
        elem,
        new_elem,
        str(sched_elem)
    ))

    # If our sched element can be anything, don't touch it. (Note that
    # the carry has already been taken into account. We just need to check
    # whether the carry made this element roll over.
    if sched_elem is None:
        if new_elem > upper:
            new_elem = lower
            new_carry = 1

        return new_elem, new_carry

    # Otherwise, select the next available schedule slot for this element.
    # If no slot could be selected, select the first one, and carry.
    no_elem_found = False
    if isinstance(sched_elem, int):
        sched_elem = [sched_elem]
    sched_elem = sorted(sched_elem)
    for i in sched_elem:
        if i < new_elem:
            continue
        else:
            new_elem = i
            break
    else:
        no_elem_found = True

    # If we couldn't find the next element, or the new element that WAS
    # selected goes over the given limit, roll back around and carry.
    if new_elem > upper or no_elem_found:
        new_elem = sched_elem[0]
        new_carry = 1

    return new_elem, new_carry


# Calculate all of a given weekday in a given month
@functools.cache
def cron_calc_days(year, month, wd):
    # Calendar starts at 0 = Monday, goes to 6 = Sunday. Cron format is offset
    # from that so we need to convert to python range.
    wd = wd_cron_to_python(wd)
    c = calendar.Calendar() 
    return [d for d, _wd in c.itermonthdays2(year, month)
            if d != 0 and _wd == wd]


# From a cron structure parsed from schedule_parse, determine what the next
# date will be the scheduled job will run, based on current date. Returns a dict:
#
# {
#   "minute": ...,
#   "hour":   ...,
#   "day":    ...,
#   "month":  ...,
#   "year":   ...
# }
#
# If carry is supplied, one minute will be added to the from_date. This can
# help avoid multiple schedule firings if the from_date already matches the
# schedule.
def cron_next_date(schedule, from_date=None, carry=0):
    # Make a copy so we can freely modify.
    schedule = dict(schedule)

    if from_date is not None:
        current_date = from_date
    else:
        current_date = datetime.now()

    next_date = {
        "minute": current_date.minute,
        "hour": current_date.hour,
        "dayofmonth": current_date.day,
        "month": current_date.month,
        "year": current_date.year
    }

    if isinstance(schedule["dayofmonth"], int):
        schedule["dayofmonth"] = [schedule["dayofmonth"]]

    # Copy original in case we need to recalculate dayofmonth for month/year
    # changes.
    if schedule["dayofmonth"] is not None:
        schedule["_orig_dayofmonth"] = list(schedule["dayofmonth"])
    else:
        schedule["_orig_dayofmonth"] = []
    
    next_date["minute"], carry = next_elem("minute", next_date["minute"],
                                           carry, next_date, schedule)
    next_date["hour"], carry = next_elem("hour", next_date["hour"],
                                         carry, next_date, schedule)

    # Day of month is tricky. If there was a carry, that means we flipped
    # to the next month, and potentially the next year. If that's the case,
    # then we need to do a second round.
    new_day, carry = _cron_next_day(schedule, carry,
        day=next_date["dayofmonth"],
        month=next_date["month"],
        year=next_date["year"]
    )

    # Only do another round if dayofweek is present.
    if carry > 0 and schedule["dayofweek"] is not None:
        # We flipped month, so do another round, starting at the first day
        # of the month. la = lookahead
        month_la = next_date["month"]
        year_la = next_date["year"]

        logging.info("cron_next_date(): month overrun, recalc day")

        if month_la == 12:
            month_la = 1
            year_la += 1
            logging.info("cron_next_date(): year overrun")
        else:
            month_la += 1

        new_day, extra_carry = _cron_next_day(schedule, 0,
            day=1,
            month=month_la,
            year=year_la
        )

        if extra_carry > 0:
            raise Exception("Could not recalculate dayofmonth")

    next_date["dayofmonth"] = new_day
    next_date["month"], carry = next_elem("month", next_date["month"],
                                          carry, next_date, schedule)

    # Don't need full elem calculation for year, so just bump it if there
    # was a carry.
    next_date["year"] += carry

    return next_date


def _cron_next_day(schedule, carry, day, month, year):
    # If dayofweek is present, fold it into dayofmonth to make things
    # easier to calculate.
    if schedule["dayofweek"] is not None:
        weekdays = cron_calc_days(
            year,
            month,
            schedule["dayofweek"]
        )

        schedule["dayofmonth"] = sorted(set(schedule["_orig_dayofmonth"] + weekdays))

    newday, carry = next_elem(
        "dayofmonth",
        day,
        carry,
        {"year": year, "month": month},
        schedule
    )

    return newday, carry


# Convert the output of cron_next_date to a datetime object.
def cron_next_to_datetime(cron_next):
    return datetime(
        cron_next["year"],
        cron_next["month"],
        cron_next["dayofmonth"],
        cron_next["hour"],
        cron_next["minute"]
    )


# Get next date as datetime.
def cron_next_date_as_datetime(schedule, from_date=None, carry=0):
    return cron_next_to_datetime(cron_next_date(schedule, from_date, carry))


# A scheduler that starts jobs at specific real-world dates.
# Expected to have minute-level accuracy.
class JobCron:
    def __init__(self, jobqueue, jobfactory):
        self.jobqueue = jobqueue
        self.jobfactory = jobfactory

        self.schedule_lock = asyncio.Lock()
        self.schedule = {}

        self.sched_create_callback = None
        self.sched_delete_callback = None

    def on_create_schedule(self, callback):
        self.sched_create_callback = callback

    def on_delete_schedule(self, callback):
        self.sched_delete_callback = callback

    # Stop a schedule from running.
    async def delete_schedule(self, id):
        async with self.schedule_lock:
            sheader = self.schedule[id]
            sheader.next = None

            if self.sched_destroy_callback is not None:
                await self.sched_destroy_callback(sheader)
        
            del self.schedule[id]

    # Schedule a job.
    async def create_schedule(self, sheader):
        async with self.schedule_lock:
            # Calculate the next run date right away. This also
            # functions to validate the cron str before scheduling.
            sheader.update_next()

            logging.info("New schedule created: " + str(sheader.as_dict()))
            if self.sched_create_callback is not None:
                await self.sched_create_callback(sheader)

            self.schedule[sheader.id] = sheader

    async def run(self):
        # The background task that starts jobs. Checks if there are new jobs
        # to start roughly once every minute.
        logging.info("Starting job scheduler...")

        try:
            while True:
                await asyncio.sleep(60)

                # Do not allow modifications to the schedule while a schedule
                # check is running.
                async with self.schedule_lock:
                    await self.mainloop()
        except:
            logging.exception("Scheduler stopped unexpectedly!")

    # Single iteration of schedule dispatch.
    async def mainloop(self):
        for id, sheader in self.schedule.items():
            # If we've gone past the scheduled time, fire the job,
            # regenerate the next time using the cron string.
            if sheader.next and sheader.next < datetime.now():
                sheader.update_next()
                await self._start_scheduled_job(sheader)

    async def _start_scheduled_job(self, cron_header):
        job = await self.jobfactory.create_job_from_cron(cron_header)
        msg = "SCHED {}: Firing job type={} {}"
        logging.info(msg.format(
            cron_header.id, job.header.task_type, job.task.display(job.header)
        ))
        await self.jobqueue.submit_job(job)


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
    def __init__(self, path, template=None, main_template=None):
        self.path = path
        self.create_lock = asyncio.Lock()
        self.db = {}
        self.template = template
        self.main_template = main_template
        self.main_cfg = None

        if path.is_dir():
            self.load_db()
        elif path.exists():
            msg = "config {} is not a directory"
            raise FileExistsError(msg.format(str(path)))
        else: # No file or dir, so create new
            self.create_new_db()

        self.load_main_cfg()
        
    # Creates a new config DB
    def create_new_db(self):
        try:
            self.path.mkdir()
        except FileNotFoundError:
            logging.error("Parent directories of config not found.")
            raise

    def cfg_loc(self, guild_id):
        return self.path / (str(guild_id) + ".json")

    def common_cfg_loc(self):
        return self.path / "common.json"

    def load_main_cfg(self):
        loc = self.common_cfg_loc()
        self.main_cfg = JsonConfig(loc, self.main_template)

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

    # Gets state and config common to all guilds.
    def get_common_config(self):
        return self.main_cfg

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

    # Get an entry, perform f(value) on it, and set it again.
    async def get_and_set(self, key, f):
        async with self.lock:
            self.opts[key] = f(self.opts[key])
            self.unsafe_write()

    # Set an entry in a sub-config.
    async def sub_set(self, d, key, value):
        async with self.lock:
            if d not in self.opts:
                self.opts[d] = {}

            self.opts[d][key] = value
            self.unsafe_write()

    # Delete an entry in a sub-config.
    async def sub_delete(self, d, key):
        async with self.lock:
            if d not in self.opts:
                return

            if key not in self.opts[d]:
                return

            del self.opts[d][key]
            self.unsafe_write()

    # Get a complete copy of a subconfig, then clear it.
    async def sub_get_all_and_clear(self, d):
        async with self.lock:
            if d not in self.opts:
                return {}

            subconf = dict(self.opts[d])
            self.opts[d] = {}
            self.unsafe_write()

        return subconf

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
            "cron": {},
            "default_post_age": 7 # days
        }

        self.common_cfgtemplate = {
            # save the last schedule id so we don't overlap new schedules
            # with old ones
            "last_schedule_id": 0
        }

        self.config_db = JsonConfigDB(
            Path(config_path),
            template=self.cfgtemplate,
            main_template=self.common_cfgtemplate
        )
        self.secret = secret

        # Job executer/consumer component
        self.jobqueue = JobQueue(self.bot.loop)
        # Events for linking to the config system.
        self.jobqueue.on_job_submit(self._cfg_job_create)
        self.jobqueue.on_job_stop(self._cfg_job_delete)
        self.jobqueue.on_job_cancel(self._cfg_job_delete)
        self.jobfactory = DiscordJobFactory(self)
        self.jobfactory.register_task_type(MessageTask)
        self.jobfactory.register_task_type(BlockerTask)
        self.jobtask = None

        # Job scheduler component
        self.jobcron = JobCron(self.jobqueue, self.jobfactory)
        self.jobcron.on_create_schedule(self._cfg_sched_create)
        self.jobcron.on_delete_schedule(self._cfg_sched_delete)
        self.cronfactory = DiscordCronFactory(
            self,
            self.config_db.get_common_config().opts["last_schedule_id"] + 1
        )
        self.cronfactory.register_task_type(MessageTask)
        self.cronfactory.register_task_type(BlockerTask)
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
        if task_type not in self.bot.cronfactory.task_types:
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

            await self.jc.schedules.delete_schedule(id)
            await ack(ctx)
        else:
            await ctx.send("Schedule {} does not exist.".format(id))


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
            s_dict = schedule_parse(cron)
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
            if schedule_match(cron, datetime.fromisoformat(date_time)):
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

            s = schedule_parse(cron)
        except ScheduleParseException as e:
            await ctx.send("Could not parse cron str: " + str(e))
            return
        except ValueError as e:
            await ctx.send(str(e))
            return

        next_date_time = cron_next_date_as_datetime(s, date_time)

        message  = "```\nFrom {}\n"
        message += "{} will next run\n"
        message += "     {}\n```"

        await ctx.send(message.format(
            date_time.strftime("%c"),
            cron,
            next_date_time.strftime("%c")
        ))


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
