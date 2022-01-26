import discord
from discord.ext import commands

import json
import logging
import typing

from . import job
from . import config
from . import util
from .exception import NotAdministrator
from .util import ack

# High level interface to the bot core.
# Automatically links together cfg and job systems, and subscribes to
# discord events.
class CoreWrapper:
    def __init__(self, bot, config_path, cfgtemplate, common_cfgtemplate):
        self.cfgtemplate = dict(cfgtemplate)
        self.cfgtemplate.update({
            "jobs": {},
            "cron": {}
        })

        self.bot = bot

        self.common_cfgtemplate = dict(common_cfgtemplate)
        self.common_cfgtemplate.update({
            # save the last schedule id so we don't overlap new schedules
            # with old ones
            "last_schedule_id": 0
        })

        self.config_db = config.JsonConfigDB(
            config_path,
            template=self.cfgtemplate,
            main_template=self.common_cfgtemplate
        )

        # Task registry
        self.task_registry = job.TaskRegistry()

        # Job executer/consumer component
        self.jobqueue = job.JobQueue(self.bot.loop)
        self.jobqueue.on_job_submit(self._cfg_job_create)
        self.jobqueue.on_job_stop(self._cfg_job_delete)
        self.jobqueue.on_job_cancel(self._cfg_job_delete)
        self.jobfactory = job.DiscordJobFactory(self.task_registry, self.bot)
        self.jobtask = None

        # Job scheduler component
        self.jobcron = job.JobCron(self.jobqueue, self.jobfactory)
        self.jobcron.on_create_schedule(self._cfg_sched_create)
        self.jobcron.on_delete_schedule(self._cfg_sched_delete)
        self.cronfactory = job.DiscordCronFactory(
            self.config_db.get_common_config().opts["last_schedule_id"] + 1
        )
        self.crontask = None

        # Register discord events
        # TODO On guild leave
        self.bot.add_listener(self.on_guild_join, 'on_guild_join')
        self.bot.add_listener(self.on_ready, 'on_ready')

        self.jobs_resumed = False

        # Create job consumer and scheduler
        loop = self.bot.loop
        self.jobtask = loop.create_task(self.jobqueue.run())
        self.crontask = loop.create_task(self.jobcron.run())

    ##################
    # EVENT HANDLERS #
    ##################

    # Resume all jobs that never properly finished from the last run.
    # Called from on_ready() to ensure that all discord state is init'd
    # properly
    async def resume_jobs(self):
        for guild_id, cfg in self.config_db.db.items():
            jobs = await cfg.sub("jobs").aget_and_clear()

            for job_id, job_header in jobs.items():
                await self.resume_job(job_header)

            msg = "Resumed {} unfinished job(s) in guild {}"
            logging.info(msg.format(len(jobs), guild_id))

    # Resume job from a loaded job header dict.
    async def resume_job(self, header):
        job = await self.jobfactory.create_job_from_dict(header)
        await self.jobqueue.submit_job(job)

    # Reschedule all cron entries from cfg
    async def reschedule_all_cron(self):
        for guild_id, cfg in self.config_db.db.items():
            crons = await cfg.sub("cron").aget_and_clear()

            for sched_id, sched_header in crons.items():
                await self.reschedule_cron(sched_header)

            msg = "Loaded {} schedule(s) in guild {}"
            logging.info(msg.format(len(crons), guild_id))

    # Get the config object for a given job/cron header.
    async def get_cfg_for_header(self, header):
        guild = self.bot.get_guild(header.guild_id)
        cfg = await self.config_db.get_config(guild)

        return cfg

    async def reschedule_cron(self, header_dict):
        header = await self.cronfactory.create_cronheader_from_dict(header_dict)
        await self.jobcron.create_schedule(header)

    # When a job is submitted, create an entry in the config DB.
    async def _cfg_job_create(self, header):
        cfg = await self.get_cfg_for_header(header)
        await cfg.sub("jobs").aset(str(header.id), header.as_dict())

    # Once a job is done, delete it from the config db.
    async def _cfg_job_delete(self, header):
        cfg = await self.get_cfg_for_header(header)
        await cfg.sub("jobs").adelete(str(header.id), ignore_keyerror=True)

    # Add created schedules to the config DB, and increase the
    # last_schedule_id parameter.
    async def _cfg_sched_create(self, header):
        cfg = await self.get_cfg_for_header(header)
        await cfg.sub("cron").aset(str(header.id), header.as_dict())

        common_cfg = self.config_db.get_common_config()
        await common_cfg.aget_and_set(
            "last_schedule_id",
            lambda val: max(val, header.id)
        )

    # Remove deleted schedules from the config DB.
    async def _cfg_sched_delete(self, header):
        cfg = await self.get_cfg_for_header(header)
        await cfg.sub("cron").adelete(str(header.id))

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

        logging.info("Core ready.")

    # Add a new config slice if we join a new guild.
    async def on_guild_join(self, guild):
        logging.info("Joined new guild: {}({})".format(guild.name, guild.id))
        await self.config_db.create_config(guild)

    #########################
    # UTILITY / PASSTHROUGH #
    #########################

    def run(self, secret):
        loop = self.bot.loop

        try:
            # Perform initialization and log in
            loop.run_until_complete(self.bot.login(secret))
            loop.run_until_complete(self.join_guilds_offline())
            loop.run_until_complete(self.bot.connect())

        except KeyboardInterrupt:
            print("Keyboard Interrupt!")
        finally:
            loop.close()

    # Enqueue a new job. Returns the created job object.
    async def start_job(self, ctx, task_type, properties):
        job = await self.jobfactory.create_job(ctx, task_type, properties)
        await self.jobqueue.submit_job(job)

        return job

    # Schedule a job
    async def schedule_job(self, ctx, task_type, properties, cron_str):
        chdr = await self.cronfactory.create_cronheader(
            ctx,
            properties,
            task_type,
            cron_str
        )
        await self.jobcron.create_schedule(chdr)

        return chdr.id

    # Register a task class.
    def task(self, tsk):
        return self.task_registry.register(tsk)


# Optional cog containing job management commands for testing/administration
# NOTE: This cog requires the Members intent.
class JobManagement(commands.Cog):
    def __init__(self, core):
        self.core = core

        self.bot = core.bot
        self.jq = core.jobqueue
        self.jc = core.jobcron
        self.registry = core.task_registry

    def pretty_print_job(self, guild, job):
        h = job.header
        owner = self.bot.get_user(h.owner_id).name
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
        corrected_user = await util.process_user_optional(ctx, user, rest)

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
    @util.check_administrator()
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
        owner = self.bot.get_user(cron.owner_id).name

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
        if task_type not in self.registry:
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
            await self.core.schedule_job(ctx, task_type, params_dict, cronstr)
            await ack(ctx)

        except job.ScheduleParseException as e:
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

    @commands.command()
    @commands.is_owner()
    async def cronflush(self, ctx):
        """Delete all schedules, and reset ID counter to 0. Bot owner only."""
        for id, cron in self.jc.schedule.items():
            await self.jc.delete_schedule(id)

        await self.core.config_db.get_common_config().aset("last_schedule_id", 0)
        await ack(ctx)


# Optional cog for debugging core.job library functions
class JobDebug(commands.Cog):
    @commands.command()
    async def testcronparse(self, ctx, cron: str):
        """Test parsing of cron strings."""
        try:
            s_dict = core.job.cron_parse(cron)
        except job.ScheduleParseException as e:
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

        except job.ScheduleParseException as e:
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
        except job.ScheduleParseException as e:
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
