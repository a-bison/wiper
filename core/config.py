#
# Config classes for dynamic, persistent configuration. For use with asyncio.
# 

from pathlib import Path
import asyncio
import logging
import json

# Very simple config database consisting of json files on disk.
# Saves a different version of the config depending on the guild.
#
# On disk structure:
# config_root_dir \_ common.json
#                 |_ <guild_id_1>.json
#                 |_ <guild_id_2>.json
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


