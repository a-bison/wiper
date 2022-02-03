# Wiper
An automated message cleaner bot for Discord written using [discord.py](https://github.com/Rapptz/discord.py).

I'm aware there are a lot of mod bots that do something similar, but I wanted to give it a
try myself, for educational purposes.

# Running

Running an instance of Wiper requires only `discord.py`. To install that, follow the instructions
[here](https://github.com/Rapptz/discord.py/blob/master/README.rst).

On first run, the configuration file `startup.json` will be created, and you will see this message:

```
$ python ./wiper.py
ERROR:root:Please open startup.json and replace the secret with a proper API token.
ERROR:root:For more information, see https://discordpy.readthedocs.io/en/stable/discord.html
```

[Create a bot account](https://discordpy.readthedocs.io/en/stable/discord.html), and copy the bot token
into startup.json:

```json
{
    "secret": "TOKEN GOES HERE",
    "config_dir": "configdb"
}
```

Wiper requires the use of the [GUILD_MEMBERS intent](https://discordpy.readthedocs.io/en/stable/intents.html).

Once your bot user is set up, rerun `wiper.py`, and you're good to go.

# Features

_Note: The default command prefix is `w!`._

The two main commands you'll use most often are `wipenow` and `wipelater`.

## `wipenow`

```
w!wipenow [channels] [older_than] [members]

Start a wipe job immediately.

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
```

## `wipelater`

```
w!wipelater <schedule> [channels] [older_than] [members]

Schedule a wipe job to occur at a regular interval.

...
```

Same arguments as `wipenow`, except for the addition of the `<schedule>` argument. The schedule is a string
that follows a subset of the [cron format](https://crontogo.com/blog/the-complete-guide-to-cron/). Currently
only `*` and integers are supported. Ranges, value lists and step values are not.

The following shortcuts for the day of week field are supported: `sun, mon, tue, wed, thu, fri, sat`.

A few macros are also recognized:

```
!weekly  - * * SUN
!monthly - 1 * *
!yearly  - 1 1 *
!daily   - * * *

Example:

30 4 !weekly - Run at 4:30 AM, every sunday.
```
