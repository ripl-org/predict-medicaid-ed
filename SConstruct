import os
from CONSTANTS import CONSTANTS

try:
    # Removes symlink only, not original directory
    os.remove("input")
except OSError:
    pass

exec(compile(open("./source/lib/SCons/setup.py").read(), "./source/lib/SCons/setup.py", "exec"))

os.symlink("/data/{0}/{0}v11".format(env.PROJECT_KEY), "input")

env.CacheDir("/data/{0}/{0}-cache".format(env.PROJECT_KEY))
env.Decider("MD5-timestamp")

# Export constant values and a master list of tables.
constants = dict((k, Value(v)) for k, v in CONSTANTS.items())
Export("constants")

# Inputs
env.SConscript("source/inputs/Make")

# Populations
env.SConscript("source/populations/Make")

# Outcomes
env.SConscript("source/outcomes/Make")

# Features
env.SConscript("source/features/Make")

# Models
env.SConscript("source/models/Make")

# Figures
env.SConscript("source/figures/Make")

# Tables
env.SConscript("source/tables/Make")

# Appendix
env.SConscript("source/appendix/Make")

# vim: syntax=python expandtab sw=4 ts=4
