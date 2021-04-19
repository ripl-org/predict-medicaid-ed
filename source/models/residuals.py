import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import sys

sns.set()

in_file, out_file = sys.argv[1:]

df = pd.read_csv(in_file)
plt.figure(figsize=(6, 6))
sns.jointplot(df.residual, df.fitted, kind="hex")
plt.tight_layout()
plt.savefig(out_file)

# vim: expandtab sw=4 ts=4
