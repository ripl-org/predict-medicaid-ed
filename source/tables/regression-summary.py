import pandas as pd
import os
import sys

coef_file, manifest_file, out_file = sys.argv[1:]

coef = pd.read_csv(coef_file)
manifest = pd.read_csv(manifest_file, sep="\t", names=["var", "desc"])

table = coef.merge(manifest, how="left", on="var").sort_values("coef", ascending=False)

with open(out_file, "w") as f:
    print(r"\begin{longtable}{lcc}", file=f)
    print(r"\em Variable & \em Coefficient & \em 95\% C.I. \\[0.5em]", file=f)
    for row in table.itertuples():
        if row.var.endswith("MISSING"):
            desc = "{} is missing".format(row.var.partition("_")[0].title())
        elif row.var == "(Intercept)":
            desc = "(intercept)"
        else:
            desc = row.desc
        desc = str(desc).replace("&", r"\&").replace("_", " ")
        print(r"{} & {:.2f} & ({:.2f}, {:.2f}) \\".format(
                  desc, row.coef, row.ci_lower, row.ci_upper),
              file=f)
    print(r"\end{longtable}", file=f)

# vim: syntax=python expandtab sw=4 ts=4
