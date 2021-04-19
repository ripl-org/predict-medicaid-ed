import numpy as np
import pandas as pd
import sys

means_file, emerge_file, spend_file = sys.argv[1:4]
bootstrap_files = sys.argv[4:-1]
out_file = sys.argv[-1]

means  = pd.read_csv(means_file,  usecols=["var", "mean"], index_col="var")

# Add X value for intercept
means.loc["(Intercept)"] = 1


def combine(spend, emerge):

    # Expected value of y evaluated at mean X
    Ey = spend.join(means)
    assert Ey["mean"].notnull().all()
    Ey = (Ey.coef * Ey["mean"]).sum()
    print("Ey:", Ey)

    # Expected value of Pr(y > 0) evaluated at mean X
    Pr = emerge.join(means)
    assert Pr["mean"].notnull().all()
    Pr = np.exp((Pr.coef * Pr["mean"]).sum())
    Pr = Pr / (1 + Pr)
    print("Pr:", Pr)

    # Combine coefficients
    coef = spend.join(emerge, how="outer", lsuffix="1", rsuffix="2").fillna(0)
    coef.loc[:, "coef1"] = coef.coef1 * Pr * Ey
    coef.loc[:, "coef2"] = coef.coef2 * Ey
    coef.loc[:, "coef"] = coef.coef1 + coef.coef2

    return coef


# Point estimate

coef = combine(pd.read_csv(spend_file, usecols=[0, 1], skiprows=1, names=["var", "coef"], index_col="var"),
               pd.read_csv(emerge_file, usecols=["var", "coef"], index_col="var"))

# Calculate CI from bootstraps

n = len(bootstrap_files) // 2
assert 2*n == len(bootstrap_files)

bootstraps = [combine(pd.read_csv(bootstrap_files[i+n], usecols=[0, 1], skiprows=1, names=["var", "coef"], index_col="var"),
                      pd.read_csv(bootstrap_files[i], usecols=["var", "coef"], index_col="var")).coef
              for i in range(n)]

bootstraps = dict((var, sorted(b[var] for b in bootstraps)) for var in coef.index)

coef["ci_lower"] = [bootstraps[var][int(0.025*n)] for var in coef.index]
coef["ci_upper"] = [bootstraps[var][int(0.975*n)] for var in coef.index]

coef[["coef", "ci_lower", "ci_upper"]].to_csv(out_file)
