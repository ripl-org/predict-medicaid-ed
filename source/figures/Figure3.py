import numpy as np
import os
import pandas as pd
import sys
from riipl import Connection

np.random.seed(int(sys.argv[1]))

population    = sys.argv[2]
subset        = sys.argv[3]
year          = int(sys.argv[4])
ed_claims     = sys.argv[5]
predict_files = sys.argv[6:-1]
out_file      = sys.argv[-1]

sql = """
      SELECT pop.sample_id,
             {agg}(pay_amt) AS pay_amt,
             {agg}(pay_amt * pc_treatable) AS pc_treatable
        FROM {population} pop
  INNER JOIN {ed_claims} ec
          ON pop.riipl_id = ec.riipl_id
       WHERE EXTRACT(YEAR FROM ec.claim_dt) = {y} AND
             pop.subset = '{subset}'
    GROUP BY pop.sample_id
      """

with Connection() as cxn:
    pop    = pd.read_sql("SELECT sample_id FROM {} WHERE subset='{}'".format(population, subset), cxn._connection)
    visits = pd.read_sql(sql.format(agg="COUNT", y=year,   **globals()), cxn._connection)
    cost   = pd.read_sql(sql.format(agg="-SUM",  y=year,   **globals()), cxn._connection)
    save   = pd.read_sql(sql.format(agg="SUM",   y=year+1, **globals()), cxn._connection)

print(cost.sum(axis=0))
print(save.sum(axis=0))

norm1 = 100 / cost.PC_TREATABLE.sum()
norm2 = 100 / save.PC_TREATABLE.sum()

# Outreach sizes
sizes = dict((p, int(p * len(pop) / 100)) for p in [1, 2, 5])
print("sizes:", str(sizes))

predictions = {}

# Load predictions
for predict_file in predict_files:
    model = os.path.basename(predict_file).partition(".")[0]
    predictions[model] = pd.read_csv(predict_file).sort_values("predicted", ascending=False).SAMPLE_ID.values

# Simulate policy based on 2+ or 4+ visits

values = visits[visits["PAY_AMT"] >= 2].SAMPLE_ID.values
np.random.shuffle(values)
predictions["2+ ED Visits"] = values
print("2+ ED Visits:", len(predictions["2+ ED Visits"]))

values = visits[visits["PAY_AMT"] >= 4].SAMPLE_ID.values
np.random.shuffle(values)
predictions["4+ ED Visits"] = values
print("4+ ED Visits:", len(predictions["4+ ED Visits"]))

def capture(df, index):
    return df[df.SAMPLE_ID.isin(index)]["PC_TREATABLE"].sum()

with open(out_file, "w") as f:
    print("Outreach", "Model", "{} Cost".format(year), "{} Savings".format(year+1), sep=",", file=f)
    for p in sorted(sizes):
        for model in predictions:
            print("Outreach to {}%".format(p),
                  model,
                  "{:.1f}%".format(capture(cost, predictions[model][:sizes[p]]) * norm1),
                  "{:.1f}%".format(capture(save, predictions[model][:sizes[p]]) * norm2),
                  sep=",", file=f)

# vim: syntax=python expandtab sw=4 ts=4
