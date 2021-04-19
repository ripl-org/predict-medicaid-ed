import pandas as pd
import sys

emergence_file, spending_file, out_file = sys.argv[1:]

def auc_cost_capture(predicted, actual):
    df = pd.DataFrame({"predicted": predicted, "actual": actual})
    return (df.sort_values("predicted", ascending=False).actual.cumsum() /
            df.sort_values("actual",    ascending=False).actual.cumsum()).mean()

index = ["SAMPLE_ID"]

emergence = pd.read_csv(emergence_file, index_col=index)
spending  = pd.read_csv(spending_file, index_col=index)

# Use the product of predicted probability of cost emergence and 
# predicted divertible spending conditional on cost emergence
p = pd.DataFrame({"predicted" : emergence.predicted*spending.predicted,
                  "emergence" : emergence.actual,
                  "spending"  : spending.actual},
                 index=emergence.index)

print("auc:", auc_cost_capture(p.predicted, p.spending))

p.to_csv(out_file)

# vim: syntax=python expandtab sw=4 ts=4
