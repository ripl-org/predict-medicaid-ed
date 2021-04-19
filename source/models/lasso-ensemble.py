import pandas as pd
import sys

predict_files = sys.argv[1:-1]
out_file      = sys.argv[-1]

index = ["SAMPLE_ID"]

# Load and average predictions
predict = pd.read_csv(predict_files[0], index_col=index)
for predict_file in predict_files[1:]:
    predict["predicted"] += pd.read_csv(predict_file, index_col=index).predicted
predict["predicted"] /= len(predict_files)
predict.to_csv(out_file)

# vim: expandtab sw=4 ts=4
