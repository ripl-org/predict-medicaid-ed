Analysis code to reproduce results from the manuscript:

Hastings JS, Howison M. 2021. Predicting Divertible Medicaid Emergency Department Costs.
*OSF Preprints* [q36es](https://osf.io/q36es/). doi:[10.31219/osf.io/q36es](https://doi.org/10.31219/osf.io/q36es)

## Supplementary tables

1. [Descriptions for all 1,963 features](features.csv)
2. [Coefficients for the basic regression model](coef.basic.csv)
3. [Coefficients for the post-LASSO regression model](coef.postlasso.csv)

## Repo layout

This project utilizes the [predictive modeling template](https://github.com/ripl-org/predictive-template) from [Research Improving People's Lives](https://www.ripl.org). The **SConstuct** file defines dependencies between analysis steps and automates the analysis run from start to finish, using the extensible [SCons](http://scons.org/) software construction tool.

The code is organized as follows:

| Subdirectory | Description |
| --- | --- |
| **input** | Raw data which will not be stored in the repo. This is typically a symlink to a read-only directly containing archival flat files. |
| **output** | Output files that are used to construct the results reported in the manuscript. |
| **scratch** | Staging area for large intermediate and output files. These files will be cached by SCons, so they often do not need to be recomputed again after they have been created the first time. SCons caches the output base on the full checksums of all dependencies. |
| **source/inputs** | Source files for staging, transforming, indexing flat files from **inputs**. |
| **source/populations** | Source files for defining populations of interest for the model. |
| **source/outcomes** | Source files for building the model's outcomes (typically depends on populations). |
| **source/features** | Source files for building model's features (typically depends on populations). |
| **source/models** | Source files for concatenating features into design matrices, then running and tuning models (typically depends on outcomes and features). |
| **source/figures** | Source files for generating figures (typically depends on models). |
| **source/tables** | Source files for generating tables (typically depends on models). |

## License

Copyright 2021, Innovative Policy Lab (d/b/a Research Improving People's Lives), Providence, RI. All Rights Reserved.

See [LICENSE](LICENSE) for details.

Pharmacy claims are grouped into prescription drug categories using: AHFS® Pharmacologic/Therapeutic Classification© used with permission. © 2019, the American Society of Health-System Pharmacists, Inc. (ASHP). The Data is a part of the AHFS Drug Information®; ASHP is not responsible for the accuracy of transpositions from the original context.