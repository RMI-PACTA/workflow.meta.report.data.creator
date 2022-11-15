# Meta Report Data Creator

## Instructions

In each of the R files (`phase-1`, `pahse-2`, and `phase-3`), update the lines defining:

* `data_path <- <path to unzipped directory from constructiva>`
* `output_dir <- <path to where meta results will live>`

Then run 
```bash
Rscript phase-1_combine-portfolios.R
```

If you encounter errors, you will need to modify the portfolio `csv` files to correct the errors.


Then run 
```bash
Rscript phase-2_run-pacta.R
```

which takes a long time.

```bash
Rscript phase-3_combine-results.R
```

combines the results into peer files (meta + org in one set, user_id in the other)
