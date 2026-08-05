# landlord-mapper
repo for organizing landlord mapper project. 

WIP Front-end visualizing collated data is here: https://ontheseams.shinyapps.io/landlord_mapper_app/

Request access to the following GDrive for further project materials: https://drive.google.com/drive/folders/1e2Ahq9sNNQ2K_Q-RuTrdkWzDL6FH_gAa?usp=sharing

## Layout

| Path | What it is |
| --- | --- |
| `_targets.R`, `*_helper_functions.R` | The R pipeline: reads the county appraisal rolls, scrapes the Texas franchise-tax registry, and groups parcels into owner portfolios. |
| `shinyApp/app.R` | The Shiny front end, deployed at the shinyapps.io link above. |
| `web/` | A second front end: a stdlib-only Python server over a read-only SQLite build of the pipeline's output, deployed on Railway. Added because it holds the whole dataset on a small box and answers a page in well under a second; see `web/README.md`. It does not replace `shinyApp/`, and neither one reads the other's code. |
| `TCAD_parse.py` | Standalone Travis appraisal-roll parser. |
| `renv.lock`, `Dockerfile` | Reproducibility for the R pipeline. |
