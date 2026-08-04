FROM rocker/r-ver:4.5.2
# Set the working directory for the project
RUN apt-get update && apt-get install -y libcurl4-openssl-dev libssl-dev libxml2-dev libproj-dev cmake gdal-bin libabsl-dev libgdal-dev libglpk-dev libpng-dev libx11-dev cloud-utils cloud-guest-utils pandoc && apt-get install -y python3.6 python3-pip python3-setuptools python3-dev python3.12-venv curl libudunits2-dev && rm -rf /var/lib/apt/lists/*

# Register TBB library
RUN ldconfig

ENV http_version=2
ENV CURL_HTTP_VERSION=CURL_HTTP_VERSION_1_1
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV RENV_VERSION=f2c9163
ENV RENV_WATCHDOG_ENABLED=FALSE

# Install renv
RUN Rscript -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN Rscript -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"

WORKDIR /landlord_mapper_etl

#copy renv.lock over to dir
COPY renv.lock renv.lock
COPY renv/activate.R renv/activate.R
COPY renv/settings.json renv/settings.json

#COPY _targets/ _targets/
# Restore R project library - force source package builds to ensure TBB compatibility
RUN R -s -e "renv::restore()"
COPY requirements.txt .
RUN pip install -r requirements.txt

# Add local files and folders needed to generate the report
COPY *.xlsx          .
COPY *.R            .
COPY *.py            .
COPY AUSTIN*.zip            .
COPY *.json            .
COPY *.txt            .
COPY link_used.csv            .

#EXPOSE 8080
CMD ["R", "-e", "targets::tar_make(callr_function = NULL, use_crew = FALSE, as_job = FALSE)"]

#
