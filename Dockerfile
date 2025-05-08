# Use a Miniconda base image
FROM continuumio/miniconda3:latest

# Install libxcb-cursor0 for Qt XCB plugin
USER root
RUN apt-get update && apt-get install -y libxcb-cursor0 libxcb-render0 libxcb-image0 libxcb-keysyms1 libxcb-render-util0 libxcb-xinerama0 libxcb-icccm4 libxcb-randr0 libxcb-shape0 libxcb-xfixes0 libxcb-xkb1 && rm -rf /var/lib/apt/lists/*
USER $NB_USER

# Set the working directory in the container
WORKDIR /app

# Copy the environment.yml file into the container
COPY environment.yml .

# Create the conda environment from the environment.yml file
# and activate it for subsequent RUN, CMD, and ENTRYPOINT instructions
RUN conda env create -f environment.yml && conda clean -afy

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "server-client-env", "/bin/bash", "-c"]

# Demonstrate the environment is activated:
RUN echo "Running in $(conda info --base)/envs/server-client-env"
RUN python --version
RUN conda list

# Copy the rest of the application code into the container
COPY . .

# Copy the entrypoint script and make it executable
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Specify the command to run on container start
CMD ["./entrypoint.sh"] 