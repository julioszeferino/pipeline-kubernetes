FROM jupyter/datascience-notebook:latest
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt && rm requirements.txt 
ENV JUPYTER_ENABLE_LAB=yes
ENTRYPOINT jupyter-lab --NotebookApp.token=''