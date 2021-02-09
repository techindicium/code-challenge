## The following instructions were made using a Linux distro command line.
You may have to adapt some commands if your're not.
--- 

## - Considering that you have pre-installed these packages into your local machine:
- Docker;
- Git;
- Python3;
- Python3-pip;

---

# Proceed with these instructions:

## - First of all, use git to clone the remote project into your local machine
### git clone https://github.com/RianBrug/data-pipeline-python-docker-psql.git

## - Now navigate to code-challenge folder. that's our project-root directory
### cd data-pipeline-python-docker-psql/

## - To build up our PostgreSQL database, run:
### docker-compose up -d [using detached mode to keep using same terminal tab]

## - You need to install some libs into your local machine to use python3+psql,
## into the project root directory, run:
### pip3 install -r app/requirements.txt

## - Now that you have the libs installed, and your env params setup, run:
### python3 app/app.py

---

## - If some problem ocurred during docker usage, you may use this following cmds

#### cleanup guide - use with caution, if you have other docker images running, you may have to use 'container image' to prevent stop/removing docker images from another project.
-

### docker stop `docker ps -qa`
### docker rm `docker ps -qa`
### docker rmi -f `docker images -qa `
### docker volume rm $(docker volume ls -qf dangling=true)
