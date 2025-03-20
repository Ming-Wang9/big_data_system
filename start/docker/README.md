# P1 (4% of grade): Counting Loans with Dockerized Shell Script

## Overview

In this project, you'll setup your programming environment for the
first part of the semester (virtual machine, Docker).  You'll practice
writing some shell commands to download a zipped file and search
(grep) through its contents.  You'll automate these steps with a shell
script.  The shell script may depend on other programs (like `unzip`),
so you'll deploy it as a Docker image with the necessary installs.

Learning objectives:
* deploy a virtual machine in the cloud
* follow a complicated series of steps to install Docker
* write a shell script to automate several bash commands
* bundle a shell script up as a Docker image/container

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

* Jan 30: fix script name (count.sh to multi.sh) in part 4
* Jan 30: update general project specifications to say "Clone with SSH" instead of "Clone with HTTPS"

## Prepare: Virtual Machine Connection

## Part 1: Download Script

Write a `download.sh` bash script that downloads the following:

* https://pages.cs.wisc.edu/~harter/cs544/data/wi2021.csv.gz
* https://pages.cs.wisc.edu/~harter/cs544/data/wi2022.csv.gz
* https://pages.cs.wisc.edu/~harter/cs544/data/wi2023.csv.gz

Your script should then decompress each of these (you can use `man gzip` to
learn more about how to do this).

The `download.sh` file should then create a single `wi.txt` file
containing the contents of all three files consecutively.

Your `download.sh` should have a shebang line so it runs with bash.

## Part 2: Multi Script

Write a `multi.sh` script first uses `download.sh` to generate wi.txt, then
counts the number of lines in wi.txt containing the text "Multifamily"
(any case).  Your script should output/print the correct number; it is
OK it generates additional output.

Your `multi.sh` should have a shebang line so it runs with bash.  Make
it executable.

## Part 3: Docker Install

Carefully follow the directions here to install Docker 27.2.0 and Compose 2.29.2 on your virtual machine: https://docs.docker.com/engine/install/ubuntu/

Notes:
* there are several different approaches described under "Installation methods".  Use the directions under "Install using the apt repository".  Make sure you don't keep going after you reach "Install from a package"
* the first step under "Install Docker Engine" has two options: "Latest" or "Specific version".  Choose **"Specific version"**
* here is the command to get the required versions: `sudo apt-get install docker-ce=5:27.5.1-1~ubuntu.24.04~noble docker-ce-cli=5:27.5.1-1~ubuntu.24.04~noble containerd.io docker-buildx-plugin docker-compose-plugin=2.32.4-1~ubuntu.24.04~noble`

To avoid needing to run every Docker command with root, there are a
few more steps you should do here:
https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user
(don't go beyond the "Manage Docker as a non-root user" section).

Create some more files so we can check your Docker install:

```
docker version > docker.txt
docker compose version > compose.txt
```

## Part 4: Docker Image

Create a `Dockerfile` that starts from a base image of your choosing
and includes your `multi.sh` file.  The Dockerfile should do any
installs needed for your script to run.

You should be able to create an image and container like this:

```
docker build . -t p1
docker run p1
```

It's OK if there is extra output besides the actual count.

## Submission

Read the directions [here](../projects.md) about how to create the
repo.

At a minimum, your submission repo should contain the following:
`docker.txt`, `compose.txt`, `download.sh`, `multi.sh`, and
`Dockerfile`.

## Tester

See the general testing directions [here](../projects.md).
