# The name of Project

Movix notification scheduler

## What is this?

This is a small daemon that runs in the background and sends notifications to the users based on`NotificationCron` table.

It stores jobs inside crontab and runs them in background.

## How to use it?

1. Clone main repo of [movix](https://github.com/stranded-in-python/movix)

2. Clone all services by running `clone_and_fetch`

3. Run `make notification` to startup the service.

4. Log in into admin and create entry for `NotificationCron`

## Authors

-   [Stranded in Python](https://github.com/stranded-in-python)
