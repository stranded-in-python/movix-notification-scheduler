from watchfiles import run_process

from scheduler import schedule

if __name__ == '__main__':
    run_process('/app', target=schedule, debug=True)
