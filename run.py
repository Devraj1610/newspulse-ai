"""Start the whole project:  python run.py            (news service + gateway + dashboard + Cosmitude bot)
                              python run.py --no-bot   (without the Cosmitude bot)"""
import subprocess, sys, time

parts = [["news_backend.py"], ["gateway.py"]]
if "--no-bot" not in sys.argv:
    parts.append(["bot.py"])

procs = []
try:
    for p in parts:
        procs.append(subprocess.Popen([sys.executable] + p))
        time.sleep(1)
    print("\nDashboard: http://localhost:8080   (Ctrl+C to stop everything)\n")
    while True:
        time.sleep(1)
        for p in procs:
            if p.poll() is not None:
                print("A part exited; stopping all.")
                raise KeyboardInterrupt
except KeyboardInterrupt:
    pass
finally:
    for p in procs:
        if p.poll() is None:
            p.terminate()
