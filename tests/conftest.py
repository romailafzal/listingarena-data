import os

# no real 120 s global pauses in tests (sources/http.py); test_global_pause sets its own
os.environ.setdefault("SOURCE_PAUSE_S", "0")
