python3 - <<'PY'
from pymodbus.client import ModbusTcpClient
import logging
# Verbose protocol logs:
logging.basicConfig(level=logging.INFO)
logging.getLogger("pymodbus").setLevel(logging.DEBUG)

IP = "10.0.0.1"
PORT = 502
TEST_START = 0     # %MW0
TEST_COUNT = 10

for UNIT in (1, 255):
    print("\n--- Trying Unit ID:", UNIT, "---")
    c = ModbusTcpClient(IP, port=PORT, timeout=3)
    print("connect():", c.connect())
    r = c.read_holding_registers(TEST_START, TEST_COUNT, slave=UNIT)
    if hasattr(r, "isError") and r.isError():
        print("READ ERROR:", r)
        # Show details if available
        try:
            print("function_code:", r.function_code, "exception_code:", r.exception_code)
        except Exception:
            pass
    else:
        print("OK registers:", getattr(r, "registers", None))
    c.close()
PY
