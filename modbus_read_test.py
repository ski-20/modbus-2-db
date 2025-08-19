from pymodbus.client import ModbusTcpClient

IP    = "10.0.0.1"
PORT  = 502
UNIT  = 1          # <-- set to the UNIT
START = 400        # logger reads
COUNT = 50

c = ModbusTcpClient(IP, port=PORT, timeout=3)
print("connect():", c.connect())

r = c.read_holding_registers(address=START, count=COUNT, slave=UNIT)
if r.isError():
    print("❌ READ ERROR:", r)   # if you see IllegalAddress, device isn’t exposing this window
else:
    print("✅ OK, len(regs) =", len(r.registers))
    print("first 10 regs:", r.registers[:10])
c.close()