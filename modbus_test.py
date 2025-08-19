from pymodbus.client import ModbusTcpClient

# 🔧 Change these as needed
IP = "10.0.0.1"   # your device IP
PORT = 502        # Modbus TCP default port
UNIT = 255        # Unit ID (sometimes 1, sometimes 255)

def main():
    client = ModbusTcpClient(IP, port=PORT, timeout=3)
    connected = client.connect()
    print("connect():", connected)

    if not connected:
        print("❌ Could not connect to device")
        return

    # Try reading 10 registers starting at 0
    result = client.read_holding_registers(0, 10, slave=UNIT)
    if result.isError():
        print("❌ READ ERROR:", result)
    else:
        print("✅ Registers:", result.registers)

    client.close()

if __name__ == "__main__":
    main()
