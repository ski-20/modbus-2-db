# modbus helpers used by the setpoints page

import struct

try:
    from config import USE_MODBUS, PLC_IP, PLC_PORT, SLAVE_ID, WORD_ORDER
except Exception:
    USE_MODBUS = True
    PLC_IP, PLC_PORT, SLAVE_ID = "10.0.0.1", 502, 1
    WORD_ORDER = "HL"

try:
    from pymodbus.client import ModbusTcpClient
except Exception:
    ModbusTcpClient = None
    USE_MODBUS = False

_mb = None

def mb_client():
    if not USE_MODBUS or ModbusTcpClient is None:
        return None
    global _mb
    if _mb is None:
        _mb = ModbusTcpClient(host=PLC_IP, port=PLC_PORT, timeout=2)
    if not getattr(_mb, "connected", False):
        try: _mb.connect()
        except Exception: pass
    return _mb

def float_to_words(val: float):
    hi, lo = struct.unpack(">HH", struct.pack(">f", float(val)))
    return (lo, hi) if WORD_ORDER == "LH" else (hi, lo)

def words_to_float(hi: int, lo: int) -> float:
    if WORD_ORDER == "LH":
        hi, lo = lo, hi
    return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

def read_setpoint_block_dyn(sps: list[dict]):
    """
    Single Modbus block read spanning all setpoints.
    Returns (values_by_name, error_message_or_None)
    """
    if not sps:
        return {}, "No setpoints defined (tag_meta empty)."
    c = mb_client()
    if not c:
        return {}, "Modbus client not available"

    def width(sp): return 2 if sp["dtype"].upper() == "FLOAT32" else 1
    start = min(sp["mw"] for sp in sps)
    end   = max(sp["mw"] + width(sp) - 1 for sp in sps)
    count = end - start + 1

    try:
        rr = c.read_holding_registers(address=start, count=count, slave=SLAVE_ID)
        if hasattr(rr, "isError") and rr.isError():
            return {}, f"Modbus read error: {rr}"
        regs = rr.registers
    except Exception as e:
        return {}, f"Modbus exception: {e}"

    out = {}
    for sp in sps:
        i = sp["mw"] - start
        try:
            if sp["dtype"].upper() == "INT16":
                out[sp["name"]] = regs[i]
            else:
                out[sp["name"]] = words_to_float(regs[i], regs[i+1])
        except Exception:
            out[sp["name"]] = None
    return out, None
