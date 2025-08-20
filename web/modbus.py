# web/modbus.py
import struct
from .db import fetch_setpoints
from config import WORD_ORDER, PLC_IP, PLC_PORT, USE_MODBUS

try:
    from pymodbus.client import ModbusTcpClient
except Exception:
    ModbusTcpClient = None

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

def words_to_float(hi, lo):
    if WORD_ORDER == "LH":
        hi, lo = lo, hi
    return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

def _setpoint_window(sps):
    """Compute minimal contiguous %MW window covering all setpoints."""
    if not sps:
        return None
    start = min(sp["mw"] for sp in sps)
    # last used word index (inclusive)
    end = max(sp["mw"] + (2 if sp.get("dtype", sp.get("type","FLOAT32")).upper() == "FLOAT32" else 1) - 1
              for sp in sps)
    count = end - start + 1
    return start, count

def read_setpoint_block_dyn(sps=None):
    """Return (values_dict, error_msg). values_dict maps name -> current_value."""
    if sps is None:
        sps = fetch_setpoints()
    if not sps:
        return {}, "No setpoints configured."

    c = mb_client()
    if not c:
        return {}, "Modbus client not available"

    win = _setpoint_window(sps)
    if not win:
        return {}, "No setpoints configured."
    start, count = win

    try:
        rr = c.read_holding_registers(address=start, count=count)
        if hasattr(rr, "isError") and rr.isError():
            return {}, f"Modbus read error: {rr}"
        regs = getattr(rr, "registers", None)
        if not regs:
            return {}, "No data returned."
    except Exception as e:
        return {}, f"Modbus exception: {e}"

    values = {}
    for sp in sps:
        i = sp["mw"] - start
        dtype = sp.get("dtype", sp.get("type", "FLOAT32")).upper()
        try:
            if dtype == "INT16":
                values[sp["name"]] = regs[i]
            else:  # FLOAT32 (2 words)
                values[sp["name"]] = words_to_float(regs[i], regs[i+1])
        except Exception:
            values[sp["name"]] = None

    return values, None
