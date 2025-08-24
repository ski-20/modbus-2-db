# web/modbus.py  (patch)

import logging, struct
from typing import Tuple, Dict, Any, List
from pymodbus.client import ModbusTcpClient

log = logging.getLogger("modbus")

# from config.py
try:
    from config import PLC_IP, PLC_PORT, SLAVE_ID, USE_MODBUS
except Exception:
    PLC_IP, PLC_PORT, SLAVE_ID, USE_MODBUS = "127.0.0.1", 502, 1, False

_client = None

def mb_client() -> ModbusTcpClient | None:
    global _client
    if not USE_MODBUS:
        return None
    if _client is None:
        _client = ModbusTcpClient(host=PLC_IP, port=PLC_PORT, timeout=2)
        _client.connect()
    return _client

def float_to_words(val: float) -> Tuple[int, int]:
    raw = struct.pack(">f", float(val))
    hi, lo = struct.unpack(">HH", raw)
    return hi, lo

# ----- compatibility shims for pymodbus 2.x (unit=) vs 3.x (slave=) -----

def _call_read_holding(c: ModbusTcpClient, **kwargs):
    # Try new API (slave=) first
    try:
        return c.read_holding_registers(**kwargs, slave=SLAVE_ID)
    except TypeError as e:
        # Fallback to old API (unit=)
        if "unexpected keyword argument 'slave'" in str(e):
            return c.read_holding_registers(**kwargs, unit=SLAVE_ID)
        raise

def _call_write_register(c: ModbusTcpClient, **kwargs):
    try:
        return c.write_register(**kwargs, slave=SLAVE_ID)
    except TypeError as e:
        if "unexpected keyword argument 'slave'" in str(e):
            return c.write_register(**kwargs, unit=SLAVE_ID)
        raise

def _call_write_registers(c: ModbusTcpClient, **kwargs):
    try:
        return c.write_registers(**kwargs, slave=SLAVE_ID)
    except TypeError as e:
        if "unexpected keyword argument 'slave'" in str(e):
            return c.write_registers(**kwargs, unit=SLAVE_ID)
        raise

# ----- setpoint helpers used by the UI -----

def read_setpoint_block_dyn(sps: List[Dict[str, Any]]) -> tuple[Dict[str, float], str]:
    """
    Read all configured setpoints in one windowed sweep when possible.
    Returns (values_by_name, error_message_if_any)
    """
    c = mb_client()
    if not c:
        return {}, "Modbus not enabled on server"

    # group contiguous ranges (simple approach: one by one, robust & clear)
    vals, first_err = {}, ""
    for sp in sps:
        try:
            dtype = (sp.get("dtype") or sp.get("type") or "FLOAT32").upper()
            mw = int(sp["mw"])
            if dtype == "INT16":
                rr = _call_read_holding(c, address=mw, count=1)
                regs = getattr(rr, "registers", None)
                if rr is None or (hasattr(rr, "isError") and rr.isError()) or not regs:
                    raise RuntimeError(rr)
                v = regs[0]
                # interpret as signed 16
                if v >= 32768: v -= 65536
                vals[sp["name"]] = float(v)
            else:
                rr = _call_read_holding(c, address=mw, count=2)
                regs = getattr(rr, "registers", None)
                if rr is None or (hasattr(rr, "isError") and rr.isError()) or not regs or len(regs) < 2:
                    raise RuntimeError(rr)
                hi, lo = regs[0], regs[1]
                if dtype == "FLOAT32":
                    vals[sp["name"]] = struct.unpack(">f", struct.pack(">HH", hi, lo))[0]
                elif dtype == "INT32":
                    v = (hi << 16) | lo
                    if v & 0x80000000: v -= (1<<32)
                    vals[sp["name"]] = float(v)
                else:  # UINT32 or default
                    vals[sp["name"]] = float((hi << 16) | lo)
        except Exception as e:
            msg = f"Read exception @%MW{sp.get('mw')}: {e}"
            log.warning(msg)
            if not first_err:
                first_err = msg

    return vals, first_err

def write_setpoint(name: str, sp: Dict[str, Any], fval: float) -> tuple[bool, str]:
    """Write a single setpoint; returns (ok, message)."""
    c = mb_client()
    if not c:
        return False, "Modbus not enabled on server"

    try:
        mw = int(sp["mw"])
        dtype = (sp.get("dtype") or sp.get("type") or "FLOAT32").upper()
        if dtype == "INT16":
            r = _call_write_register(c, address=mw, value=int(fval))
        else:
            hi, lo = float_to_words(fval)
            r = _call_write_registers(c, address=mw, values=[hi, lo])
        ok = not (hasattr(r, "isError") and r.isError())
        return ok, ("OK" if ok else "Write failed")
    except Exception as e:
        return False, f"Write exception: {e}"
