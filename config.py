# /home/ele/plc_logger/Modbus-2-db/config.py
DB_ROOT = "/home/ele/plc_logger/data"

# Modbus / PLC
USE_MODBUS = True
PLC_IP     = "10.0.0.1"
PLC_PORT   = 502
SLAVE_ID   = 1
WORD_ORDER = "LH"   # "HL" = HI word first; "LH" = LO word first

# Timezone for web display. Logger/db always uses UTC
LOCAL_TZ = "America/New_York"   # e.g., "America/New_York", "UTC", etc.
WEEK_START = 0 # 0=mon, 6=sun

# file management (raw-only)
RETENTION = {
    "total_cap_mb": 10000,   # 10 GB hard cap
    "chunk_max_mb": 64,      # ~64 MB per chunk is a nice balance

    # Split the space across families (optional but recommended)
    "caps": {
        "conditional": 5000,  # pumps’ bursty data
        "continuous":  4500,  # Wet Well Level / Analogs
        "onchange":     500,  # faults/edges/etc.
    },

    # Optional overrides if a tag should live in a different family:
    # "family_overrides": {"SomeNoisyConditionalTag": "continuous"},
}
