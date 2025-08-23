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

# file management
RETENTION = {
    "total_cap_mb": 10,     # global hard cap (test)
    "chunk_max_mb": 1,      # rotate chunk around ~1 MB (test)

    # Optional per-family caps:
    # "caps": {"continuous": 7, "conditional": 2, "onchange": 1},

    # Optional family overrides (tag -> family):
    # "family_overrides": {"SomeNoisyConditionalTag": "continuous"}
}


