# /home/ele/plc_logger/Modbus-2-db/config.py
DB = "/home/ele/plc_logger/plc.db"

# Modbus / PLC
USE_MODBUS = True
PLC_IP     = "10.0.0.1"
PLC_PORT   = 502
SLAVE_ID   = 1
WORD_ORDER = "LH"   # "HL" = HI word first; "LH" = LO word first

# Timezone for web display. Logger/db always uses UTC
LOCAL_TZ = "America/New_York"   # e.g., "America/New_York", "UTC", etc.

# Database storage/retention options
RETENTION = {
    "max_db_mb": 20000,          # hard cap (MB), DB + WAL/SHM
    "raw_keep_days": 30,         # keep full-fidelity rows this many days
    "delete_batch": 10_000,      # rows per delete batch
    "enforce_every_s": 600,      # how often to run the cleanup
    "incremental_vacuum_pages": 2000,
    "primary_purge_tags": ["SYS_WetWellLevel"], # trim continuous tags first to save space
}


