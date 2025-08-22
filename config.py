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

