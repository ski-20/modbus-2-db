# tags.py
# Central definition of all tags and their logging modes

FAST_SEC   = 1.0   # fast cadence for conditional tags
SAMPLE_SEC = 0.5   # poll interval for Modbus reads

# --- System tags ---
SYSTEM_TAGS = [
    {"name": "SYS_WetWellLevel", "label": "Wet Well Level", "mw": 440,
     "dtype": "FLOAT32", "scale": 1.0, "unit": "level",
     "mode": "interval", "interval_sec": 10},

    {"name": "SYS_OutDataWord", "label": "System Output Data Word", "mw": 442,
     "dtype": "INT16", "unit": "",
     "mode": "interval", "interval_sec": 10},
]

# --- Pump tags generator (13 tags each) ---
def pump_tags(base: int, pump_key: str, pump_label: str):
    return [
        {"name":f"{pump_key}_DrvStatusWord", "label":f"{pump_label} Drive Status Word",
         "mw":base+0,  "dtype":"UINT16", "unit":"", "mode":"on_change"},

        {"name":f"{pump_key}_SpeedRaw", "label":f"{pump_label} Speed (raw)",
         "mw":base+1,  "dtype":"INT16", "unit":"raw",
         "mode":"conditional", "condition":{"tag":f"{pump_key}_Status","op":"==","value":1}},

        {"name":f"{pump_key}_MotorCurrent", "label":f"{pump_label} Motor Current",
         "mw":base+2,  "dtype":"INT16", "scale":0.1, "unit":"A",
         "mode":"conditional", "condition":{"tag":f"{pump_key}_Status","op":"==","value":1}},

        {"name":f"{pump_key}_DCBusV", "label":f"{pump_label} DC Bus Voltage",
         "mw":base+3,  "dtype":"INT16", "unit":"V",
         "mode":"interval", "interval_sec":30},

        {"name":f"{pump_key}_OutV", "label":f"{pump_label} Output Voltage",
         "mw":base+4,  "dtype":"INT16", "unit":"V",
         "mode":"interval", "interval_sec":30},

        {"name":f"{pump_key}_TorqueRaw", "label":f"{pump_label} Torque (raw)",
         "mw":base+5,  "dtype":"INT16", "unit":"raw",
         "mode":"conditional", "condition":{"tag":f"{pump_key}_Status","op":"==","value":1}},

        {"name":f"{pump_key}_FaultActive", "label":f"{pump_label} Active Fault",
         "mw":base+6,  "dtype":"UINT16", "unit":"", "mode":"on_change"},

        {"name":f"{pump_key}_FaultPrev", "label":f"{pump_label} Previous Fault",
         "mw":base+7,  "dtype":"UINT16", "unit":"", "mode":"on_change"},

        {"name":f"{pump_key}_Starts", "label":f"{pump_label} Total Starts",
         "mw":base+8,  "dtype":"INT32", "unit":"", "mode":"on_change"},

        {"name":f"{pump_key}_Hours_x10", "label":f"{pump_label} Total Hours (x10)",
         "mw":base+10, "dtype":"INT32", "unit":"tenth_hr",
         "mode":"interval", "interval_sec":60},

        {"name":f"{pump_key}_Status", "label":f"{pump_label} Status (1=Running)",
         "mw":base+12, "dtype":"UINT16", "unit":"", "mode":"on_change"},

        {"name":f"{pump_key}_Mode", "label":f"{pump_label} Mode",
         "mw":base+13, "dtype":"UINT16", "unit":"", "mode":"on_change"},

        {"name":f"{pump_key}_OutDataWord", "label":f"{pump_label} Output Data Word",
         "mw":base+14, "dtype":"UINT16", "unit":"",
         "mode":"interval", "interval_sec":30},
    ]

# --- Full tag list ---
TAGS = (
    SYSTEM_TAGS
    + pump_tags(400, "P1", "Pump 1")
    + pump_tags(420, "P2", "Pump 2")
)


SETPOINTS = [
    {"name":"WetWell_Stop_Level",        "label":"Wet Well Stop Level",                 "mw":300, "dtype":"FLOAT32", "unit": "In."},
    {"name":"WetWell_Lead_Start_Level",  "label":"Wet Well Lead Pump Start Level",      "mw":302, "dtype":"FLOAT32"},
    {"name":"WetWell_Lag_Start_Level",   "label":"Wet Well Lag Pump Start Level",       "mw":304, "dtype":"FLOAT32"},
    {"name":"WetWell_High_Level",        "label":"Wet Well High Level",                 "mw":306, "dtype":"FLOAT32"},
    {"name":"WetWell_Level_Scale_0V",    "label":"Wet Well Level Scaling - 0V",         "mw":308, "dtype":"FLOAT32"},
    {"name":"WetWell_Level_Scale_10V",   "label":"Wet Well Level Scaling - 10V",        "mw":310, "dtype":"FLOAT32"},
    {"name":"Spare_Analog_IO_1",         "label":"Spare Analog IO 1",                   "mw":312, "dtype":"FLOAT32"},
    {"name":"Spare_Analog_IO_2",         "label":"Spare Analog IO 2",                   "mw":314, "dtype":"FLOAT32"},
    {"name":"Pump1_Speed_Setpoint_pct",  "label":"Pump 1 Speed Setpoint (%)",           "mw":316, "dtype":"FLOAT32"},
    {"name":"Pump2_Speed_Setpoint_pct",  "label":"Pump 2 Speed Setpoint (%)",           "mw":318, "dtype":"FLOAT32"},
    {"name":"Pump1_FailToRun_Delay_sec", "label":"Pump 1 Fail To Run Delay (sec.)",     "mw":320, "dtype":"INT16"},
    {"name":"Pump2_FailToRun_Delay_sec", "label":"Pump 2 Fail To Run Delay (sec.)",     "mw":321, "dtype":"INT16"},
    {"name":"Spare_Analog_IO_HighLevel", "label":"Spare Analog IO High Level",          "mw":322, "dtype":"FLOAT32"},
]
