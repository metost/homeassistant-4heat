"""Read-only diagnostics verified against product SYEVO0000513, version 1."""
from .transport import ProtocolError, REGISTER, validate_response

CONF_SUPPLEMENTAL = "supplemental_telemetry"
CONF_SUPPLEMENTAL_INTERVAL = "supplemental_interval"
# I requests read values; never construct a B/J write from these definitions.
SUPPLEMENTAL_QUERY = (
    b'["SEC","3","I30011000000000000","I50010000000000000",'
    b'"I30025000000000000"]'
)
SUPPLEMENTAL_IDS = {"30011", "50010", "30025"}
POWER_LABELS = {
    0: "P Stb", 1: "P 1", 2: "P 2", 3: "P 3", 4: "P 4", 5: "P 5",
    6: "P 6", 7: "P Cln", 8: "P Ign2", 9: "P Mod", 10: "P 0",
    11: "P MAX", 100: "P OFF", 105: "P Ext", 106: "P Stby",
    110: "P OFF", 120: "P MAX", 255: "P Ign",
}


def decode_supplemental(message):
    """Decode this profile separately; keep legacy integer interpretation intact.

    Type 5 follows the app's base /100 decoder. No additional map multiplier or
    physical unit is inferred for the auger reading.
    """
    validate_response(message)
    if message[0] != "SEC":
        raise ProtocolError("Supplemental read rejected")
    readings = {}
    for raw in message[2:]:
        if not REGISTER.fullmatch(raw) or raw[0] != "J":
            raise ProtocolError("Unexpected supplemental register")
        key = raw[1:6]
        if key not in SUPPLEMENTAL_IDS or key in readings:
            raise ProtocolError("Unexpected or duplicate supplemental reading")
        if raw[1] == "3":
            value = int(raw[13:]) * (-1 if raw[12] != "0" else 1)
        elif raw[1] == "5":
            value = int(raw[11:]) / 100 * (-1 if raw[10] != "0" else 1)
        else:
            raise ProtocolError("Unsupported supplemental encoding")
        readings[key] = {"value": value, "raw": raw}
    if readings.keys() != SUPPLEMENTAL_IDS:
        raise ProtocolError("Missing supplemental reading")
    return readings
