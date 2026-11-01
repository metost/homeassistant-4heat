"""Additional diagnostic entities; existing names and identities are untouched."""
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from .supplemental import POWER_LABELS

DESCRIPTIONS = {
    "selected_power": "Selected pellet power",
    "30011": "Actual combustion power",
    "50010": "Auger reading",
    "30025": "Combustion fan reading",
}


class FourHeatDiagnosticSensor(CoordinatorEntity, SensorEntity):
    """Model-specific observations without control actions or inferred units."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, coordinator, entry, key):
        super().__init__(coordinator)
        self.key = key
        self._attr_name = f"{entry.title} Diagnostics {DESCRIPTIONS[key]}"
        self._attr_unique_id = f"{entry.entry_id}_diagnostic_{key}"
        self._attr_device_info = {
            "identifiers": {("4heat", coordinator.serial_number)},
        }

    @property
    def available(self):
        if not self.coordinator.last_update_success:
            return False
        if self.key == "selected_power":
            return {"20813", "20211"}.issubset(self.coordinator.fresh_registers)
        return self.key in self.coordinator.supplemental_data

    @property
    def native_value(self):
        if not self.available:
            return None
        if self.key == "selected_power":
            selected = self.coordinator.data["20813"][0]
            maximum = self.coordinator.data["20211"][0]
            return "Auto" if selected == maximum + 1 else f"P {selected}"
        value = self.coordinator.supplemental_data[self.key]["value"]
        return POWER_LABELS.get(value, f"Unknown ({value})") if self.key == "30011" else value

    @property
    def extra_state_attributes(self):
        if not self.available:
            return None
        attrs = {"profile": "SYEVO0000513/1"}
        if self.key == "selected_power":
            attrs.update(register="20813", raw_value=self.coordinator.data["20813"][0],
                         maximum_manual_power=self.coordinator.data["20211"][0])
        else:
            reading = self.coordinator.supplemental_data[self.key]
            attrs.update(register=self.key, raw_register=reading["raw"],
                         decoded_value=reading["value"],
                         last_successful_read=self.coordinator.supplemental_updated_at.isoformat())
            if self.key == "50010":
                attrs["interpretation"] = "App base decoder /100; physical unit unverified"
        return attrs
