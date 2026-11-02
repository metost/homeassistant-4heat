""" Integration for 4heat"""
import voluptuous as vol
import logging

from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.core import HomeAssistant, valid_entity_id
from homeassistant.const import (
    CONF_HOST,
    CONF_NAME,
    CONF_MONITORED_CONDITIONS,
)
import homeassistant.helpers.config_validation as cv

from homeassistant.exceptions import ConfigEntryNotReady


from .const import ATTR_MARKER, ATTR_READING_ID, ATTR_STOVE_ID, DOMAIN, DATA_COORDINATOR, CONF_MODE
from .coordinator import FourHeatDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_NAME): cv.string,
                vol.Required(CONF_HOST): cv.string,
                vol.Optional(CONF_MODE, default=False): cv.boolean,
                vol.Optional(CONF_MONITORED_CONDITIONS): cv.ensure_list,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass, config):
    """Platform setup, do nothing."""
    hass.data.setdefault(DOMAIN, {})

    if DOMAIN not in config:
        return True

    hass.async_create_task(
        hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_IMPORT}, data=dict(config[DOMAIN])
        )
    )
    return True



async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Load the saved entities."""
    coordinator = FourHeatDataUpdateCoordinator(
        hass,
        config=entry.data,
        options=entry.options,
        id=entry.entry_id,
    )

    await coordinator.async_config_entry_first_refresh()

    hass.data[DOMAIN][entry.entry_id] = {
        DATA_COORDINATOR: coordinator,
    }


    if not hass.services.has_service(DOMAIN, "set_value"):
        async def async_handle_set_value(call):
            from homeassistant.exceptions import HomeAssistantError
            entity_id = call.data.get("entity_id", "")
            value = call.data.get("value")
            if not valid_entity_id(entity_id):
                raise HomeAssistantError("A valid entity_id is required")
            entity = hass.states.get(entity_id)
            if entity is None or entity.attributes.get(ATTR_MARKER) != "B":
                raise HomeAssistantError("Entity is missing or register is not writable")
            if isinstance(value, str):
                try:
                    float(value)
                except ValueError:
                    if valid_entity_id(value):
                        source = hass.states.get(value)
                        value = source.state if source else None
            try:
                number = float(value)
                if isinstance(value, bool) or not number.is_integer():
                    raise ValueError
                number = int(number)
            except (ValueError, TypeError, OverflowError):
                raise HomeAssistantError("A finite integer value is required") from None
            stove_id = entity.attributes.get(ATTR_STOVE_ID)
            if stove_id not in hass.data[DOMAIN]:
                raise HomeAssistantError("Unknown stove")
            coord = hass.data[DOMAIN][stove_id][DATA_COORDINATOR]
            await coord.async_set_value(entity.attributes.get(ATTR_READING_ID), number)
            await coord.async_request_refresh()
        hass.services.async_register(DOMAIN, "set_value", async_handle_set_value)

    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor", "switch"])
    return True


async def async_reload_entry(hass, entry):
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass, entry):
    if await hass.config_entries.async_unload_platforms(entry, ["sensor", "switch"]):
        await hass.data[DOMAIN][entry.entry_id][DATA_COORDINATOR].async_shutdown()
        hass.data[DOMAIN].pop(entry.entry_id)
        if not hass.data[DOMAIN]:
            hass.services.async_remove(DOMAIN, "set_value")
        return True
    return False
