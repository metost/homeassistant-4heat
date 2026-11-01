"""Local 4heat coordination with bounded asynchronous communication."""
from __future__ import annotations
import asyncio
from datetime import timedelta
import logging
from homeassistant.const import CONF_HOST, CONF_MONITORED_CONDITIONS
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util.dt import utcnow
from .const import DOMAIN, TCP_PORT, DATA_QUERY, ERROR_QUERY, CONF_MODE, MODES, MODE_TYPE, ERROR_TYPE
from .transport import FourHeatTransport, ProtocolError, decode_registers

_LOGGER = logging.getLogger(__name__)


class FourHeatDataUpdateCoordinator(DataUpdateCoordinator):
    def __init__(self, hass, *, config, options, id):
        self._host = config[CONF_HOST]
        self._mode = config.get(CONF_MODE, False)
        self.swiches = [MODE_TYPE]  # Preserve existing platform spelling.
        self.stove_id = id
        self.model = "Basic"
        self.serial_number = "1"  # Preserve device registry identity.
        self._on_cmd, self._off_cmd, self._unblock_cmd = MODES[bool(self._mode)]
        if not self._mode:
            self.swiches.append(ERROR_TYPE)
        self.active_interval = options.get("active_interval", 10)
        self.idle_interval = options.get("idle_interval", 60)
        self._failures = 0
        self.last_successful_update = None
        self.last_command_accepted = None
        self.transport = FourHeatTransport(self._host, TCP_PORT,
            timeout=options.get("transaction_timeout", 5),
            settle=options.get("connection_settle", 0.5))
        monitored = {s[1:6] for s in config.get(CONF_MONITORED_CONDITIONS, [])
                     if isinstance(s, str) and len(s) == 18}
        self._required = {MODE_TYPE} | (monitored & {ERROR_TYPE, "30017"})
        super().__init__(hass, _LOGGER, name=DOMAIN,
                         update_interval=timedelta(seconds=self.idle_interval))

    async def _async_update_data(self):
        try:
            response = await self.transport.request(DATA_QUERY)
            if response[0] == "ERR":
                response = await self.transport.request(ERROR_QUERY)
            snapshot = decode_registers(response)
            if not self._required.issubset(snapshot):
                raise ProtocolError("Required stove telemetry is missing")
        except (OSError, asyncio.TimeoutError, ProtocolError) as err:
            self._failures += 1
            self.update_interval = timedelta(seconds=min(
                300, self.idle_interval * 2 ** min(self._failures-1, 4)))
            raise UpdateFailed(f"4heat communication failed: {err}") from err
        self._failures = 0
        self.last_successful_update = utcnow()
        interval = self.idle_interval if snapshot[MODE_TYPE][0] in (0, 8, 9) else self.active_interval
        self.update_interval = timedelta(seconds=interval)
        # A partial SEC fallback may retain noncritical fields; all critical
        # fields above must be fresh. Never mutate published data in place.
        return {**(self.data or {}), **snapshot} if response[0] == "SEC" else snapshot

    async def _command(self, payload):
        if payload is None:
            raise HomeAssistantError("Command unsupported in legacy mode")
        try:
            response = await self.transport.request(payload)
            if response[0] != "SEC":
                raise ProtocolError("Stove rejected the command")
        except (OSError, asyncio.TimeoutError, ProtocolError) as err:
            raise HomeAssistantError(
                f"4heat command failed or unconfirmed: {err}. Check actual stove state before retrying."
            ) from err
        self.last_command_accepted = utcnow()
        # Acceptance is not proof that ignition or shutdown has completed.
        # The existing switch platform requests a separate telemetry refresh.
        return True

    async def async_turn_on(self):
        return await self._command(self._on_cmd)

    async def async_turn_off(self):
        return await self._command(self._off_cmd)

    async def async_unblock(self):
        return await self._command(self._unblock_cmd)

    async def async_set_value(self, id, value):
        if not isinstance(id, str) or len(id) != 5 or not id.isdigit():
            raise HomeAssistantError("Invalid register identifier")
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 10**12:
            raise HomeAssistantError("Register value must be an unsigned 12-digit integer")
        payload = f'["SEC","1","B{id}{value:012d}"]'.encode()
        return await self._command(payload)
