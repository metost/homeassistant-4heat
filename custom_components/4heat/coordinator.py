"""Local 4heat coordination with bounded asynchronous communication."""
from __future__ import annotations
import asyncio
from datetime import timedelta
import json
import logging
from homeassistant.const import CONF_HOST, CONF_MONITORED_CONDITIONS
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util.dt import utcnow
from .const import DOMAIN, TCP_PORT, DATA_QUERY, ERROR_QUERY, CONF_MODE, MODES, MODE_TYPE, ERROR_TYPE
from .transport import FourHeatTransport, ProtocolError, decode_registers, validate_response
from .supplemental import (
    CONF_SUPPLEMENTAL, CONF_SUPPLEMENTAL_INTERVAL, SUPPLEMENTAL_QUERY,
    decode_supplemental,
)

_LOGGER = logging.getLogger(__name__)
CRITICAL_READS = tuple(json.loads(ERROR_QUERY)[2:])
CRITICAL_IDS = {item[1:6] for item in CRITICAL_READS}
ON_STATES = {1, 2, 3, 4, 5, 6, 10, 13, 30, 31, 32, 33, 34}


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
        self.active_backoff_max = max(self.active_interval, options.get("active_backoff_max", 60))
        self.direct_enabled = options.get("direct_telemetry", False)
        self.direct_interval = options.get("direct_interval", 10)
        self.confirmation_timeout = options.get("command_confirmation_timeout", 600)
        self._failures = 0
        self._last_state = None
        self.last_successful_update = None
        self.last_command_accepted = None
        self.last_command = None
        self._pending = None
        self._command_lock = asyncio.Lock()
        self.core_source = "module_cache"
        self.last_direct_update = None
        self.direct_read_status = "not_requested"
        self._direct_attempt = float("-inf")
        self._direct_snapshot = {}
        self.supplemental_enabled = options.get(CONF_SUPPLEMENTAL, False)
        self.supplemental_interval = options.get(CONF_SUPPLEMENTAL_INTERVAL, 30)
        self.supplemental_data = {}
        self.supplemental_updated_at = None
        self._supplemental_attempt = float("-inf")
        self._supplemental_task = None
        self._cache_task = None
        self._cache_attempt = float("-inf")
        self.cache_updated_at = None
        self._shutting_down = False
        self.fresh_registers = set()
        self.transport = FourHeatTransport(self._host, TCP_PORT,
            timeout=options.get("transaction_timeout", 5),
            settle=options.get("connection_settle", 0.5),
            transaction_gap=options.get("transaction_gap", 1))
        monitored = {s[1:6] for s in config.get(CONF_MONITORED_CONDITIONS, [])
                     if isinstance(s, str) and len(s) == 18}
        self._required = {MODE_TYPE} | (monitored & {ERROR_TYPE, "30017"})
        super().__init__(hass, _LOGGER, name=DOMAIN,
                         update_interval=timedelta(seconds=self.idle_interval))

    @property
    def communication_attributes(self):
        """Timestamps describe completed transactions, not probe sample times."""
        return {
            "last_successful_communication": (self.last_successful_update.isoformat()
                                               if self.last_successful_update else None),
            "poll_interval_seconds": self.update_interval.total_seconds(),
            "critical_data_source": self.core_source,
            "direct_read_status": self.direct_read_status,
            "last_direct_read": (self.last_direct_update.isoformat()
                                 if self.last_direct_update else None),
            "last_command": dict(self.last_command) if self.last_command else None,
            "last_cached_read": self.cache_updated_at.isoformat() if self.cache_updated_at else None,
            "read_retries_total": self.transport.read_retries,
            "last_transport_failure": self.transport.last_failure,
            "last_transport_failure_at": (self.transport.last_failure_at.isoformat()
                                          if self.transport.last_failure_at else None),
            "consecutive_poll_failures": self._failures,
        }

    async def _read_critical(self):
        pending = self._pending
        if pending and not pending["ready"]:
            pending = None
        commands = list(CRITICAL_READS)
        if pending and pending["action"] == "set_value":
            key = pending["register"]
            if key not in CRITICAL_IDS:
                commands.append(f"A{key}000000000000")
        readings = await self.transport.read_registers(commands)
        return readings, pending

    def _record_direct(self, readings):
        self._direct_snapshot = readings
        self.last_direct_update = utcnow()
        self.direct_read_status = "ok"
        self._direct_attempt = asyncio.get_running_loop().time()

    def _expire_pending(self):
        if self._pending and asyncio.get_running_loop().time() >= self._pending["deadline"]:
            self._pending["record"]["status"] = "unconfirmed"
            self._pending = None

    def _confirm_pending(self, readings, observed_command):
        """Only a new direct read may confirm a requested observable result."""
        self._expire_pending()
        pending = self._pending
        if not pending or pending is not observed_command:
            return
        state = readings[MODE_TYPE][0]
        action = pending["action"]
        confirmed = (
            (action == "turn_on" and state in ON_STATES) or
            (action == "turn_off" and state == 0) or
            (action == "unblock" and state != 9 and readings[ERROR_TYPE][0] == 0) or
            (action == "set_value" and pending["register"] in readings and
             readings[pending["register"]][0] == pending["value"])
        )
        if confirmed:
            pending["record"].update(status="confirmed", confirmed_at=utcnow().isoformat())
            self._pending = None

    def _validate_primary(self, response):
        # ERR is a normal request for the existing explicit fallback. Malformed
        # status snapshots, including missing critical values, may be read again.
        if response[0] != "ERR":
            snapshot = decode_registers(response)
            if not self._required.issubset(snapshot):
                raise ProtocolError("Required stove telemetry is missing")
        return response

    async def _async_update_data(self):
        self._expire_pending()
        direct = None
        observed_command = None
        try:
            if self.direct_enabled or self._pending:
                # The critical board read is the primary transaction. A cached
                # SEL failure must not block it or replace its state with old data.
                direct, observed_command = await self._read_critical()
                snapshot = dict(direct)
                partial = True
            else:
                response = await self.transport.request_read(DATA_QUERY, decoder=self._validate_primary)
                partial = response[0] in ("ERR", "SEC")
                if response[0] == "ERR":
                    direct, observed_command = await self._read_critical()
                    snapshot = dict(direct)
                else:
                    snapshot = decode_registers(response)
                    self.cache_updated_at = utcnow()
            if not self._required.issubset(snapshot):
                raise ProtocolError("Required stove telemetry is missing")
        except (OSError, asyncio.TimeoutError, ProtocolError) as err:
            self._failures += 1
            if self.direct_enabled or self._pending:
                self.direct_read_status = "failed"
            active = self._pending is not None or self._last_state not in (0, 8, 9)
            # Retry acquisition promptly even when the last observed state was
            # OFF; a failed connection should not create a full idle-period gap.
            base = self.active_interval
            cap = self.active_backoff_max if active else 300
            self.update_interval = timedelta(seconds=min(cap, base * 2 ** min(self._failures-1, 8)))
            raise UpdateFailed(f"4heat communication failed: {str(err) or type(err).__name__}") from err

        if direct is not None:
            self._record_direct(direct)
            self._confirm_pending(direct, observed_command)
            self.core_source = "direct_read"
        else:
            self.core_source = "module_cache"
        self._failures = 0
        self.last_successful_update = utcnow()
        self._last_state = snapshot[MODE_TYPE][0]
        idle = self._last_state in (0, 8, 9) and self._pending is None
        interval = self.idle_interval if idle else self.active_interval
        if self.direct_enabled:
            interval = max(interval, self.direct_interval)
        self.update_interval = timedelta(seconds=interval)
        self.fresh_registers = set(snapshot)
        if self.direct_enabled and self.cache_updated_at and (utcnow() - self.cache_updated_at).total_seconds() <= 120:
            self.fresh_registers.update(key for key in (self.data or {}) if key not in CRITICAL_IDS)
        if self.direct_enabled:
            self._schedule_cache()
        self._schedule_supplemental()
        # Only noncritical values may survive between direct primary samples.
        return {**(self.data or {}), **snapshot} if partial else snapshot

    def _schedule_cache(self):
        if (not self._shutting_down and self._cache_task is None and
                asyncio.get_running_loop().time() - self._cache_attempt >= 60):
            self._cache_attempt = asyncio.get_running_loop().time()
            self._cache_task = asyncio.create_task(self._async_read_cache())

    async def _async_read_cache(self):
        """Refresh legacy noncritical fields without changing critical state."""
        try:
            await asyncio.sleep(2)
            if self.transport.busy:
                return
            readings = await self.transport.request_read(DATA_QUERY, decoder=decode_registers)
            noncritical = {key: value for key, value in readings.items() if key not in CRITICAL_IDS}
            # Merge against the current snapshot, not the one that existed when
            # this delayed read started. Never overwrite fresh core telemetry.
            self.data = {**(self.data or {}), **noncritical}
            self.cache_updated_at = utcnow()
            self.fresh_registers.update(noncritical)
            self.async_update_listeners()
        except (OSError, asyncio.TimeoutError, ProtocolError) as err:
            _LOGGER.debug("Noncritical cached telemetry unavailable: %s", err)
        finally:
            self._cache_task = None

    def _schedule_supplemental(self):
        if (self.supplemental_enabled and not self._shutting_down and
                self._supplemental_task is None and
                asyncio.get_running_loop().time() - self._supplemental_attempt >=
                self.supplemental_interval):
            # Strong reference + explicit cancellation on unload. The task starts
            # after this core update returns, and never replaces coordinator.data.
            self._supplemental_task = asyncio.create_task(self._async_read_supplemental())

    async def _async_read_supplemental(self):
        """Optional failures never hold up publication of required telemetry."""
        self._supplemental_attempt = asyncio.get_running_loop().time()
        try:
            if self.direct_enabled:
                await asyncio.sleep(4)
                if self.transport.busy:
                    return
            try:
                self.supplemental_data = await self.transport.request_read(
                    SUPPLEMENTAL_QUERY, decoder=decode_supplemental)
            except (OSError, asyncio.TimeoutError, ProtocolError) as err:
                self.supplemental_data = {}
                self.supplemental_updated_at = None
                _LOGGER.debug("Optional SYEVO0000513 diagnostics unavailable: %s", err)
            else:
                self.supplemental_updated_at = utcnow()
            self.async_update_listeners()
        finally:
            self._supplemental_task = None

    async def async_shutdown(self):
        self._shutting_down = True
        tasks = [task for task in (self._supplemental_task, self._cache_task) if task is not None]
        self._supplemental_task = self._cache_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await super().async_shutdown()

    async def _command(self, payload, action, *, register=None, value=None):
        if payload is None:
            raise HomeAssistantError("Command unsupported in legacy mode")
        async with self._command_lock:
            self._expire_pending()
            if self._pending and self._pending["payload"] == payload:
                # Repeated automation calls must not resend an uncertain write.
                return True
            record = {"action": action, "status": "pending", "acknowledgment": "unconfirmed",
                      "requested_at": utcnow().isoformat(), "confirmed_at": None}
            if register is not None:
                record.update(register=register, value=value)
            self.last_command = record
            pending = {"payload": payload, "action": action, "register": register,
                             "value": value, "record": record, "ready": False,
                             "deadline": asyncio.get_running_loop().time() + self.confirmation_timeout}
            self._pending = pending
            try:
                response = validate_response(await self.transport.request(payload))
                record["protocol_response"] = list(response)
                if response[0] == "ERR":
                    record.update(status="rejected", acknowledgment="rejected")
                    self._pending = None
                    raise ProtocolError("Module rejected the command")
                if response[0] != "SEC":
                    raise ProtocolError("Unexpected command response type")
                # The app accepts a valid SEC receipt without requiring an echo.
                # Receipt is never evidence that the stove changed phase.
                echoed = response == json.loads(payload)
            except (OSError, asyncio.TimeoutError, ProtocolError) as err:
                record["error"] = str(err)
                raise HomeAssistantError(
                    f"4heat command failed or unconfirmed: {err}. Check actual stove state before retrying."
                ) from err
            else:
                self.last_command_accepted = utcnow()
                record.update(acknowledgment="accepted" if echoed else "received_unverified",
                              acknowledged_at=self.last_command_accepted.isoformat())
            finally:
                # Reads already in flight or started before this transaction
                # finished cannot confirm this command, even if values match.
                pending["ready"] = True
                if self.data is not None:
                    self.async_update_listeners()
            # An acknowledgment is never a fabricated stove state.
            return True

    async def async_turn_on(self):
        return await self._command(self._on_cmd, "turn_on")

    async def async_turn_off(self):
        return await self._command(self._off_cmd, "turn_off")

    async def async_unblock(self):
        return await self._command(self._unblock_cmd, "unblock")

    async def async_set_value(self, id, value):
        if not isinstance(id, str) or len(id) != 5 or not id.isdigit():
            raise HomeAssistantError("Invalid register identifier")
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 10**12:
            raise HomeAssistantError("Register value must be an unsigned 12-digit integer")
        payload = f'["SEC","1","B{id}{value:012d}"]'.encode()
        return await self._command(payload, "set_value", register=id, value=value)
